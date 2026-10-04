package com.iceberglearn.operations;

import com.iceberglearn.manifests.DeleteManifestEntries;
import com.iceberglearn.manifests.DeleteManifestEntry;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.DeleteFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * RowDelta implementation that encodes row-level changes — inserts plus row deletes —
 * in a single new snapshot.
 *
 * <p>Unlike {@link BaseOverwriteFiles}, which removes whole data files by writing DELETED
 * entries against them, RowDelta <em>adds</em> {@link DeleteFile}s alongside new
 * {@link DataFile}s. A delete file does not remove a data file from the table; it tells
 * readers which rows to drop when scanning a data file. Position deletes name one data
 * file and list row ordinals inside it; equality deletes list value tuples over a set
 * of fields whose matching rows should be removed.
 *
 * <p>Two manifests come out of one commit:
 * <ul>
 *   <li>a DATA manifest holding {@link ManifestEntry.Status#ADDED ADDED} entries for each
 *       new {@link DataFile} of inserted rows;</li>
 *   <li>a DELETES manifest holding {@link DeleteManifestEntry}s of
 *       {@link ManifestEntry.Status#ADDED ADDED} status for each new {@link DeleteFile}.</li>
 * </ul>
 * Both are listed in the new snapshot's manifest list. A scan reads the DATA manifest to
 * plan data files (as before) and the DELETES manifest via {@link TableScan#planDeleteFiles()}
 * to retrieve the delete files — the engine is then responsible for applying the encoded
 * row deletes when it reads the rows.
 *
 * <p>The committed snapshot's {@code operation} follows the rule: when only delete files
 * are added (no data files), the operation is {@code delete}; otherwise it is
 * {@code overwrite}.
 */
public class BaseRowDelta extends SnapshotUpdate implements RowDelta {
    private final List<DataFile> dataFiles = new ArrayList<>();
    private final List<DeleteFile> deleteFiles = new ArrayList<>();

    public BaseRowDelta(TableOperations operations) {
        super(operations, "row-delta");
    }

    @Override
    public RowDelta addRows(DataFile inserts) {
        dataFiles.add(inserts);
        return this;
    }

    @Override
    public RowDelta addDeletes(DeleteFile deletes) {
        deleteFiles.add(deletes);
        return this;
    }

    @Override
    public void commit() {
        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }
        super.commit();
    }

    @Override
    protected TableMetadata apply(TableMetadata base) {
        long snapshotId = freshSnapshotId();
        long sequenceNumber = nextSequenceNumber(base);

        // DATA manifest: one ADDED entry per new data file. When no data files are added
        // this manifest is skipped, and only the DELETES manifest goes into the snapshot.
        List<ManifestFile> newManifests = new ArrayList<>();
        if (!dataFiles.isEmpty()) {
            List<ManifestEntry> dataEntries = new ArrayList<>();
            for (DataFile file : dataFiles) {
                dataEntries.add(new ManifestEntry(ManifestEntry.Status.ADDED, snapshotId, file));
            }
            newManifests.add(writeDataManifest(base, dataEntries, snapshotId, sequenceNumber));
        }

        // DELETES manifest: one ADDED DeleteManifestEntry per new delete file. The entry's
        // status semantics mirror ManifestEntry — a delete file participates in a scan
        // when its latest entry is ADDED or EXISTING.
        if (!deleteFiles.isEmpty()) {
            List<DeleteManifestEntry> deleteEntries = new ArrayList<>();
            for (DeleteFile file : deleteFiles) {
                deleteEntries.add(new DeleteManifestEntry(
                        ManifestEntry.Status.ADDED, snapshotId, file));
            }
            newManifests.add(writeDeleteManifest(base, deleteEntries, snapshotId, sequenceNumber));
        }

        List<ManifestFile> manifests = currentManifests(base);
        manifests.addAll(newManifests);
        String manifestListLocation =
                "metadata/snap-" + snapshotId + "-" + UUID.randomUUID() + ".manifest-list.json";
        writeJson(base.location(), manifestListLocation, new ManifestList(manifests));
        System.out.println("  [row-delta] write manifest list: " + manifestListLocation);

        // only-delete-files → delete, else → overwrite.
        boolean onlyDeletes = deleteFiles.isEmpty() ? false : dataFiles.isEmpty();
        String operation = onlyDeletes ? "delete" : "overwrite";
        System.out.println("  [row-delta] data-files=" + dataFiles.size()
                + " delete-files=" + deleteFiles.size() + " operation=" + operation);

        Snapshot snapshot = Snapshot.builder(snapshotId)
                .parentId(base.currentSnapshotId())
                .timestampMillis(System.currentTimeMillis())
                .manifestListLocation(manifestListLocation)
                .sequenceNumber(sequenceNumber)
                .operation(operation)
                .summary(Map.of(
                        "added-data-files", Integer.toString(dataFiles.size()),
                        "added-delete-files", Integer.toString(deleteFiles.size()),
                        "added-records", Long.toString(
                                dataFiles.stream().mapToLong(DataFile::recordCount).sum()),
                        "deleted-records", Long.toString(
                                deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum())))
                .build();
        return appendSnapshot(base, snapshot);
    }
}
