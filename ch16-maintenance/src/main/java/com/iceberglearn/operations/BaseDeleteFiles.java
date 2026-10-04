package com.iceberglearn.operations;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.metrics.StrictMetricsFilter;
import com.iceberglearn.scan.DataTableScan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DeleteFiles implementation that removes data files from the table by writing DELETED
 * entries into a new manifest and committing one snapshot.
 *
 * <p>Deletion is logical: the physical Parquet files stay on disk untouched. The new
 * manifest re-declares each deleted file's path with status
 * {@link ManifestEntry.Status#DELETED}; the read side's two-pass scan
 * ({@link DataTableScan#liveDataFilesBySpec}) collects the DELETED set first and then
 * skips any file whose path appears in it. A later maintenance pass reclaims the orphaned
 * physical files.
 *
 * <p>Like {@link BaseOverwriteFiles}, this class selects files whose metrics prove every
 * row matches the row filter (strict match), and additionally honours explicit
 * {@link #deleteFile(DataFile)} calls. Unlike overwrite, delete never adds files, so the
 * committed snapshot's {@code operation} is always {@code delete}.
 */
public class BaseDeleteFiles extends SnapshotUpdate implements DeleteFiles {
    private final List<DataFile> filesToDelete = new ArrayList<>();
    private Expr rowFilter;

    public BaseDeleteFiles(TableOperations operations) {
        super(operations, "delete");
    }

    @Override
    public DeleteFiles deleteFile(DataFile file) {
        filesToDelete.add(file);
        return this;
    }

    @Override
    public DeleteFiles deleteFromRowFilter(Expr expr) {
        this.rowFilter = expr;
        return this;
    }

    @Override
    public void commit() {
        if (rowFilter == null && filesToDelete.isEmpty()) {
            return;
        }
        super.commit();
    }

    @Override
    protected TableMetadata apply(TableMetadata base) {
        long snapshotId = freshSnapshotId();
        long sequenceNumber = nextSequenceNumber(base);

        List<DataFile> liveFiles = liveFiles(base);
        List<DataFile> toDelete = new ArrayList<>();
        if (rowFilter != null) {
            for (DataFile file : liveFiles) {
                if (StrictMetricsFilter.allRowsMatch(rowFilter, file)) {
                    toDelete.add(file);
                }
            }
        }
        toDelete.addAll(filesToDelete);

        List<ManifestEntry> entries = new ArrayList<>();
        for (DataFile file : toDelete) {
            entries.add(new ManifestEntry(ManifestEntry.Status.DELETED, snapshotId, file));
        }
        ManifestFile newManifest = writeDataManifest(base, entries, snapshotId, sequenceNumber);

        List<ManifestFile> manifests = currentManifests(base);
        manifests.add(newManifest);
        String manifestListLocation =
                "metadata/snap-" + snapshotId + "-" + UUID.randomUUID() + ".manifest-list.json";
        writeJson(base.location(), manifestListLocation, new ManifestList(manifests));
        System.out.println("  [delete] write manifest list: " + manifestListLocation);
        System.out.println("  [delete] deleted=" + toDelete.size() + " operation=delete");

        Snapshot snapshot = Snapshot.builder(snapshotId)
                .parentId(base.currentSnapshotId())
                .timestampMillis(System.currentTimeMillis())
                .manifestListLocation(manifestListLocation)
                .sequenceNumber(sequenceNumber)
                .operation("delete")
                .summary(Map.of(
                        "deleted-data-files", Integer.toString(toDelete.size()),
                        "deleted-records", Long.toString(
                                toDelete.stream().mapToLong(DataFile::recordCount).sum())))
                .build();
        return appendSnapshot(base, snapshot);
    }

    private List<DataFile> liveFiles(TableMetadata base) {
        Snapshot current = base.currentSnapshot();
        if (current == null) {
            return List.of();
        }
        return DataTableScan.liveDataFilesBySpec(base.location(), current).values().stream()
                .flatMap(List::stream)
                .toList();
    }
}
