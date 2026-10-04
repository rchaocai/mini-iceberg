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
 * OverwriteFiles implementation that selects existing files by strict metrics match and
 * replaces them with new files in a single new snapshot.
 *
 * <p>Extends {@link SnapshotUpdate} so the read-compute-commit cycle is retried with
 * optimistic concurrency: when a concurrent writer publishes first, this overwrite
 * re-reads the new base, re-selects files to delete against the new live set, and
 * rebuilds the manifest against it.
 *
 * <p>The {@code operation} stamped into the new snapshot follows the rule:
 * only-delete → {@code delete}, only-add → {@code append}, both → {@code overwrite}.
 */
public class BaseOverwriteFiles extends SnapshotUpdate implements OverwriteFiles {
    private final List<DataFile> filesToAdd = new ArrayList<>();
    private final List<DataFile> filesToDelete = new ArrayList<>();
    private Expr rowFilter;

    public BaseOverwriteFiles(TableOperations operations) {
        super(operations, "overwrite");
    }

    @Override
    public OverwriteFiles overwriteByRowFilter(Expr expr) {
        this.rowFilter = expr;
        return this;
    }

    @Override
    public OverwriteFiles addFile(DataFile file) {
        filesToAdd.add(file);
        return this;
    }

    @Override
    public OverwriteFiles deleteFile(DataFile file) {
        filesToDelete.add(file);
        return this;
    }

    @Override
    public void commit() {
        if (rowFilter == null && filesToAdd.isEmpty() && filesToDelete.isEmpty()) {
            return;
        }
        super.commit();
    }

    @Override
    protected TableMetadata apply(TableMetadata base) {
        long snapshotId = freshSnapshotId();
        long sequenceNumber = nextSequenceNumber(base);

        // The set of files this overwrite will mark DELETED comes from two sources:
        //   1. Existing live files whose metrics prove every row matches rowFilter
        //      (strict match — deleting a file whose rows only *might* match would lose
        //      rows that fall outside the filter).
        //   2. Files the caller named explicitly via deleteFile(DataFile).
        // Both are stamped DELETED in the new manifest; the read side's two-pass scan
        // (DataTableScan.liveDataFilesBySpec) skips any path that appears as DELETED in
        // any manifest, so a scan after this commit no longer returns these files even
        // though the physical Parquet files stay on disk.
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
        for (DataFile file : filesToAdd) {
            entries.add(new ManifestEntry(ManifestEntry.Status.ADDED, snapshotId, file));
        }

        // One manifest holds both sides of the overwrite. The per-status counts in its
        // descriptor are derived from the entries, so the manifest list carries accurate
        // added/deleted totals without the caller tallying them by hand.
        ManifestFile newManifest = writeDataManifest(base, entries, snapshotId, sequenceNumber);

        List<ManifestFile> manifests = currentManifests(base);
        manifests.add(newManifest);
        String manifestListLocation =
                "metadata/snap-" + snapshotId + "-" + UUID.randomUUID() + ".manifest-list.json";
        writeJson(base.location(), manifestListLocation, new ManifestList(manifests));
        System.out.println("  [overwrite] write manifest list: " + manifestListLocation);

        boolean hasDeletes = !toDelete.isEmpty();
        boolean hasAdds = !filesToAdd.isEmpty();
        String operation = hasDeletes && !hasAdds ? "delete"
                : hasAdds && !hasDeletes ? "append"
                : "overwrite";
        System.out.println("  [overwrite] deleted=" + toDelete.size()
                + " added=" + filesToAdd.size() + " operation=" + operation);

        Snapshot snapshot = Snapshot.builder(snapshotId)
                .parentId(base.currentSnapshotId())
                .timestampMillis(System.currentTimeMillis())
                .manifestListLocation(manifestListLocation)
                .sequenceNumber(sequenceNumber)
                .operation(operation)
                .summary(Map.of(
                        "deleted-data-files", Integer.toString(toDelete.size()),
                        "added-data-files", Integer.toString(filesToAdd.size()),
                        "deleted-records", Long.toString(
                                toDelete.stream().mapToLong(DataFile::recordCount).sum()),
                        "added-records", Long.toString(
                                filesToAdd.stream().mapToLong(DataFile::recordCount).sum())))
                .build();
        return appendSnapshot(base, snapshot);
    }

    /** Flatten the current snapshot's live files so strict matching can run over them. */
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
