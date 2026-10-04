package com.iceberglearn.scan;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.manifests.*;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotUtil;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TableScan implementation that plans data files from one snapshot.
 *
 * <p>The two planning entry points share the same pruning pipeline (snapshot resolution
 * → manifest list → per-spec partition pruning → metrics pruning, all inside
 * {@link ManifestGroup}) and differ only in the post-pruning step:
 * <ul>
 *   <li>{@link #planFiles()} returns the surviving {@link DataFile}s as-is;</li>
 *   <li>{@link #planTasks()} feeds them through {@link SplitPlanner} to split large
 *       files and bin-pack small ones into {@link CombinedScanTask}s.</li>
 * </ul>
 */
public class DataTableScan implements TableScan {
    private final Table table;
    private final Long snapshotId;
    private final Expr filter;

    public DataTableScan(Table table, Long snapshotId) {
        this(table, snapshotId, null);
    }

    public DataTableScan(Table table, Long snapshotId, Expr filter) {
        this.table = table;
        this.snapshotId = snapshotId;
        this.filter = filter;
    }

    @Override
    public Table table() {
        return table;
    }

    @Override
    public TableScan useSnapshot(long scanSnapshotId) {
        if (snapshotId != null) {
            throw new IllegalArgumentException(
                    "Cannot override snapshot, already set snapshot id=" + snapshotId);
        }
        if (table.snapshot(scanSnapshotId) == null) {
            throw new IllegalArgumentException("Cannot find snapshot with ID " + scanSnapshotId);
        }
        return new DataTableScan(table, scanSnapshotId, filter);
    }

    @Override
    public TableScan asOfTime(long timestampMillis) {
        if (snapshotId != null) {
            throw new IllegalArgumentException(
                    "Cannot override snapshot, already set snapshot id=" + snapshotId);
        }
        return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table, timestampMillis));
    }

    @Override
    public TableScan filter(Expr predicate) {
        return new DataTableScan(table, snapshotId, predicate);
    }

    @Override
    public Expr filter() {
        return filter;
    }

    @Override
    public Snapshot snapshot() {
        return snapshotId != null ? table.snapshot(snapshotId) : table.currentSnapshot();
    }

    @Override
    public List<DataFile> planFiles() {
        Snapshot scanSnapshot = snapshot();
        if (scanSnapshot == null) {
            return List.of();
        }

        Map<Integer, List<DataFile>> filesBySpecId = collectLiveFiles(scanSnapshot);
        if (filter == null) {
            return filesBySpecId.values().stream().flatMap(List::stream).toList();
        }

        // Each manifest carries the id of the spec its partition tuples were written with.
        // Grouping by it lets ManifestGroup project the row filter with the RIGHT spec per
        // group — after spec evolution this is the whole point of the read path: the same
        // source-column predicate becomes a day predicate for the day group and an hour
        // predicate for the hour group.
        return new ManifestGroup(filesBySpecId, table.metadata().specsById())
                .filterRows(filter)
                .planFiles();
    }

    @Override
    public List<CombinedScanTask> planTasks() {
        // Stage 1: prune (metadata-only — no data file is opened).
        List<DataFile> plannedFiles = planFiles();
        // Stage 2: split large files into byte-range FileScanTasks.
        List<FileScanTask> splitTasks = SplitPlanner.splitFiles(plannedFiles, targetSplitSize());
        // Stage 3: bin-pack the splits into CombinedScanTasks of roughly targetSplitSize().
        return SplitPlanner.planTasks(
                splitTasks, targetSplitSize(), splitLookback(), splitOpenFileCost());
    }

    @Override
    public long targetSplitSize() {
        return SplitPlanner.DEFAULT_SPLIT_SIZE;
    }

    @Override
    public int splitLookback() {
        return SplitPlanner.DEFAULT_SPLIT_LOOKBACK;
    }

    @Override
    public long splitOpenFileCost() {
        return SplitPlanner.DEFAULT_OPEN_FILE_COST;
    }

    private Map<Integer, List<DataFile>> collectLiveFiles(Snapshot scanSnapshot) {
        try {
            ManifestList manifestList = ManifestFiles.readManifestList(
                    table.location(), scanSnapshot.manifestListLocation());
            Map<Integer, List<DataFile>> filesBySpecId = new LinkedHashMap<>();
            for (ManifestFile manifest : manifestList.manifests()) {
                ManifestEntries entries = ManifestFiles.readManifest(table.location(), manifest.path());
                for (ManifestEntry entry : entries.entries()) {
                    if (entry.isLive()) {
                        // Insertion order = manifest order; the spec id comes from the
                        // manifest header, not from the file, so it survives future specs.
                        filesBySpecId
                                .computeIfAbsent(manifest.partitionSpecId(), id -> new ArrayList<>())
                                .add(entry.dataFile());
                    }
                }
            }
            return filesBySpecId;
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to plan files for snapshot " + scanSnapshot.snapshotId(), e);
        }
    }
}
