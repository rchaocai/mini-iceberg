package com.iceberglearn.scan;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.manifests.*;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.DeleteFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotUtil;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 *
 * <p>This class also owns the <em>live-file enumeration</em> used by both the read and the
 * write paths. A file is live in a snapshot when its latest entry across all DATA manifests
 * is {@link ManifestEntry.Status#ADDED ADDED} or {@link ManifestEntry.Status#EXISTING
 * EXISTING} — never {@link ManifestEntry.Status#DELETED DELETED}. Delete and overwrite
 * operations mark files DELETED by writing a new manifest; this class's static
 * {@link #liveDataFilesBySpec} and {@link #deleteFiles} helpers see those DELETED entries
 * and skip the corresponding files, so a scan after a delete returns fewer files even
 * though no physical file was touched.
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

        Map<Integer, List<DataFile>> filesBySpecId = liveDataFilesBySpec(
                table.location(), scanSnapshot);
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
    public List<DeleteFile> planDeleteFiles() {
        Snapshot scanSnapshot = snapshot();
        if (scanSnapshot == null) {
            return List.of();
        }
        return deleteFiles(table.location(), scanSnapshot);
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

    // ---------- shared live-file enumeration ----------

    /**
     * Live {@link DataFile}s in {@code snapshot}, grouped by the partition spec id of the
     * manifest that holds each file's latest entry.
     *
     * <p>Two passes over the snapshot's DATA manifests:
     * <ol>
     *   <li>collect every path that has ever been marked
     *       {@link ManifestEntry.Status#DELETED DELETED};</li>
     *   <li>collect every {@link ManifestEntry.Status#ADDED ADDED} or
     *       {@link ManifestEntry.Status#EXISTING EXISTING} file whose path is not in the
     *       deleted set.</li>
     * </ol>
     * Two passes are necessary because deletion happens by writing a <em>new</em> manifest
     * that re-declares the file with status DELETED; the original manifest that ADDEDed the
     * file is untouched and still lists it as ADDED. Only by first collecting the DELETED
     * set can the second pass know to skip those files. This is the read-side counterpart
     * of the write-side rule "deletion appends a new manifest; it never edits an old one".
     *
     * <p>DELETES manifests (those carrying {@link DeleteFile} entries) are skipped here —
     * their rows cannot be deserialized as {@link ManifestEntry}. Use {@link #deleteFiles}
     * for those.
     */
    public static Map<Integer, List<DataFile>> liveDataFilesBySpec(
            String tableLocation, Snapshot snapshot) {
        try {
            ManifestList manifestList = ManifestFiles.readManifestList(
                    tableLocation, snapshot.manifestListLocation());
            Set<String> deletedPaths = new HashSet<>();
            for (ManifestFile manifest : manifestList.manifests()) {
                if (manifest.content() != ManifestContent.DATA) {
                    continue;
                }
                ManifestEntries entries = ManifestFiles.readManifest(tableLocation, manifest.path());
                for (ManifestEntry entry : entries.entries()) {
                    if (entry.status() == ManifestEntry.Status.DELETED) {
                        deletedPaths.add(entry.dataFile().path());
                    }
                }
            }
            Map<Integer, List<DataFile>> filesBySpecId = new LinkedHashMap<>();
            for (ManifestFile manifest : manifestList.manifests()) {
                if (manifest.content() != ManifestContent.DATA) {
                    continue;
                }
                ManifestEntries entries = ManifestFiles.readManifest(tableLocation, manifest.path());
                for (ManifestEntry entry : entries.entries()) {
                    if (entry.isLive() && !deletedPaths.contains(entry.dataFile().path())) {
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
                    "Failed to plan files for snapshot " + snapshot.snapshotId(), e);
        }
    }

    /**
     * Live {@link DeleteFile}s in {@code snapshot}, flat list (no spec grouping needed —
     * delete files are passed to the engine as-is, not pruned by partition projection here).
     *
     * <p>Same two-pass shape as {@link #liveDataFilesBySpec}, but over the snapshot's DELETES
     * manifests and reading each as a {@link DeleteManifestEntries} envelope. A delete file
     * is live when its latest entry is ADDED or EXISTING.
     */
    public static List<DeleteFile> deleteFiles(String tableLocation, Snapshot snapshot) {
        try {
            ManifestList manifestList = ManifestFiles.readManifestList(
                    tableLocation, snapshot.manifestListLocation());
            Set<String> deletedPaths = new HashSet<>();
            for (ManifestFile manifest : manifestList.manifests()) {
                if (manifest.content() != ManifestContent.DELETES) {
                    continue;
                }
                DeleteManifestEntries entries =
                        ManifestFiles.readDeleteManifest(tableLocation, manifest.path());
                for (DeleteManifestEntry entry : entries.entries()) {
                    if (entry.status() == ManifestEntry.Status.DELETED) {
                        deletedPaths.add(entry.deleteFile().path());
                    }
                }
            }
            List<DeleteFile> live = new ArrayList<>();
            for (ManifestFile manifest : manifestList.manifests()) {
                if (manifest.content() != ManifestContent.DELETES) {
                    continue;
                }
                DeleteManifestEntries entries =
                        ManifestFiles.readDeleteManifest(tableLocation, manifest.path());
                for (DeleteManifestEntry entry : entries.entries()) {
                    if (entry.isLive() && !deletedPaths.contains(entry.deleteFile().path())) {
                        live.add(entry.deleteFile());
                    }
                }
            }
            return live;
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read delete files for snapshot " + snapshot.snapshotId(), e);
        }
    }
}
