package com.iceberglearn.scan;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.DeleteFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.table.Table;

import java.util.List;

/**
 * API for configuring a scan of one table snapshot.
 *
 * <p>A scan is immutable: refinement methods such as {@link #useSnapshot(long)} and
 * {@link #filter(Expr)} return new scans rather than mutating this one, so a configured
 * scan is safe to share between threads.
 *
 * <p>Three planning entry points:
 * <ul>
 *   <li>{@link #planFiles()} — pruning only. Returns the surviving {@link DataFile}s after
 *       partition and metrics pruning. Use this when you want to see what was kept.</li>
 *   <li>{@link #planTasks()} — the full data pipeline. Prunes, then splits large files and
 *       bin-packs small ones into {@link CombinedScanTask}s of roughly
 *       {@link #targetSplitSize()} bytes. This is what an execution engine calls to read
 *       rows.</li>
 *   <li>{@link #planDeleteFiles()} — list the live {@link DeleteFile}s in this snapshot.
 *       Delete files are <em>not</em> applied here; they are handed to the execution
 *       engine, which is responsible for dropping the rows they encode.</li>
 * </ul>
 */
public interface TableScan {

    /** Return the table from which this scan loads files. */
    Table table();

    /** Return a new scan bound to the snapshot with the given ID. */
    TableScan useSnapshot(long snapshotId);

    /** Return a new scan bound to the snapshot that was current at the given time. */
    TableScan asOfTime(long timestampMillis);

    /**
     * Return a new scan whose results are filtered by the given row predicate.
     *
     * <p>The predicate is written against <em>source column</em> field IDs. Projection from source
     * columns to partition fields happens later, inside {@link com.iceberglearn.manifests.ManifestGroup},
     * so callers never construct partition predicates by hand.
     */
    TableScan filter(Expr predicate);

    /** Return this scan's filter expression, or {@code null} if none is set. */
    Expr filter();

    /** Return the snapshot that will be used by this scan. */
    Snapshot snapshot();

    /**
     * Plan the live data files referenced by the selected snapshot, applying any filter.
     *
     * <p>This is the metadata-only pruning stage: partition pruning then metrics pruning,
     * with no data file opened. Each surviving file is returned whole — no splitting or
     * bin-packing happens here.
     */
    List<DataFile> planFiles();

    /**
     * Plan balanced combined tasks for this scan by splitting large files and bin-packing
     * small ones.
     *
     * <p>Runs {@link #planFiles()} first, then passes the surviving files through
     * {@link SplitPlanner#splitFiles} (cut large files into byte-range chunks) and
     * {@link SplitPlanner#planTasks} (bin-pack the chunks into combined tasks of
     * roughly {@link #targetSplitSize()} bytes). This is the entry point an execution
     * engine calls — the engine iterates the returned tasks and reads each one's
     * byte ranges, never touching table metadata.
     */
    List<CombinedScanTask> planTasks();

    /**
     * Plan the live delete files referenced by the selected snapshot.
     *
     * <p>Delete files live in DELETES manifests alongside the data files in DATA manifests.
     * The returned {@link DeleteFile}s describe rows to drop from data files — a position
     * delete file points at one data file and lists row ordinals; an equality delete file
     * lists value tuples over its {@code equalityFieldIds}. This implementation returns
     * them and stops: applying the deletes to the rows is the execution engine's job,
     * consistent with {@link #planTasks()} handing the engine byte-range tasks without
     * reading the bytes itself.
     */
    List<DeleteFile> planDeleteFiles();

    /** Target size in bytes for each combined task produced by {@link #planTasks()}. */
    long targetSplitSize();

    /** Lookback window for bin-packing (number of previous bins to consider). */
    int splitLookback();

    /** Estimated cost in bytes of opening one file, used as a minimum task weight. */
    long splitOpenFileCost();
}
