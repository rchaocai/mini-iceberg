package com.iceberglearn.scan;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.metadata.DataFile;
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
 * <p>Two planning entry points:
 * <ul>
 *   <li>{@link #planFiles()} — pruning only. Returns the surviving {@link DataFile}s after
 *       partition and metrics pruning. Use this when you want to see what was kept.</li>
 *   <li>{@link #planTasks()} — the full pipeline. Prunes, then splits large files and
 *       bin-packs small ones into {@link CombinedScanTask}s of roughly
 *       {@link #targetSplitSize()} bytes. This is what an execution engine calls.</li>
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

    /** Target size in bytes for each combined task produced by {@link #planTasks()}. */
    long targetSplitSize();

    /** Lookback window for bin-packing (number of previous bins to consider). */
    int splitLookback();

    /** Estimated cost in bytes of opening one file, used as a minimum task weight. */
    long splitOpenFileCost();
}
