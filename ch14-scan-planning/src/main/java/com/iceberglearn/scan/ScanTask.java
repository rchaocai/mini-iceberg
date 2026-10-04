package com.iceberglearn.scan;

/**
 * A unit of work produced by scan planning and consumed by an execution engine.
 *
 * <p>This is the bridge between the metadata-only planning phase (which produces tasks)
 * and the data-reading execution phase (which consumes them). An engine that reads a
 * table never touches manifests or metadata files — it only iterates the tasks
 * returned by {@link TableScan#planTasks()}.
 *
 * <p>The sealed hierarchy has two members:
 * <ul>
 *   <li>{@link FileScanTask} — one byte range in one data file;</li>
 *   <li>{@link CombinedScanTask} — a bin-packed group of file tasks.</li>
 * </ul>
 */
public sealed interface ScanTask permits FileScanTask, CombinedScanTask {

    /** Total bytes this task will read. Used by engines to estimate work and balance load. */
    long sizeBytes();
}
