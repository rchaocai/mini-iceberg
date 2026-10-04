package com.iceberglearn.scan;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.table.Table;

import java.util.List;

/**
 * API for configuring a scan of one table snapshot.
 *
 * <p>Like the real Iceberg {@code TableScan} / {@code Scan}, a scan is immutable: refinement
 * methods such as {@link #useSnapshot(long)} and {@link #filter(Expr)} return new scans rather
 * than mutating this one, so a configured scan is safe to share between threads.
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
     * so callers never construct partition predicates by hand — mirroring the real Iceberg scan,
     * where {@code ManifestGroup} runs {@code Projections.inclusive(spec).project(dataFilter)}.
     */
    TableScan filter(Expr predicate);

    /** Return this scan's filter expression, or {@code null} if none is set. */
    Expr filter();

    /** Return the snapshot that will be used by this scan. */
    Snapshot snapshot();

    /** Plan the live data files referenced by the selected snapshot, applying any filter. */
    List<DataFile> planFiles();
}
