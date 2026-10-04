package com.iceberglearn.metadata;

import com.iceberglearn.table.Table;


/** Utilities for resolving snapshots from table history. */
public final class SnapshotUtil {
    private SnapshotUtil() {
    }

    /** Return the snapshot that was current at the given time. */
    public static long snapshotIdAsOfTime(Table table, long timestampMillis) {
        Long snapshotId = nullableSnapshotIdAsOfTime(table, timestampMillis);
        if (snapshotId == null) {
            throw new IllegalArgumentException(
                    "Cannot find a snapshot older than " + timestampMillis);
        }
        return snapshotId;
    }

    /** Return the snapshot that was current at the given time, or null if none existed. */
    public static Long nullableSnapshotIdAsOfTime(Table table, long timestampMillis) {
        Long snapshotId = null;
        for (SnapshotLogEntry entry : table.history()) {
            if (entry.timestampMillis() <= timestampMillis) {
                snapshotId = entry.snapshotId();
            }
        }
        return snapshotId;
    }
}
