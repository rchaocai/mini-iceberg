package com.iceberglearn.metadata;

/** A table-history entry recording when one snapshot became current. */
public record SnapshotLogEntry(long timestampMillis, long snapshotId) {
}
