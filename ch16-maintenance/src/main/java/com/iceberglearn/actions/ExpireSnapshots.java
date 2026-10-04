package com.iceberglearn.actions;

import java.util.function.Consumer;

/**
 * Expire old snapshots and physically delete files that are no longer referenced by
 * any remaining snapshot.
 *
 * <p>Expiration is a two-phase change. First, a new {@code TableMetadata} is built
 * that drops the expired snapshots from the snapshots list and commits the metadata
 * update atomically. Then, files that were only reachable through the expired
 * snapshots (manifest lists, manifests, and data files) are physically deleted. The
 * commit-before-delete order keeps the metadata consistent if the deletion phase
 * fails partway: the table keeps working correctly and a follow-up expire run can
 * pick up the leftover unreferenced files.
 *
 * <p>Files that are still referenced by any non-expired snapshot (including the
 * current snapshot) are never removed, so time travel to retained snapshots stays
 * correct.
 */
public interface ExpireSnapshots {

    /** Expire all snapshots whose timestamp is strictly older than the given millis. */
    ExpireSnapshots expireOlderThan(long timestampMillis);

    /**
     * Never expire the {@code numSnapshots} most-recent ancestors of the current
     * snapshot, regardless of their age. Explicitly-expired snapshot IDs are still
     * removed even if they would otherwise be retained.
     */
    ExpireSnapshots retainLast(int numSnapshots);

    /** Expire a snapshot by ID explicitly, bypassing age and retain-last checks. */
    ExpireSnapshots expireSnapshotId(long snapshotId);

    /**
     * Provide an alternative delete callback that receives each file path. Defaults
     * to physical deletion via {@link java.nio.file.Files#delete}; callers can pass
     * a collector to preview orphans without removing them.
     */
    ExpireSnapshots deleteWith(Consumer<String> deleteFunc);

    /** Run expiration and return the counts of deleted files by kind. */
    Result execute();

    /** Deletion counts broken down by file category. */
    interface Result {
        long deletedDataFilesCount();
        long deletedManifestsCount();
        long deletedManifestListsCount();
    }
}
