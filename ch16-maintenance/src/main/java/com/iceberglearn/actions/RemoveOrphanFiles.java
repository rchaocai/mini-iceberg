package com.iceberglearn.actions;

import java.util.function.Consumer;

/**
 * Remove orphan files from the table directory.
 *
 * <p>An orphan file is a file physically present under the table location but not
 * reachable from any snapshot referenced by the current metadata (including all
 * retained snapshots, their manifest lists, their manifests, and their data/delete
 * files). Typical sources of orphans are failed writes whose commit step never
 * runs, aborted retries of snapshot updates, and external debris accidentally
 * copied into the data directory.
 *
 * <p>Two safety guards prevent removing files that are still in flight: only files
 * whose last-modified timestamp is older than a configurable threshold are
 * considered, and {@code *.metadata.json} files are never touched. The caller can
 * also pass a custom {@code deleteWith} callback to list orphans without deleting
 * them before running the real pass.
 *
 * <p>Orphan removal is the only maintenance operation that does not produce a
 * commit. Because orphaned files are not part of the metadata tree, readers are
 * unaware of them and their removal does not change the table's logical state.
 */
public interface RemoveOrphanFiles {

    /**
     * Only consider files whose last-modified timestamp is strictly older than the
     * given value. Files written after this point are assumed to be part of a
     * concurrent in-progress write and are skipped. Defaults to three days ago.
     */
    RemoveOrphanFiles olderThan(long timestampMillis);

    /**
     * Provide an alternative delete callback that receives each orphan path. The
     * default physically removes the file; a no-op or collection callback can be
     * used for a dry run.
     */
    RemoveOrphanFiles deleteWith(Consumer<String> deleteFunc);

    /** Run the scan-and-delete pass and return the paths of removed orphan files. */
    Result execute();

    /** Result carrying the actual orphan file locations that were acted upon. */
    interface Result {
        Iterable<String> orphanFileLocations();
    }
}
