package com.iceberglearn.operations;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.metadata.DataFile;

/**
 * An update that removes data files from the table in a new snapshot.
 *
 * <p>Deletion is logical: the selected files are written into a new manifest with
 * {@link com.iceberglearn.manifests.ManifestEntry.Status#DELETED} entries; the physical
 * files are left untouched for a later maintenance pass to reclaim. The committed
 * snapshot carries {@code operation=delete}.
 */
public interface DeleteFiles {

    /** Delete a specific data file by reference. */
    DeleteFiles deleteFile(DataFile file);

    /**
     * Delete every data file whose rows <em>all</em> match {@code expr} (strict match).
     * Files that may contain both matching and non-matching rows are kept.
     */
    DeleteFiles deleteFromRowFilter(Expr expr);

    /** Build and atomically commit the new snapshot. */
    void commit();
}
