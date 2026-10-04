package com.iceberglearn.operations;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.metadata.DataFile;

/**
 * An update that replaces a set of data files with new ones in a new snapshot.
 *
 * <p>{@link #overwriteByRowFilter(Expr)} selects existing files whose rows <em>all</em>
 * match the expression (strict match) and marks them deleted; {@link #addFile(DataFile)}
 * appends a new file. The commit produces one manifest holding both ADDED and DELETED
 * entries, with {@code operation=overwrite} (or {@code delete}/{@code append} when only
 * one side is present).
 */
public interface OverwriteFiles {

    /**
     * Select existing data files to delete: a file is selected when every row it holds
     * matches {@code expr}. Files that may contain both matching and non-matching rows are
     * left untouched — overwriting must never lose rows that fall outside the filter.
     */
    OverwriteFiles overwriteByRowFilter(Expr expr);

    /** Add a new data file that replaces the deleted ones. */
    OverwriteFiles addFile(DataFile file);

    /** Delete a specific data file by reference. */
    OverwriteFiles deleteFile(DataFile file);

    /** Build and atomically commit the new snapshot. */
    void commit();
}
