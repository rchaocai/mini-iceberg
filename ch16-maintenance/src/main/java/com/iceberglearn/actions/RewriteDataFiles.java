package com.iceberglearn.actions;

import com.iceberglearn.expressions.Expr;

/**
 * Rewrite small data files into fewer, larger files.
 *
 * <p>The operation does not change the logical table contents — after rewrite the same
 * rows are visible — only the physical layout of data files changes: small files are
 * read, their rows concatenated, and the combined rows written into larger output
 * files. The commit replaces the rewritten files with the new ones in a fresh snapshot
 * whose manifest carries both ADDED entries (the merged files) and DELETED entries
 * (the original small files). Previous snapshots keep referencing the small files,
 * so time travel remains correct until those snapshots are expired.
 */
public interface RewriteDataFiles {

    /** Only consider files with a size strictly below this value (bytes) for rewriting. */
    RewriteDataFiles targetSizeBytes(long targetSizeBytes);

    /**
     * Restrict rewriting to files that could match the given row filter. An inclusive
     * metrics check is used: files whose bounds cannot match the filter are kept as-is.
     */
    RewriteDataFiles filter(Expr expr);

    /**
     * Execute the rewrite: pick eligible files, read and merge their rows, write new
     * files, and commit a replacement snapshot. The result describes how many files
     * were rewritten and how many were produced.
     */
    Result execute();

    /** Summary of a completed rewrite run. */
    interface Result {
        int rewrittenDataFilesCount();
        int addedDataFilesCount();
        long rewrittenBytesCount();
    }
}
