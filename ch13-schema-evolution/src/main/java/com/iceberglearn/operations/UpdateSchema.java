package com.iceberglearn.operations;

/**
 * API for schema evolution.
 *
 * <p>Changes are staged (add / rename / delete) and only take effect on {@link #commit()},
 * which rewrites the table's metadata file. Commit conflicts are NOT retried: like the real
 * Iceberg UpdateSchema, a conflicting commit surfaces as a CommitFailedException and the
 * caller must start over with fresh state. A schema update never creates a snapshot — it is
 * a pure metadata change, which is why old data files are neither rewritten nor moved.
 */
public interface UpdateSchema extends PendingUpdate<com.iceberglearn.schema.Schema> {

    /**
     * Add a new optional top-level column. The column draws a fresh table-level field id;
     * ids are never recycled, so the new column can never be mistaken for a dropped one.
     */
    UpdateSchema addColumn(String name, String type);

    /**
     * Rename a column in the schema. The field id, type and nullability are untouched —
     * data files store {@code field-id -> data}, so a rename changes only the label.
     */
    UpdateSchema renameColumn(String name, String newName);

    /** Delete a column in the schema. The data lives on in old files, unread. */
    UpdateSchema deleteColumn(String name);
}
