package com.iceberglearn.operations;

import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.transforms.Transform;

/**
 * API for partition spec evolution.
 *
 * <p>Changes are staged (add / remove / rename) and only take effect on {@link #commit()}
 * which rewrites the table's metadata file. Commit conflicts are NOT retried: a conflicting
 * commit surfaces as a CommitFailedException and the caller must start over with fresh
 * state. A spec update never creates a snapshot — it is a pure metadata change, which is
 * why old data files are neither rewritten nor moved.
 */
public interface UpdatePartitionSpec extends PendingUpdate<PartitionSpec> {

    /** Add an identity partition field, named after the source column. */
    UpdatePartitionSpec addField(String sourceName);

    /**
     * Add a partition field applying {@code transform} to the source column, named by the
     * shared default-name rule. Teaching stand-in for the official addField(Term).
     */
    UpdatePartitionSpec addField(String sourceName, Transform transform);

    /** Remove a partition field by its current name. */
    UpdatePartitionSpec removeField(String name);

    /** Rename a partition field; the field id and source id are untouched. */
    UpdatePartitionSpec renameField(String name, String newName);
}
