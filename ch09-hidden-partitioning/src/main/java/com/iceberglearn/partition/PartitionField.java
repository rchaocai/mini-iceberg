package com.iceberglearn.partition;

import com.iceberglearn.transforms.Transform;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One field of a {@link PartitionSpec}: binds a source column (by id) to a {@link Transform}
 * and names the resulting partition field.
 *
 * <p>The source is referenced by field id, not by name, so renaming the source column never
 * breaks the partition spec — that is the second half of hidden partitioning.
 */
public record PartitionField(
        @JsonProperty("source-id") int sourceId,
        @JsonProperty("field-id") int fieldId,
        @JsonProperty("name") String name,
        @JsonProperty("transform") Transform transform
) {
    @Override
    public String toString() {
        return fieldId + ": " + name + ": " + transform + "(" + sourceId + ")";
    }
}
