package com.iceberglearn.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.transforms.*;

import java.util.ArrayList;
import java.util.List;

/**
 * A complete partition specification: a list of {@link PartitionField}s and an id.
 *
 * <p>The id lets a table carry several specs over its lifetime (later chapters); the list of
 * fields says, in order, how to derive each partition value from a row.
 */
public record PartitionSpec(
        @JsonProperty("spec-id") int specId,
        @JsonProperty("fields") List<PartitionField> fields
) {

    // IDs for partition fields start at 1000, leaving the 1..n schema-id range to data columns.
    public static final int PARTITION_DATA_ID_START = 1000;

    public PartitionSpec {
        fields = List.copyOf(fields);
    }

    public static PartitionSpec unpartitioned() {
        return new PartitionSpec(0, List.of());
    }

    public boolean isPartitioned() {
        return !fields.isEmpty();
    }

    /** All partition fields derived from one source column, in spec order. */
    public List<PartitionField> getFieldsBySourceId(int sourceId) {
        return fields.stream().filter(field -> field.sourceId() == sourceId).toList();
    }

    /**
     * The one place that knows how a partition field is named by default: identity keeps the
     * source column name, every other transform appends its suffix. Shared by the Builder
     * (fresh tables) and spec evolution so the two naming paths can never drift apart.
     */
    public static String defaultFieldName(Schema.NestedField source, Transform transform) {
        if (transform instanceof Identity) {
            return source.name();
        }
        if (transform instanceof Days) {
            return source.name() + "_day";
        }
        if (transform instanceof Hours) {
            return source.name() + "_hour";
        }
        if (transform instanceof Bucket) {
            return source.name() + "_bucket";
        }
        if (transform instanceof Truncate) {
            return source.name() + "_trunc";
        }
        throw new IllegalArgumentException("Unknown transform: " + transform);
    }

    public static Builder builderFor(Schema schema) {
        return new Builder(schema);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (PartitionField field : fields) {
            sb.append("\n  ").append(field);
        }
        if (!fields.isEmpty()) {
            sb.append('\n');
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * Builds a PartitionSpec from column names, translating each name to its field id via
     * the schema — which is why the builder needs the schema in the first place.
     */
    public static final class Builder {
        private final Schema schema;
        private final List<PartitionField> fields = new ArrayList<>();
        private final List<String> usedNames = new ArrayList<>();
        private int specId = 0;
        // First partition field id is 1000; this is incremented as fields are added.
        private int lastAssignedFieldId = PARTITION_DATA_ID_START - 1;

        private Builder(Schema schema) {
            this.schema = schema;
        }

        public Builder withSpecId(int newSpecId) {
            this.specId = newSpecId;
            return this;
        }

        public Builder identity(String sourceName) {
            Schema.NestedField source = requireSource(sourceName);
            // identity reuses the source column name — no suffix — so queries like
            // WHERE category = 'x' map straight onto the partition value.
            return add(source, defaultFieldName(source, Identity.get()), Identity.get());
        }

        public Builder day(String sourceName) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, defaultFieldName(source, Days.get()), Days.get());
        }

        public Builder hour(String sourceName) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, defaultFieldName(source, Hours.get()), Hours.get());
        }

        public Builder bucket(String sourceName, int numBuckets) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, defaultFieldName(source, Bucket.get(numBuckets)), Bucket.get(numBuckets));
        }

        public Builder truncate(String sourceName, int width) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, defaultFieldName(source, Truncate.get(width)), Truncate.get(width));
        }

        private Builder add(Schema.NestedField source, String name, Transform transform) {
            return add(source.id(), lastAssignedFieldId + 1, name, transform);
        }

        /**
         * Low-level add that carries an explicit field id — the entry point for spec
         * evolution, where surviving fields must keep the ids their manifests already
         * reference. Mirrors the official package-private Builder.add(int, int, String,
         * Transform); public here because evolution lives in the operations package.
         */
        public Builder add(int sourceId, int fieldId, String name, Transform transform) {
            if (usedNames.contains(name)) {
                throw new IllegalArgumentException("Partition field name already used: " + name);
            }
            usedNames.add(name);
            // max-accumulate, never decrement: an explicit id must raise the bar so a later
            // auto-assigned id can never fall back below one that was handed out explicitly.
            lastAssignedFieldId = Math.max(lastAssignedFieldId, fieldId);
            fields.add(new PartitionField(sourceId, fieldId, name, transform));
            return this;
        }

        private Schema.NestedField requireSource(String sourceName) {
            Schema.NestedField source = schema.findField(sourceName);
            if (source == null) {
                throw new IllegalArgumentException(
                        "Cannot find source column: " + sourceName);
            }
            return source;
        }

        public PartitionSpec build() {
            return new PartitionSpec(specId, fields);
        }
    }
}
