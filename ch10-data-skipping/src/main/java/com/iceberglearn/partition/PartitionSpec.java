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
            return add(source, source.name(), Identity.get());
        }

        public Builder day(String sourceName) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, source.name() + "_day", Days.get());
        }

        public Builder hour(String sourceName) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, source.name() + "_hour", Hours.get());
        }

        public Builder bucket(String sourceName, int numBuckets) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, source.name() + "_bucket", Bucket.get(numBuckets));
        }

        public Builder truncate(String sourceName, int width) {
            Schema.NestedField source = requireSource(sourceName);
            return add(source, source.name() + "_trunc", Truncate.get(width));
        }

        private Builder add(Schema.NestedField source, String name, Transform transform) {
            if (usedNames.contains(name)) {
                throw new IllegalArgumentException("Partition field name already used: " + name);
            }
            usedNames.add(name);
            lastAssignedFieldId++;
            fields.add(new PartitionField(source.id(), lastAssignedFieldId, name, transform));
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
