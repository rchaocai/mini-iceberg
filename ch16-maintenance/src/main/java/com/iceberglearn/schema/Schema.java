package com.iceberglearn.schema;

import java.util.List;

/**
 * The schema of a data table.
 * Each field has an integer id that is unique in the table schema.
 */
public record Schema(
        int schemaId,
        List<NestedField> fields,
        int highestFieldId
) {

    public Schema {
        fields = List.copyOf(fields);
    }

    public Schema(List<NestedField> fields) {
        this(0, fields, fields.stream().mapToInt(NestedField::id).max().orElse(0));
    }

    public Schema(int schemaId, List<NestedField> fields) {
        this(schemaId, fields, fields.stream().mapToInt(NestedField::id).max().orElse(0));
    }

    public NestedField findField(int id) {
        return fields.stream()
                .filter(f -> f.id() == id)
                .findFirst()
                .orElse(null);
    }

    public NestedField findField(String name) {
        return fields.stream()
                .filter(f -> f.name().equals(name))
                .findFirst()
                .orElse(null);
    }

    public static Schema userSchema() {
        return new Schema(List.of(
                new NestedField(1, "id", "long", true),
                new NestedField(2, "name", "string", true),
                new NestedField(3, "age", "int", true),
                new NestedField(4, "email", "string", true)
        ));
    }

    @Override
    public String toString() {
        return fields.stream()
                .map(f -> f.name() + "(" + f.type() + ")")
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
    }

    /**
     * A field in a schema. Each field has an integer id that is unique in the table schema.
     */
    public record NestedField(int id, String name, String type, boolean required) {

        public static NestedField optional(int id, String name, String type) {
            return new NestedField(id, name, type, false);
        }

        public static NestedField required(int id, String name, String type) {
            return new NestedField(id, name, type, true);
        }

        @Override
        public String toString() {
            return (required ? "required " : "optional ") +
                    type + " " + name + " (id=" + id + ")";
        }
    }
}
