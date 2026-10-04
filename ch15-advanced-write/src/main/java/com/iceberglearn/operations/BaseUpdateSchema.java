package com.iceberglearn.operations;

import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.schema.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stages schema changes against a fixed base and rebuilds one new schema on apply().
 *
 * <p>Two staged collections mirror the official SchemaUpdate: {@code updates} holds the
 * replacement {@code NestedField} for every changed column, keyed by field id — renames and
 * additions land here alike; {@code deletes} holds the field ids to drop. Unchanged columns
 * keep their ids: the id is how data files and manifests refer to a column, so renumbering
 * would silently misattribute old files' bytes to different columns.
 *
 * <p>New columns draw ids from the table-level counter {@code lastColumnId} (the max field id
 * ever used by any schema, +1). The per-schema {@code highestFieldId} is not a valid source:
 * it shrinks when the highest column is dropped, and reusing a dropped id would make readers
 * resolve old files' data as the new column.
 */
public class BaseUpdateSchema implements UpdateSchema {

    private final TableOperations ops;
    // Captured once at construction: a conflicting commit fails instead of retrying.
    private final TableMetadata base;
    private final Schema schema;
    private final Map<String, Schema.NestedField> nameToField;

    // Replacement fields keyed by field id; insertion order = staging order, so added
    // columns append to the new schema in the order they were staged.
    private final Map<Integer, Schema.NestedField> updates = new LinkedHashMap<>();
    private final List<Integer> deletes = new ArrayList<>();
    private int lastColumnId;

    public BaseUpdateSchema(TableOperations ops) {
        this.ops = ops;
        this.base = ops.current();
        this.schema = base.currentSchema();
        this.nameToField = new LinkedHashMap<>();
        for (Schema.NestedField field : schema.fields()) {
            nameToField.put(field.name(), field);
        }
        this.lastColumnId = base.lastColumnId();
    }

    @Override
    public UpdateSchema addColumn(String name, String type) {
        Schema.NestedField existing = schema.findField(name);
        // The name must not collide with a column of the CURRENT schema — one that this
        // update deletes no longer counts (mirrors the official rule, which lets a dropped
        // name come back). Names may repeat across schema versions; only field ids are
        // permanent.
        if (existing != null && !deletes.contains(existing.id())) {
            throw new IllegalArgumentException("Cannot add column, name already exists: " + name);
        }
        for (Schema.NestedField staged : updates.values()) {
            if (schema.findField(staged.id()) == null && staged.name().equals(name)) {
                throw new IllegalArgumentException("Cannot add column, name already exists: " + name);
            }
        }

        // Table-level fresh id: ids are global to the table, never recycled, so a new column
        // can never be mistaken for a dropped one in an older data file.
        lastColumnId++;
        Schema.NestedField added = Schema.NestedField.optional(lastColumnId, name, type);
        updates.put(added.id(), added);
        return this;
    }

    @Override
    public UpdateSchema renameColumn(String name, String newName) {
        Schema.NestedField field = nameToField.get(name);
        if (field == null) {
            throw new IllegalArgumentException("Cannot rename missing column: " + name);
        }
        if (deletes.contains(field.id())) {
            throw new IllegalArgumentException("Cannot rename a column that will be deleted: " + name);
        }
        if (nameToField.containsKey(newName) || newName.equals(stagedNameOf(field.id()))) {
            throw new IllegalArgumentException("Cannot rename to a name already used: " + newName);
        }

        // Merge with a staged update if present: rename composes with an earlier change to
        // the same column by renaming the staged replacement, not the original field.
        Schema.NestedField current = updates.getOrDefault(field.id(), field);
        Schema.NestedField renamed = new Schema.NestedField(
                field.id(), newName, current.type(), current.required());
        updates.put(field.id(), renamed);
        return this;
    }

    @Override
    public UpdateSchema deleteColumn(String name) {
        Schema.NestedField field = nameToField.get(name);
        if (field == null) {
            throw new IllegalArgumentException("Cannot delete missing column: " + name);
        }
        if (updates.containsKey(field.id())) {
            throw new IllegalArgumentException("Cannot delete a column that has updates: " + name);
        }
        deletes.add(field.id());
        return this;
    }

    /**
     * Rebuild one schema: existing fields keep their ids (skipping deletes, applying staged
     * replacements), then added fields append in staging order.
     */
    @Override
    public Schema apply() {
        List<Schema.NestedField> fields = new ArrayList<>();
        for (Schema.NestedField field : schema.fields()) {
            if (!deletes.contains(field.id())) {
                fields.add(updates.getOrDefault(field.id(), field));
            }
        }
        for (Schema.NestedField staged : updates.values()) {
            if (schema.findField(staged.id()) == null) {
                fields.add(staged);
            }
        }
        // The built schema carries no meaningful schemaId yet — TableMetadata owns schema-id
        // allocation, exactly like the official implementation.
        return new Schema(fields);
    }

    /**
     * Publish the new schema as a pure metadata update. No OCC retry loop by design: the
     * official javadoc says commit conflicts are not resolved for schema updates, so a
     * conflicting commit surfaces immediately instead of being retried against a new base
     * (the staged changes were validated against the old one).
     */
    @Override
    public void commit() {
        TableMetadata update = base.updateSchema(apply());
        try {
            ops.commit(base, update);
        } catch (CommitFailedException e) {
            throw e;
        }
    }

    private String stagedNameOf(int fieldId) {
        Schema.NestedField staged = updates.get(fieldId);
        return staged != null ? staged.name() : null;
    }
}
