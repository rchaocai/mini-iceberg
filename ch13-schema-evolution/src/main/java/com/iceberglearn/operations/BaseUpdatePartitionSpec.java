package com.iceberglearn.operations;

import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.partition.PartitionField;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.transforms.Identity;
import com.iceberglearn.transforms.Transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Stages partition spec changes against a fixed base and rebuilds one new spec on apply().
 *
 * <p>Existing fields that are not removed keep their original field ids — a partition field
 * id is how manifests and partition tuples refer to a field, so renumbering would corrupt
 * the meaning of every already-written manifest. New fields draw ids from the table-level
 * counter {@code lastAssignedPartitionId} (the max field id ever used by any spec, +1),
 * never from the per-spec 1000-start sequence the plain Builder uses for fresh tables.
 */
public class BaseUpdatePartitionSpec implements UpdatePartitionSpec {

    private final TableOperations ops;
    // Captured once at construction: a conflicting commit fails instead of retrying.
    private final TableMetadata base;
    private final Schema schema;
    private final PartitionSpec spec;
    private final Map<String, PartitionField> nameToField;

    private final List<PartitionField> adds = new ArrayList<>();
    private final Set<String> addedNames = new HashSet<>();
    private final Set<Integer> deletes = new HashSet<>();
    private final Map<String, String> renames = new HashMap<>();
    private int lastAssignedPartitionId;

    public BaseUpdatePartitionSpec(TableOperations ops) {
        this.ops = ops;
        this.base = ops.current();
        this.schema = base.currentSchema();
        this.spec = base.defaultSpec();
        this.nameToField = new LinkedHashMap<>();
        for (PartitionField field : spec.fields()) {
            nameToField.put(field.name(), field);
        }
        this.lastAssignedPartitionId = base.lastAssignedPartitionId();
    }

    @Override
    public UpdatePartitionSpec addField(String sourceName) {
        return addField(sourceName, Identity.get());
    }

    @Override
    public UpdatePartitionSpec addField(String sourceName, Transform transform) {
        Schema.NestedField source = schema.findField(sourceName);
        if (source == null) {
            throw new IllegalArgumentException("Cannot find source column: " + sourceName);
        }
        String name = PartitionSpec.defaultFieldName(source, transform);

        // The same source column under the same transform would make two partition fields
        // answer one question — pointless and ambiguous, so reject it. A field scheduled for
        // deletion no longer counts as present.
        for (PartitionField field : spec.fields()) {
            if (field.sourceId() == source.id()
                    && field.transform().equals(transform)
                    && !deletes.contains(field.fieldId())) {
                throw new IllegalArgumentException(
                        "Cannot add duplicate partition field: " + name + ", conflicts with " + field);
            }
        }
        // The official code renames a colliding deleted field out of the way; the teaching
        // version simply rejects the collision — pick a different name or transform.
        if (nameToField.containsKey(name) || addedNames.contains(name)) {
            throw new IllegalArgumentException("Partition field name already used: " + name);
        }

        // Table-level fresh id: ids are global to the table, never recycled, so a new field
        // can never be mistaken for a removed one from an older spec.
        lastAssignedPartitionId++;
        PartitionField added = new PartitionField(source.id(), lastAssignedPartitionId, name, transform);
        adds.add(added);
        addedNames.add(name);
        return this;
    }

    @Override
    public UpdatePartitionSpec removeField(String name) {
        PartitionField field = nameToField.get(name);
        if (field == null) {
            throw new IllegalArgumentException("Cannot find partition field to remove: " + name);
        }
        if (renames.containsKey(name)) {
            throw new IllegalArgumentException("Cannot rename and delete partition field: " + name);
        }
        deletes.add(field.fieldId());
        return this;
    }

    @Override
    public UpdatePartitionSpec renameField(String name, String newName) {
        PartitionField field = nameToField.get(name);
        if (field == null) {
            throw new IllegalArgumentException("Cannot find partition field to rename: " + name);
        }
        if (deletes.contains(field.fieldId())) {
            throw new IllegalArgumentException("Cannot delete and rename partition field: " + name);
        }
        if (nameToField.containsKey(newName) || addedNames.contains(newName)) {
            throw new IllegalArgumentException("Partition field name already used: " + newName);
        }
        renames.put(name, newName);
        return this;
    }

    /**
     * Rebuild one spec: surviving fields keep their ids, removed fields drop out
     * (format-version 2 semantics — no void-transform placeholder), adds are appended.
     */
    @Override
    public PartitionSpec apply() {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (PartitionField field : spec.fields()) {
            if (!deletes.contains(field.fieldId())) {
                String name = renames.getOrDefault(field.name(), field.name());
                builder.add(field.sourceId(), field.fieldId(), name, field.transform());
            }
        }
        for (PartitionField added : adds) {
            builder.add(added.sourceId(), added.fieldId(), added.name(), added.transform());
        }
        // The built spec carries no meaningful specId yet — TableMetadata owns spec-id
        // allocation, exactly like the official implementation.
        return builder.build();
    }

    /**
     * Publish the new spec as a pure metadata update. No OCC retry loop by design: the
     * official javadoc says commit conflicts are not resolved for spec updates, so a
     * conflicting commit surfaces immediately instead of being retried against a new base
     * (the staged changes were validated against the old one).
     */
    @Override
    public void commit() {
        TableMetadata update = base.updatePartitionSpec(apply());
        try {
            ops.commit(base, update);
        } catch (CommitFailedException e) {
            throw e;
        }
    }
}
