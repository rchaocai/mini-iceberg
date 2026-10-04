package com.iceberglearn.metadata;

import com.iceberglearn.partition.PartitionField;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The root metadata object serialized as a *.metadata.json file. */
public record TableMetadata(
        int formatVersion,
        String tableUuid,
        String location,
        long lastSequenceNumber,
        int currentSchemaId,
        List<Schema> schemas,
        int defaultSpecId,
        List<PartitionSpec> partitionSpecs,
        Long currentSnapshotId,
        List<Snapshot> snapshots,
        List<SnapshotLogEntry> snapshotLog
) {
    // Backward-compatible constructor used by earlier chapters' code and tests that do not
    // yet carry partition specs: an unpartitioned table is the safe default.
    public TableMetadata(
            int formatVersion,
            String tableUuid,
            String location,
            long lastSequenceNumber,
            int currentSchemaId,
            List<Schema> schemas,
            Long currentSnapshotId,
            List<Snapshot> snapshots,
            List<SnapshotLogEntry> snapshotLog) {
        this(formatVersion, tableUuid, location, lastSequenceNumber, currentSchemaId, schemas,
                0, List.of(PartitionSpec.unpartitioned()),
                currentSnapshotId, snapshots, snapshotLog);
    }

    // Constructor without snapshotLog for call sites that predate the snapshot log.
    public TableMetadata(
            int formatVersion,
            String tableUuid,
            String location,
            long lastSequenceNumber,
            int currentSchemaId,
            List<Schema> schemas,
            Long currentSnapshotId,
            List<Snapshot> snapshots) {
        this(formatVersion, tableUuid, location, lastSequenceNumber, currentSchemaId, schemas,
                0, List.of(PartitionSpec.unpartitioned()),
                currentSnapshotId, snapshots, List.of());
    }

    public TableMetadata {
        schemas = List.copyOf(schemas);
        partitionSpecs = partitionSpecs == null || partitionSpecs.isEmpty()
                ? List.of(PartitionSpec.unpartitioned()) : List.copyOf(partitionSpecs);
        snapshots = List.copyOf(snapshots);
        snapshotLog = snapshotLog == null ? List.of() : List.copyOf(snapshotLog);
    }

    public Schema currentSchema() {
        return schemas.stream()
                .filter(schema -> schema.schemaId() == currentSchemaId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Current schema not found: " + currentSchemaId));
    }

    public Schema schema(int schemaId) {
        return schemas.stream()
                .filter(schema -> schema.schemaId() == schemaId)
                .findFirst()
                .orElse(null);
    }

    /** Max column field id ever used by ANY schema. Derived rather than stored: the schemas
     *  list is append-only and dropped columns keep appearing in the older schema they were
     *  dropped from, so an id can never silently disappear — deriving the max is therefore
     *  always safe. (The per-schema {@code highestFieldId} is NOT a valid source: it shrinks
     *  when the highest column is dropped, and ids must never be recycled.) */
    public int lastColumnId() {
        return schemas.stream()
                .mapToInt(Schema::highestFieldId)
                .max()
                .orElse(0);
    }

    /** Next free schema id: one more than the highest (mirrors the official fresh schema id). */
    public int nextSchemaId() {
        return schemas.stream()
                .mapToInt(Schema::schemaId)
                .max()
                .orElse(-1) + 1;
    }

    /**
     * Append the schema under a fresh id and make it current.
     *
     * <p>A schema update is a pure metadata change: snapshots, the current snapshot pointer, the
     * specs and the sequence number are copied verbatim, and no data file is touched. The
     * incoming schema carries no meaningful schemaId yet — stamping it with the next id is this
     * method's job, because schema-id allocation is a table-level decision (mirrors the
     * official addSchemaInternal).
     */
    public TableMetadata updateSchema(Schema newSchema) {
        int freshSchemaId = nextSchemaId();
        Schema freshSchema = new Schema(freshSchemaId, newSchema.fields());
        List<Schema> updatedSchemas = new ArrayList<>(schemas);
        updatedSchemas.add(freshSchema);
        return new TableMetadata(
                formatVersion,
                tableUuid,
                location,
                lastSequenceNumber,
                freshSchemaId,   // the current pointer switches — new writes use this schema
                updatedSchemas,
                defaultSpecId,
                partitionSpecs,
                currentSnapshotId,
                snapshots,
                snapshotLog);
    }

    public PartitionSpec defaultSpec() {
        return partitionSpecs.stream()
                .filter(spec -> spec.specId() == defaultSpecId)
                .findFirst()
                .orElse(PartitionSpec.unpartitioned());
    }

    public PartitionSpec spec(int specId) {
        return partitionSpecs.stream()
                .filter(spec -> spec.specId() == specId)
                .findFirst()
                .orElse(null);
    }

    /** All specs keyed by id. The list is append-only, so ids are unique and the
     *  map never needs to handle collisions. Callers treat the map as read-only. */
    public Map<Integer, PartitionSpec> specsById() {
        Map<Integer, PartitionSpec> byId = new LinkedHashMap<>();
        for (PartitionSpec spec : partitionSpecs) {
            byId.put(spec.specId(), spec);
        }
        return byId;
    }

    /** Max partition field id ever used by ANY spec. Derived rather than stored: the specs
     *  list is append-only and removed fields keep appearing in the older spec, so an id can
     *  never silently disappear — deriving the max is therefore always safe. */
    public int lastAssignedPartitionId() {
        return partitionSpecs.stream()
                .flatMap(spec -> spec.fields().stream())
                .mapToInt(PartitionField::fieldId)
                .max()
                .orElse(PartitionSpec.PARTITION_DATA_ID_START - 1);
    }

    /** Next free spec id: one more than the highest. */
    public int nextSpecId() {
        return partitionSpecs.stream()
                .mapToInt(PartitionSpec::specId)
                .max()
                .orElse(-1) + 1;
    }

    /**
     * Append the spec under a fresh id and make it the default.
     *
     * <p>A spec update is a pure metadata change: snapshots, the current snapshot pointer and
     * the sequence number are copied verbatim, and no data file is touched. The incoming spec
     * carries no meaningful specId yet — stamping it with the next id is this method's job,
     * because spec-id allocation is a table-level decision (mirrors the official freshSpec).
     */
    public TableMetadata updatePartitionSpec(PartitionSpec newSpec) {
        int freshSpecId = nextSpecId();
        PartitionSpec freshSpec = new PartitionSpec(freshSpecId, newSpec.fields());
        List<PartitionSpec> specs = new ArrayList<>(partitionSpecs);
        specs.add(freshSpec);
        return new TableMetadata(
                formatVersion,
                tableUuid,
                location,
                lastSequenceNumber,
                currentSchemaId,
                schemas,
                freshSpecId,   // the default switches — new writes and manifests stamp this id
                specs,
                currentSnapshotId,
                snapshots,
                snapshotLog);
    }

    public Snapshot currentSnapshot() {
        return currentSnapshotId == null ? null : snapshot(currentSnapshotId);
    }

    public Snapshot snapshot(long snapshotId) {
        return snapshots.stream()
                .filter(snapshot -> snapshot.snapshotId() == snapshotId)
                .findFirst()
                .orElse(null);
    }
}
