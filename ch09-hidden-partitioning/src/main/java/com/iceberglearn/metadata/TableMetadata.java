package com.iceberglearn.metadata;

import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;

import java.util.List;

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
