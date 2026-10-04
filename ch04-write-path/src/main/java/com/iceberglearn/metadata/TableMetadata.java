package com.iceberglearn.metadata;

import java.util.List;
import com.iceberglearn.schema.Schema;



/** The root metadata object serialized as a *.metadata.json file. */
public record TableMetadata(
        int formatVersion,
        String tableUuid,
        String location,
        long lastSequenceNumber,
        int currentSchemaId,
        List<Schema> schemas,
        Long currentSnapshotId,
        List<Snapshot> snapshots
) {
    public TableMetadata {
        schemas = List.copyOf(schemas);
        snapshots = List.copyOf(snapshots);
    }

    public Schema currentSchema() {
        return schemas.stream()
                .filter(schema -> schema.schemaId() == currentSchemaId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Current schema not found: " + currentSchemaId));
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
