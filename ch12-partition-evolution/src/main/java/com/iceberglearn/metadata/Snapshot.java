package com.iceberglearn.metadata;

import java.util.HashMap;
import java.util.Map;

/** Minimal snapshot subset; the next metadata layer is referenced by file location. */
public record Snapshot(
        long snapshotId,
        long timestampMillis,
        String manifestListLocation,
        long sequenceNumber,
        Long parentId,
        Map<String, String> summary
) {
    public Snapshot {
        summary = summary == null ? Map.of() : Map.copyOf(summary);
    }

    public String operation() {
        return summary.get("operation");
    }

    public static Builder builder(long snapshotId) {
        return new Builder(snapshotId);
    }

    /**
     * Builder for Snapshot.
     */
    public static class Builder {
        private final long snapshotId;
        private long timestampMillis = 0;
        private String manifestListLocation;
        private long sequenceNumber = 0;
        private Long parentId;
        private String operation;
        private Map<String, String> summary = Map.of();

        private Builder(long snapshotId) {
            this.snapshotId = snapshotId;
        }

        public Builder timestampMillis(long timestampMillis) {
            this.timestampMillis = timestampMillis;
            return this;
        }

        public Builder manifestListLocation(String manifestListLocation) {
            this.manifestListLocation = manifestListLocation;
            return this;
        }

        public Builder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        public Builder parentId(Long parentId) {
            this.parentId = parentId;
            return this;
        }

        public Builder operation(String operation) {
            this.operation = operation;
            return this;
        }

        public Builder summary(Map<String, String> summary) {
            this.summary = summary;
            return this;
        }

        public Snapshot build() {
            Map<String, String> snapshotSummary = new HashMap<>(summary);
            if (operation != null) {
                snapshotSummary.put("operation", operation);
            }
            return new Snapshot(
                    snapshotId, timestampMillis, manifestListLocation,
                    sequenceNumber, parentId, snapshotSummary);
        }
    }
}
