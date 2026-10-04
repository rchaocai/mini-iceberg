package com.iceberglearn.manifests;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.iceberglearn.metadata.DataFile;

/** One row in a manifest file. */
public record ManifestEntry(
        Status status,
        Long snapshotId,
        DataFile dataFile
) {
    public enum Status {
        EXISTING,
        ADDED,
        DELETED
    }

    @JsonIgnore
    public boolean isLive() {
        return status == Status.ADDED || status == Status.EXISTING;
    }
}
