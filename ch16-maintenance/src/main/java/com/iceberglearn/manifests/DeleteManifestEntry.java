package com.iceberglearn.manifests;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.iceberglearn.metadata.DeleteFile;

/**
 * One row in a <em>delete</em> manifest file. Mirrors {@link ManifestEntry} but carries a
 * {@link DeleteFile} instead of a {@link com.iceberglearn.metadata.DataFile}.
 *
 * <p>The {@link Status} enum and {@link #isLive()} semantics are identical to
 * {@link ManifestEntry}: a delete file participates in a scan when its latest entry is
 * {@link Status#ADDED} or {@link Status#EXISTING}.
 */
public record DeleteManifestEntry(
        ManifestEntry.Status status,
        Long snapshotId,
        DeleteFile deleteFile
) {
    @JsonIgnore
    public boolean isLive() {
        return status == ManifestEntry.Status.ADDED || status == ManifestEntry.Status.EXISTING;
    }
}
