package com.iceberglearn.manifests;

import java.util.List;

/** JSON envelope used to store delete-manifest rows in this module. */
public record DeleteManifestEntries(List<DeleteManifestEntry> entries) {
    public DeleteManifestEntries {
        entries = List.copyOf(entries);
    }
}
