package com.iceberglearn.manifests;

import java.util.List;

/** JSON envelope used to store manifest rows in this module. */
public record ManifestEntries(List<ManifestEntry> entries) {
    public ManifestEntries {
        entries = List.copyOf(entries);
    }
}
