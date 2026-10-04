package com.iceberglearn.manifests;

import java.util.List;

/** JSON representation of the rows in a physical manifest-list file. */
public record ManifestList(List<ManifestFile> manifests) {
    public ManifestList {
        manifests = List.copyOf(manifests);
    }
}
