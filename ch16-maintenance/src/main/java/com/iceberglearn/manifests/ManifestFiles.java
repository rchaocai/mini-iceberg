package com.iceberglearn.manifests;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;

/** Reads the JSON manifest encodings used by this project. */
public final class ManifestFiles {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private ManifestFiles() {
    }

    public static ManifestList readManifestList(String tableLocation, String fileLocation)
            throws IOException {
        return OBJECT_MAPPER.readValue(resolve(tableLocation, fileLocation).toFile(), ManifestList.class);
    }

    public static ManifestEntries readManifest(String tableLocation, String fileLocation)
            throws IOException {
        return OBJECT_MAPPER.readValue(resolve(tableLocation, fileLocation).toFile(), ManifestEntries.class);
    }

    /**
     * Read a <em>delete</em> manifest — one whose rows carry {@link DeleteManifestEntry} envelopes
     * (and therefore {@link com.iceberglearn.metadata.DeleteFile}s rather than
     * {@link com.iceberglearn.metadata.DataFile}s).
     *
     * <p>Same on-disk shape as a data manifest (one JSON list of entries), but a different
     * envelope type. Callers select which reader to use by inspecting
     * {@link ManifestFile#content()}: {@link ManifestContent#DATA} → {@link #readManifest},
     * {@link ManifestContent#DELETES} → this method.
     */
    public static DeleteManifestEntries readDeleteManifest(String tableLocation, String fileLocation)
            throws IOException {
        return OBJECT_MAPPER.readValue(resolve(tableLocation, fileLocation).toFile(),
                DeleteManifestEntries.class);
    }

    private static Path resolve(String tableLocation, String fileLocation) {
        Path path = Path.of(fileLocation);
        return path.isAbsolute() ? path : Path.of(tableLocation).resolve(path);
    }
}
