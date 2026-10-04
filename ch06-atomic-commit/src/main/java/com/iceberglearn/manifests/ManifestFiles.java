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

    private static Path resolve(String tableLocation, String fileLocation) {
        Path path = Path.of(fileLocation);
        return path.isAbsolute() ? path : Path.of(tableLocation).resolve(path);
    }
}
