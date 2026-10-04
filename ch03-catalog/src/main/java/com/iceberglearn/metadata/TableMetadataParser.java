package com.iceberglearn.metadata;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/** Reads and writes the JSON encoding used for TableMetadata in this project. */
public final class TableMetadataParser {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private TableMetadataParser() {
    }

    public static TableMetadata read(Path path) throws IOException {
        return OBJECT_MAPPER.readValue(path.toFile(), TableMetadata.class);
    }

    public static void write(Path path, TableMetadata metadata, OpenOption... options) throws IOException {
        try (OutputStream output = Files.newOutputStream(path, options)) {
            OBJECT_MAPPER.writeValue(output, metadata);
        }
    }
}
