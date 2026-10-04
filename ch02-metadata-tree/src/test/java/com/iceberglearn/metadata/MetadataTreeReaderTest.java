package com.iceberglearn.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.schema.Schema;

class MetadataTreeReaderTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tableDir;

    @Test
    void readsTwoSnapshotsThroughReusedPhysicalManifest() throws IOException {
        Path metadataDir = Files.createDirectories(tableDir.resolve("metadata"));
        DataFile dataFile = DataFile.builder("data/file.parquet")
                .recordCount(10)
                .fileSizeInBytes(100)
                .lowerBounds(Map.of(1, 1L))
                .upperBounds(Map.of(1, 10L))
                .build();

        String manifestPath = "metadata/manifest-a.json";
        mapper.writeValue(
                tableDir.resolve(manifestPath).toFile(),
                new ManifestEntries(List.of(
                        new ManifestEntry(ManifestEntry.Status.ADDED, 1L, dataFile))));

        ManifestFile manifest = ManifestFile.builder(manifestPath)
                .length(Files.size(tableDir.resolve(manifestPath)))
                .sequenceNumber(1)
                .minSequenceNumber(1)
                .snapshotId(1L)
                .addedFilesCount(1)
                .addedRowsCount(10L)
                .build();
        String manifestListPath = "metadata/snap-1.manifest-list.json";
        mapper.writeValue(
                tableDir.resolve(manifestListPath).toFile(),
                new ManifestList(List.of(manifest)));

        Snapshot snapshot = Snapshot.builder(1)
                .timestampMillis(1)
                .manifestListLocation(manifestListPath)
                .sequenceNumber(1)
                .operation("append")
                .build();
        DataFile secondDataFile = DataFile.builder("data/file-2.parquet")
                .recordCount(5)
                .fileSizeInBytes(50)
                .lowerBounds(Map.of(1, 11L))
                .upperBounds(Map.of(1, 15L))
                .build();
        String secondManifestPath = "metadata/manifest-b.json";
        mapper.writeValue(
                tableDir.resolve(secondManifestPath).toFile(),
                new ManifestEntries(List.of(
                        new ManifestEntry(ManifestEntry.Status.ADDED, 2L, secondDataFile))));
        ManifestFile secondManifest = ManifestFile.builder(secondManifestPath)
                .length(Files.size(tableDir.resolve(secondManifestPath)))
                .sequenceNumber(2)
                .minSequenceNumber(2)
                .snapshotId(2L)
                .addedFilesCount(1)
                .addedRowsCount(5L)
                .build();
        String secondManifestListPath = "metadata/snap-2.manifest-list.json";
        mapper.writeValue(
                tableDir.resolve(secondManifestListPath).toFile(),
                new ManifestList(List.of(manifest, secondManifest)));
        Snapshot secondSnapshot = Snapshot.builder(2)
                .parentId(1L)
                .timestampMillis(2)
                .manifestListLocation(secondManifestListPath)
                .sequenceNumber(2)
                .operation("append")
                .build();

        TableMetadata firstMetadata = new TableMetadata(
                2, "table-uuid", tableDir.toString(), 1, 0,
                List.of(Schema.userSchema()), 1, List.of(snapshot));
        mapper.writeValue(metadataDir.resolve("v1.metadata.json").toFile(), firstMetadata);
        TableMetadata secondMetadata = new TableMetadata(
                2, "table-uuid", tableDir.toString(), 2, 0,
                List.of(Schema.userSchema()), 2, List.of(snapshot, secondSnapshot));
        mapper.writeValue(metadataDir.resolve("v2.metadata.json").toFile(), secondMetadata);

        var metadataJson = mapper.readTree(metadataDir.resolve("v2.metadata.json").toFile());
        var secondSnapshotJson = metadataJson.path("snapshots").get(1);
        assertFalse(secondSnapshotJson.has("operation"));
        assertEquals("append", secondSnapshotJson.path("summary").path("operation").asText());
        assertFalse(mapper.readTree(tableDir.resolve(secondManifestListPath).toFile()).has("path"));

        MetadataTreeReader reader = new MetadataTreeReader();
        assertEquals(List.of(dataFile), reader.listDataFiles(tableDir.toString(), 1));
        assertEquals(List.of(dataFile, secondDataFile), reader.listDataFiles(tableDir.toString(), 2));

        ManifestList firstList = reader.readManifestList(tableDir.toString(), manifestListPath);
        ManifestList secondList = reader.readManifestList(tableDir.toString(), secondManifestListPath);
        assertEquals(firstList.manifests().get(0).path(), secondList.manifests().get(0).path());

        Files.delete(tableDir.resolve(manifestPath));
        assertThrows(IOException.class, () -> reader.listDataFiles(tableDir.toString(), 1));
        assertThrows(IOException.class, () -> reader.listDataFiles(tableDir.toString(), 2));
    }
}
