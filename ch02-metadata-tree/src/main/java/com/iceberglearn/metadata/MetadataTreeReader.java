package com.iceberglearn.metadata;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;

/** Reads every metadata layer from its physical file. */
public class MetadataTreeReader {

    private static final Pattern METADATA_VERSION = Pattern.compile("v(\\d+)\\.metadata\\.json");
    private final ObjectMapper objectMapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public TableMetadata readMetadata(String tableDir) throws IOException {
        Path metadataDir = Paths.get(tableDir, "metadata");
        Path latest;
        try (var files = Files.list(metadataDir)) {
            latest = files
                    .filter(path -> metadataVersion(path) >= 0)
                    .max(Comparator.comparingInt(this::metadataVersion))
                    .orElseThrow(() -> new IllegalStateException("No metadata file found in: " + metadataDir));
        }
        return objectMapper.readValue(latest.toFile(), TableMetadata.class);
    }

    public ManifestList readManifestList(String tableDir, String relativePath) throws IOException {
        return objectMapper.readValue(resolve(tableDir, relativePath).toFile(), ManifestList.class);
    }

    public ManifestEntries readManifest(String tableDir, String relativePath) throws IOException {
        return objectMapper.readValue(resolve(tableDir, relativePath).toFile(), ManifestEntries.class);
    }

    public List<DataFile> listDataFiles(String tableDir, long snapshotId) throws IOException {
        TableMetadata metadata = readMetadata(tableDir);
        Snapshot snapshot = metadata.snapshot(snapshotId);
        ManifestList manifestList = readManifestList(tableDir, snapshot.manifestListLocation());

        List<DataFile> liveFiles = new ArrayList<>();
        for (ManifestFile manifest : manifestList.manifests()) {
            ManifestEntries entries = readManifest(tableDir, manifest.path());
            for (ManifestEntry entry : entries.entries()) {
                if (entry.isLive()) {
                    liveFiles.add(entry.dataFile());
                }
            }
        }
        return liveFiles;
    }

    public void printMetadataTree(String tableDir, long snapshotId) throws IOException {
        TableMetadata metadata = readMetadata(tableDir);
        Snapshot snapshot = metadata.snapshot(snapshotId);
        ManifestList manifestList = readManifestList(tableDir, snapshot.manifestListLocation());

        System.out.printf("%nTableMetadata: format-v%d, current-snapshot=%d%n",
                metadata.formatVersion(), metadata.currentSnapshotId());
        System.out.printf("  Snapshot: id=%d, parent=%s%n", snapshot.snapshotId(), snapshot.parentId());
        System.out.println("    ManifestList: " + snapshot.manifestListLocation());
        for (ManifestFile manifest : manifestList.manifests()) {
            System.out.printf("      Manifest: %s, content=%s, added=%d%n",
                    manifest.path(), manifest.content(), manifest.addedFilesCount());
            ManifestEntries entries = readManifest(tableDir, manifest.path());
            for (ManifestEntry entry : entries.entries()) {
                System.out.printf("        %s DataFile: %s (%d rows)%n",
                        entry.status(), entry.dataFile().path(), entry.dataFile().recordCount());
            }
        }
    }

    private Path resolve(String tableDir, String relativePath) {
        return Paths.get(tableDir).resolve(relativePath);
    }

    private int metadataVersion(Path path) {
        Matcher matcher = METADATA_VERSION.matcher(path.getFileName().toString());
        return matcher.matches() ? Integer.parseInt(matcher.group(1)) : -1;
    }
}
