package com.iceberglearn;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.MetadataTreeReader;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.schema.Schema;

/** Builds and traverses Iceberg's metadata file boundaries using JSON encoding. */
public class Main {

    static final String TABLE_DIR = "./data/ch02/user_table";
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static void main(String[] args) throws Exception {
        System.out.println("=== 第2章：表是一组不可变元数据文件 ===\n");
        cleanup();

        Schema schema = Schema.userSchema();

        System.out.println("[步骤1] 写第一批 3 个 Parquet 文件");
        List<DataFile> firstBatch = writeDataFiles(0, 3);
        ManifestFile manifestA = writeManifest("metadata/manifest-a.json", 1, firstBatch);
        String manifestListPath1 = "metadata/snap-1.manifest-list.json";
        writeManifestList(manifestListPath1, List.of(manifestA));
        Snapshot snapshot1 = newSnapshot(1, null, 1, manifestListPath1, firstBatch.size() * 100L);
        TableMetadata metadataV1 = metadata(schema, List.of(snapshot1), snapshot1.snapshotId());
        writeJson("metadata/v1.metadata.json", metadataV1);

        System.out.println("\n[步骤2] 再追加 1 个 Parquet 文件");
        List<DataFile> secondBatch = writeDataFiles(3, 1);
        ManifestFile manifestB = writeManifest("metadata/manifest-b.json", 2, secondBatch);

        // The new list references manifest A again. It does not copy manifest A's entries.
        String manifestListPath2 = "metadata/snap-2.manifest-list.json";
        writeManifestList(manifestListPath2, List.of(manifestA, manifestB));
        Snapshot snapshot2 = newSnapshot(2, 1L, 2, manifestListPath2, secondBatch.size() * 100L);
        TableMetadata metadataV2 = metadata(schema, List.of(snapshot1, snapshot2), snapshot2.snapshotId());
        writeJson("metadata/v2.metadata.json", metadataV2);

        System.out.println("\n[步骤3] 从磁盘逐层读取两个快照");
        MetadataTreeReader reader = new MetadataTreeReader();
        reader.printMetadataTree(TABLE_DIR, 1);
        reader.printMetadataTree(TABLE_DIR, 2);

        List<DataFile> oldFiles = reader.listDataFiles(TABLE_DIR, 1);
        List<DataFile> currentFiles = reader.listDataFiles(TABLE_DIR, 2);
        System.out.printf("%n快照 1: %d 个文件，快照 2: %d 个文件%n", oldFiles.size(), currentFiles.size());
        System.out.println("Manifest 复用: snapshot-1 和 snapshot-2 都引用 " + manifestA.path());
        System.out.println("\n结果：新快照写入新 manifest-list，同时继续引用旧 manifest。");
    }

    private static TableMetadata metadata(
            Schema schema, List<Snapshot> snapshots, long currentSnapshotId) {
        return new TableMetadata(
                2,
                UUID.nameUUIDFromBytes(TABLE_DIR.getBytes(StandardCharsets.UTF_8)).toString(),
                TABLE_DIR,
                snapshots.size(),
                schema.schemaId(),
                List.of(schema),
                currentSnapshotId,
                snapshots);
    }

    private static Snapshot newSnapshot(
            long snapshotId,
            Long parentId,
            long sequenceNumber,
            String manifestListLocation,
            long addedRecords) {
        return Snapshot.builder(snapshotId)
                .parentId(parentId)
                .timestampMillis(Instant.now().toEpochMilli())
                .manifestListLocation(manifestListLocation)
                .sequenceNumber(sequenceNumber)
                .operation("append")
                .summary(Map.of("added-records", Long.toString(addedRecords)))
                .build();
    }

    private static List<DataFile> writeDataFiles(int firstFileIndex, int fileCount) throws IOException {
        Path dataDir = Paths.get(TABLE_DIR, "data");
        Files.createDirectories(dataDir);
        List<DataFile> dataFiles = new ArrayList<>();

        for (int i = firstFileIndex; i < firstFileIndex + fileCount; i++) {
            List<User> users = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                int userId = i * 100 + j + 1;
                users.add(new User(
                        userId,
                        "user_" + userId,
                        20 + (userId % 40),
                        "user" + userId + "@example.com"));
            }

            String fileName = "part-" + String.format("%03d", i) + ".parquet";
            Path physicalPath = dataDir.resolve(fileName);
            ParquetFileWriter.writeUsers(users, physicalPath.toString());

            Map<Integer, Long> lowerBounds = new HashMap<>();
            lowerBounds.put(1, (long) i * 100 + 1);
            Map<Integer, Long> upperBounds = new HashMap<>();
            upperBounds.put(1, (long) (i + 1) * 100);

            DataFile dataFile = DataFile.builder("data/" + fileName)
                    .format("parquet")
                    .recordCount(users.size())
                    .fileSizeInBytes(Files.size(physicalPath))
                    .lowerBounds(lowerBounds)
                    .upperBounds(upperBounds)
                    .build();
            dataFiles.add(dataFile);
            System.out.printf("  写入 %s, id=[%d,%d]%n", dataFile.path(), i * 100 + 1, (i + 1) * 100);
        }

        return dataFiles;
    }

    private static ManifestFile writeManifest(
            String relativePath, long snapshotId, List<DataFile> dataFiles) throws IOException {
        List<ManifestEntry> entries = dataFiles.stream()
                .map(file -> new ManifestEntry(ManifestEntry.Status.ADDED, snapshotId, file))
                .toList();
        writeJson(relativePath, new ManifestEntries(entries));

        Path physicalPath = Paths.get(TABLE_DIR).resolve(relativePath);
        return ManifestFile.builder(relativePath)
                .length(Files.size(physicalPath))
                .partitionSpecId(0)
                .sequenceNumber(snapshotId)
                .minSequenceNumber(snapshotId)
                .snapshotId(snapshotId)
                .addedFilesCount(dataFiles.size())
                .addedRowsCount(dataFiles.stream().mapToLong(DataFile::recordCount).sum())
                .build();
    }

    private static void writeManifestList(
            String relativePath, List<ManifestFile> manifests) throws IOException {
        writeJson(relativePath, new ManifestList(manifests));
    }

    private static void writeJson(String relativePath, Object value) throws IOException {
        Path path = Paths.get(TABLE_DIR).resolve(relativePath);
        Files.createDirectories(path.getParent());
        OBJECT_MAPPER.writeValue(path.toFile(), value);
        System.out.println("  写入元数据: " + relativePath);
    }

    private static void cleanup() throws IOException {
        Path tablePath = Paths.get(TABLE_DIR);
        if (!Files.exists(tablePath)) {
            return;
        }

        try (var paths = Files.walk(tablePath)) {
            for (Path path : paths.sorted((left, right) -> right.compareTo(left)).toList()) {
                Files.delete(path);
            }
        }
    }
}
