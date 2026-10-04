package com.iceberglearn.operations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.iceberglearn.SnapshotIdGeneratorUtil;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestFiles;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;




/** AppendFiles implementation that prepares a snapshot before committing table metadata. */
public class MergeAppend implements AppendFiles {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final TableOperations operations;
    private final List<DataFile> files = new ArrayList<>();

    private TableMetadata base;
    private TableMetadata pendingMetadata;
    private boolean committed;

    public MergeAppend(TableOperations operations) {
        this.operations = operations;
    }

    @Override
    public AppendFiles appendFile(DataFile file) {
        if (pendingMetadata != null) {
            throw new IllegalStateException("Cannot add files after commit has started");
        }
        files.add(file);
        return this;
    }

    @Override
    public void commit() {
        if (committed) {
            throw new IllegalStateException("Append has already been committed");
        }
        if (files.isEmpty()) {
            return;
        }
        if (pendingMetadata == null) {
            prepareSnapshot();
        }

        operations.commit(base, pendingMetadata);
        committed = true;
    }

    private void prepareSnapshot() {
        base = operations.refresh();
        long snapshotId = SnapshotIdGeneratorUtil.generateSnapshotID();
        long sequenceNumber = base.lastSequenceNumber() + 1;

        String manifestLocation = "metadata/manifest-" + UUID.randomUUID() + ".json";
        List<ManifestEntry> entries = files.stream()
                .map(file -> new ManifestEntry(ManifestEntry.Status.ADDED, snapshotId, file))
                .toList();
        writeJson(base.location(), manifestLocation, new ManifestEntries(entries));
        System.out.println("  [append] write manifest: " + manifestLocation);

        Path manifestPath = Path.of(base.location()).resolve(manifestLocation);
        ManifestFile manifest;
        try {
            manifest = ManifestFile.builder(manifestLocation)
                    .length(Files.size(manifestPath))
                    .partitionSpecId(0)
                    .sequenceNumber(sequenceNumber)
                    .minSequenceNumber(sequenceNumber)
                    .snapshotId(snapshotId)
                    .addedFilesCount(files.size())
                    .addedRowsCount(files.stream().mapToLong(DataFile::recordCount).sum())
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to inspect manifest " + manifestLocation, e);
        }

        List<ManifestFile> manifests = currentManifests(base);
        manifests.add(manifest);
        String manifestListLocation = "metadata/snap-" + snapshotId + ".manifest-list.json";
        writeJson(base.location(), manifestListLocation, new ManifestList(manifests));
        System.out.println("  [append] write manifest list: " + manifestListLocation);

        Snapshot snapshot = Snapshot.builder(snapshotId)
                .parentId(base.currentSnapshotId())
                .timestampMillis(System.currentTimeMillis())
                .manifestListLocation(manifestListLocation)
                .sequenceNumber(sequenceNumber)
                .operation("append")
                .summary(Map.of(
                        "added-data-files", Integer.toString(files.size()),
                        "added-records", Long.toString(
                                files.stream().mapToLong(DataFile::recordCount).sum())))
                .build();
        System.out.println("  [append] build snapshot: " + snapshotId);

        List<Snapshot> snapshots = new ArrayList<>(base.snapshots());
        snapshots.add(snapshot);
        pendingMetadata = new TableMetadata(
                base.formatVersion(),
                base.tableUuid(),
                base.location(),
                snapshot.sequenceNumber(),
                base.currentSchemaId(),
                base.schemas(),
                snapshot.snapshotId(),
                snapshots);
    }

    private List<ManifestFile> currentManifests(TableMetadata metadata) {
        Snapshot currentSnapshot = metadata.currentSnapshot();
        if (currentSnapshot == null) {
            return new ArrayList<>();
        }

        try {
            ManifestList manifestList = ManifestFiles.readManifestList(
                    metadata.location(),
                    currentSnapshot.manifestListLocation());
            return new ArrayList<>(manifestList.manifests());
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read manifests for snapshot " + currentSnapshot.snapshotId(), e);
        }
    }

    private void writeJson(String tableLocation, String relativePath, Object value) {
        Path path = Path.of(tableLocation).resolve(relativePath);
        try {
            Files.createDirectories(path.getParent());
            try (OutputStream output = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)) {
                OBJECT_MAPPER.writeValue(output, value);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write " + path, e);
        }
    }
}
