package com.iceberglearn.operations;

import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestFiles;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotLogEntry;
import com.iceberglearn.metadata.TableMetadata;

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

/**
 * AppendFiles implementation that prepares a snapshot before committing table metadata.
 *
 * <p>Extends {@link SnapshotUpdate} so the read-compute-commit cycle is retried with
 * optimistic concurrency: when a concurrent writer publishes first, this append re-reads
 * the new base and rebuilds the manifest and manifest-list against it.
 */
public class MergeAppend extends SnapshotUpdate implements AppendFiles {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final List<DataFile> files = new ArrayList<>();

    public MergeAppend(TableOperations operations) {
        super(operations, "append");
    }

    @Override
    public AppendFiles appendFile(DataFile file) {
        files.add(file);
        return this;
    }

    @Override
    public void commit() {
        if (files.isEmpty()) {
            return;
        }
        // Delegate to SnapshotUpdate.commit(), which owns the OCC retry loop. On every retry
        // it will call apply() again with a freshly refreshed base, so the manifest and
        // manifest-list are rebuilt against the latest known state.
        super.commit();
    }

    @Override
    protected TableMetadata apply(TableMetadata base) {
        long snapshotId = SnapshotIdGeneratorUtil.generateSnapshotID();
        long sequenceNumber = base.lastSequenceNumber() + 1;

        // UUID-based file names so that retries never collide with files written by a previous
        // apply() call. If a retry leaves an earlier manifest behind, it simply becomes an
        // orphan that no metadata ever references — correctness is preserved.
        String manifestLocation = "metadata/manifest-" + snapshotId + "-" + UUID.randomUUID() + ".json";
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
        String manifestListLocation = "metadata/snap-" + snapshotId + "-" + UUID.randomUUID() + ".manifest-list.json";
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
        List<SnapshotLogEntry> snapshotLog = new ArrayList<>(base.snapshotLog());
        snapshotLog.add(new SnapshotLogEntry(snapshot.timestampMillis(), snapshot.snapshotId()));
        return new TableMetadata(
                base.formatVersion(),
                base.tableUuid(),
                base.location(),
                snapshot.sequenceNumber(),
                base.currentSchemaId(),
                base.schemas(),
                snapshot.snapshotId(),
                snapshots,
                snapshotLog);
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
