package com.iceberglearn.operations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.exceptions.SimulatedCrashException;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotLogEntry;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.metadata.TableMetadataParser;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;





class AtomicCommitTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    @TempDir
    Path warehouse;

    @Test
    void filesAreInvisibleUntilMetadataCommit() throws IOException {
        Fixture fixture = new Fixture(warehouse);
        TableMetadata next = fixture.metadataWithSnapshot(1);

        Table beforeCommit = fixture.reload();
        assertNull(beforeCommit.currentSnapshot());
        assertTrue(beforeCommit.newScan().planFiles().isEmpty());

        fixture.operations().commit(fixture.table().metadata(), next);

        Table afterCommit = fixture.reload();
        assertEquals(1, afterCommit.currentSnapshot().snapshotId());
        assertEquals(List.of("data/a.parquet"), paths(afterCommit.newScan().planFiles()));
    }

    @Test
    void temporaryMetadataIsIgnoredAfterCrash() throws IOException {
        Fixture fixture = new Fixture(warehouse);
        TableMetadata next = fixture.metadataWithSnapshot(1);

        Path tempMetadata = fixture.operations().writeTemporaryMetadata(next);

        assertTrue(Files.exists(tempMetadata));
        assertFalse(Files.exists(fixture.metadataDirectory().resolve("v2.metadata.json")));
        assertNull(fixture.reload().currentSnapshot());
    }

    @Test
    void atomicRenamePublishesACompleteMetadataVersion() throws IOException {
        Fixture fixture = new Fixture(warehouse);
        TableMetadata next = fixture.metadataWithSnapshot(1);

        fixture.operations().commit(fixture.table().metadata(), next);

        Path committedFile = fixture.metadataDirectory().resolve("v2.metadata.json");
        assertTrue(Files.exists(committedFile));
        assertEquals(next, TableMetadataParser.read(committedFile));
    }

    @Test
    void versionHintIsOnlyAnOptimization() throws IOException {
        Fixture fixture = new Fixture(warehouse);
        TableMetadata next = fixture.metadataWithSnapshot(1);
        fixture.operations().commit(fixture.table().metadata(), next);

        Files.delete(fixture.operations().versionHintFile());

        Table recovered = fixture.reload();
        assertEquals(1, recovered.currentSnapshot().snapshotId());
        assertTrue(recovered.metadataFileLocation().endsWith("v2.metadata.json"));
    }

    @Test
    void appendFilesBuildsAndCommitsTheMetadataChain() {
        Fixture fixture = new Fixture(warehouse);

        fixture.table().newAppend()
                .appendFile(file("data/a.parquet"))
                .commit();

        Table committed = fixture.reload();
        long committedSnapshotId = committed.currentSnapshot().snapshotId();
        assertEquals(committedSnapshotId, committed.currentSnapshot().snapshotId());
        assertEquals(List.of("data/a.parquet"), paths(committed.newScan().planFiles()));
    }

    @Test
    void theSameAppendCanRetryAfterCrashingBeforeRename() {
        Fixture fixture = new Fixture(warehouse);
        Table table = new Table(
                fixture.identifier(),
                new FailOnceBeforeRenameTableOperations(Path.of(fixture.table().location())));
        AppendFiles append = table.newAppend().appendFile(file("data/a.parquet"));

        org.junit.jupiter.api.Assertions.assertThrows(SimulatedCrashException.class, append::commit);
        assertNull(fixture.reload().currentSnapshot());

        append.commit();

        long retrySnapshotId = fixture.reload().currentSnapshot().snapshotId();
        assertEquals(retrySnapshotId, fixture.reload().currentSnapshot().snapshotId());
    }

    private static List<String> paths(List<DataFile> files) {
        return files.stream().map(DataFile::path).toList();
    }

    private static DataFile file(String path) {
        return DataFile.builder(path)
                .recordCount(10)
                .lowerBounds(Map.of(1, 1L))
                .upperBounds(Map.of(1, 10L))
                .build();
    }

    private static class Fixture {
        private final FileSystemCatalog catalog;
        private final TableIdentifier identifier = TableIdentifier.of("events");
        private final Table table;

        private Fixture(Path warehouse) {
            this.catalog = new FileSystemCatalog(warehouse.toString());
            this.table = catalog.createTable(identifier, Schema.userSchema());
        }

        private Table table() {
            return table;
        }

        private TableIdentifier identifier() {
            return identifier;
        }

        private FileSystemTableOperations operations() {
            return (FileSystemTableOperations) table.operations();
        }

        private Table reload() {
            return catalog.loadTable(identifier);
        }

        private Path metadataDirectory() {
            return Path.of(table.location()).resolve("metadata");
        }

        private TableMetadata metadataWithSnapshot(long snapshotId) throws IOException {
            DataFile dataFile = DataFile.builder("data/a.parquet")
                    .recordCount(10)
                    .lowerBounds(Map.of(1, 1L))
                    .upperBounds(Map.of(1, 10L))
                    .build();

            String manifestLocation = "metadata/manifest-" + snapshotId + ".json";
            writeJson(
                    manifestLocation,
                    new ManifestEntries(List.of(
                            new ManifestEntry(ManifestEntry.Status.ADDED, snapshotId, dataFile))));

            ManifestFile manifest = ManifestFile.builder(manifestLocation)
                    .length(Files.size(Path.of(table.location()).resolve(manifestLocation)))
                    .sequenceNumber(snapshotId)
                    .minSequenceNumber(snapshotId)
                    .snapshotId(snapshotId)
                    .addedFilesCount(1)
                    .addedRowsCount(dataFile.recordCount())
                    .build();

            String manifestListLocation = "metadata/snap-" + snapshotId + ".manifest-list.json";
            writeJson(manifestListLocation, new ManifestList(List.of(manifest)));

            Snapshot snapshot = Snapshot.builder(snapshotId)
                    .timestampMillis(1_000 + snapshotId)
                    .manifestListLocation(manifestListLocation)
                    .sequenceNumber(snapshotId)
                    .operation("append")
                    .build();

            TableMetadata base = table.metadata();
            List<Snapshot> snapshots = new ArrayList<>(base.snapshots());
            snapshots.add(snapshot);
            return new TableMetadata(
                    base.formatVersion(),
                    base.tableUuid(),
                    base.location(),
                    snapshot.sequenceNumber(),
                    base.currentSchemaId(),
                    base.schemas(),
                    snapshot.snapshotId(),
                    snapshots,
                    List.of(new SnapshotLogEntry(
                            snapshot.timestampMillis(), snapshot.snapshotId())));
        }

        private void writeJson(String relativePath, Object value) throws IOException {
            Path path = Path.of(table.location()).resolve(relativePath);
            OBJECT_MAPPER.writeValue(path.toFile(), value);
        }
    }
}
