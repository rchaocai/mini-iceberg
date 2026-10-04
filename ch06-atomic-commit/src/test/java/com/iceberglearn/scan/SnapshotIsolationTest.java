package com.iceberglearn.scan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.metadata.TableMetadataParser;
import com.iceberglearn.operations.FileSystemTableOperations;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;





class SnapshotIsolationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    @TempDir
    Path warehouse;

    @Test
    void anEmptyTablePlansNoFiles() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());

        TableScan scan = table.newScan();

        assertNull(scan.snapshot());
        assertTrue(scan.planFiles().isEmpty());
    }

    @Test
    void useSnapshotReturnsANewScanForHistoricalFiles() throws IOException {
        Fixture fixture = new Fixture(warehouse);
        fixture.publish(1, null, List.of(), List.of(file("data/a.parquet"), file("data/b.parquet")));
        fixture.publish(2, 1L, fixture.manifests(), List.of(file("data/c.parquet")));

        TableScan current = fixture.table().newScan();
        TableScan historical = current.useSnapshot(1);

        assertEquals(2, current.snapshot().snapshotId());
        assertEquals(List.of("data/a.parquet", "data/b.parquet", "data/c.parquet"),
                paths(current.planFiles()));
        assertEquals(1, historical.snapshot().snapshotId());
        assertEquals(List.of("data/a.parquet", "data/b.parquet"),
                paths(historical.planFiles()));
        assertEquals(2, current.snapshot().snapshotId());
    }

    @Test
    void aBoundScanKeepsItsSnapshotAfterTheTableRefreshes() throws IOException {
        Fixture fixture = new Fixture(warehouse);
        fixture.publish(1, null, List.of(), List.of(file("data/a.parquet"), file("data/b.parquet")));
        fixture.publish(2, 1L, fixture.manifests(), List.of(file("data/c.parquet")));
        TableScan bound = fixture.table().newScan().useSnapshot(2);

        fixture.publish(3, 2L, fixture.manifests(), List.of(file("data/d.parquet")));

        assertEquals(3, fixture.table().currentSnapshot().snapshotId());
        assertEquals(2, bound.snapshot().snapshotId());
        assertEquals(3, bound.planFiles().size());
        assertEquals(4, fixture.table().newScan().planFiles().size());
    }

    @Test
    void rejectsUnknownSnapshotsAndSnapshotOverrides() throws IOException {
        Fixture fixture = new Fixture(warehouse);
        fixture.publish(1, null, List.of(), List.of(file("data/a.parquet")));

        IllegalArgumentException missing = assertThrows(
                IllegalArgumentException.class,
                () -> fixture.table().newScan().useSnapshot(99));
        assertTrue(missing.getMessage().contains("99"));

        TableScan bound = fixture.table().newScan().useSnapshot(1);
        IllegalArgumentException override = assertThrows(
                IllegalArgumentException.class,
                () -> bound.useSnapshot(1));
        assertTrue(override.getMessage().contains("already set"));
    }

    private static DataFile file(String path) {
        return DataFile.builder(path)
                .recordCount(100)
                .lowerBounds(Map.of(1, 1L))
                .upperBounds(Map.of(1, 100L))
                .build();
    }

    private static List<String> paths(List<DataFile> files) {
        return files.stream().map(DataFile::path).toList();
    }

    private static class Fixture {
        private final Table table;
        private final List<ManifestFile> manifests = new ArrayList<>();
        private int metadataVersion = 1;

        private Fixture(Path warehouse) {
            FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
            this.table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());
        }

        private Table table() {
            return table;
        }

        private List<ManifestFile> manifests() {
            return List.copyOf(manifests);
        }

        private void publish(
                long snapshotId,
                Long parentId,
                List<ManifestFile> previousManifests,
                List<DataFile> addedFiles) throws IOException {
            String manifestLocation = "metadata/manifest-" + snapshotId + ".json";
            List<ManifestEntry> entries = addedFiles.stream()
                    .map(file -> new ManifestEntry(ManifestEntry.Status.ADDED, snapshotId, file))
                    .toList();
            writeJson(manifestLocation, new ManifestEntries(entries));

            Path manifestPath = Path.of(table.location()).resolve(manifestLocation);
            ManifestFile manifest = ManifestFile.builder(manifestLocation)
                    .length(Files.size(manifestPath))
                    .sequenceNumber(snapshotId)
                    .minSequenceNumber(snapshotId)
                    .snapshotId(snapshotId)
                    .addedFilesCount(addedFiles.size())
                    .addedRowsCount(addedFiles.stream().mapToLong(DataFile::recordCount).sum())
                    .build();

            List<ManifestFile> nextManifests = new ArrayList<>(previousManifests);
            nextManifests.add(manifest);
            String manifestListLocation = "metadata/snap-" + snapshotId + ".manifest-list.json";
            writeJson(manifestListLocation, new ManifestList(nextManifests));

            Snapshot snapshot = Snapshot.builder(snapshotId)
                    .parentId(parentId)
                    .timestampMillis(1_000 + snapshotId)
                    .manifestListLocation(manifestListLocation)
                    .sequenceNumber(snapshotId)
                    .operation("append")
                    .build();

            TableMetadata base = table.metadata();
            List<Snapshot> snapshots = new ArrayList<>(base.snapshots());
            snapshots.add(snapshot);
            TableMetadata next = new TableMetadata(
                    base.formatVersion(),
                    base.tableUuid(),
                    base.location(),
                    snapshot.sequenceNumber(),
                    base.currentSchemaId(),
                    base.schemas(),
                    snapshot.snapshotId(),
                    snapshots);

            metadataVersion += 1;
            Path metadataDir = Path.of(table.location()).resolve("metadata");
            TableMetadataParser.write(
                    metadataDir.resolve("v" + metadataVersion + ".metadata.json"),
                    next,
                    StandardOpenOption.CREATE_NEW);
            Files.writeString(
                    metadataDir.resolve(FileSystemTableOperations.VERSION_HINT_FILENAME),
                    Integer.toString(metadataVersion));
            table.refresh();

            manifests.clear();
            manifests.addAll(nextManifests);
        }

        private void writeJson(String relativePath, Object value) throws IOException {
            Path path = Path.of(table.location()).resolve(relativePath);
            OBJECT_MAPPER.writeValue(path.toFile(), value);
        }
    }
}
