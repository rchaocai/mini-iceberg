package com.iceberglearn.catalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.iceberglearn.exceptions.AlreadyExistsException;
import com.iceberglearn.exceptions.NoSuchTableException;
import com.iceberglearn.exceptions.ValidationException;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.metadata.TableMetadataParser;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;





class FileSystemCatalogTest {

    @TempDir
    Path warehouse;

    @Test
    void createsAndLoadsAnEmptyTableByIdentifier() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        TableIdentifier identifier = TableIdentifier.of("analytics", "events");

        Table created = catalog.createTable(identifier, Schema.userSchema());
        Path tableLocation = warehouse.resolve("analytics/events").toAbsolutePath().normalize();

        assertEquals("analytics.events", created.name());
        assertEquals(tableLocation.toString(), created.location());
        assertEquals(Schema.userSchema(), created.schema());
        assertNull(created.metadata().currentSnapshotId());
        assertTrue(Files.isRegularFile(tableLocation.resolve("metadata/v1.metadata.json")));
        assertEquals(
                "1",
                readString(tableLocation.resolve("metadata/version-hint.text")));

        Table loaded = catalog.loadTable(TableIdentifier.parse("analytics.events"));
        assertEquals(created.metadata().tableUuid(), loaded.metadata().tableUuid());
        assertEquals("v1.metadata.json", Path.of(loaded.metadataFileLocation()).getFileName().toString());
        assertThrows(AlreadyExistsException.class, () -> catalog.createTable(identifier, Schema.userSchema()));
    }

    @Test
    void followsNewerContiguousVersionsWhenHintIsStale() throws IOException {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        TableIdentifier identifier = TableIdentifier.of("events");
        Table table = catalog.createTable(identifier, Schema.userSchema());
        Path metadataDir = Path.of(table.location()).resolve("metadata");

        TableMetadata version2 = withSequenceNumber(table.metadata(), 2);
        TableMetadataParser.write(
                metadataDir.resolve("v2.metadata.json"),
                version2,
                StandardOpenOption.CREATE_NEW);
        Files.writeString(metadataDir.resolve("version-hint.text"), "1");

        Table loaded = catalog.loadTable(identifier);
        assertEquals(2, loaded.metadata().lastSequenceNumber());
        assertEquals("v2.metadata.json", Path.of(loaded.metadataFileLocation()).getFileName().toString());
    }

    @Test
    void currentUsesCacheUntilRefreshIsRequested() throws IOException {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());
        Path metadataDir = Path.of(table.location()).resolve("metadata");

        TableMetadata version2 = withSequenceNumber(table.metadata(), 2);
        TableMetadataParser.write(
                metadataDir.resolve("v2.metadata.json"),
                version2,
                StandardOpenOption.CREATE_NEW);

        assertEquals(0, table.metadata().lastSequenceNumber());
        assertEquals(2, table.refresh().lastSequenceNumber());
        assertEquals(2, table.metadata().lastSequenceNumber());
    }

    @Test
    void scansNumericVersionsWhenHintCannotBeRead() throws IOException {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());
        Path metadataDir = Path.of(table.location()).resolve("metadata");

        TableMetadata version9 = withSequenceNumber(table.metadata(), 9);
        TableMetadata version10 = withSequenceNumber(table.metadata(), 10);
        TableMetadataParser.write(
                metadataDir.resolve("v9.metadata.json"),
                version9,
                StandardOpenOption.CREATE_NEW);
        TableMetadataParser.write(
                metadataDir.resolve("v10.metadata.json"),
                version10,
                StandardOpenOption.CREATE_NEW);
        Files.writeString(metadataDir.resolve("version-hint.text"), "not-a-number");

        Table loaded = catalog.loadTable(TableIdentifier.of("events"));
        assertEquals(10, loaded.metadata().lastSequenceNumber());
        assertEquals("v10.metadata.json", Path.of(loaded.metadataFileLocation()).getFileName().toString());
    }

    @Test
    void rejectsAHintThatPointsAtAMissingMetadataFile() throws IOException {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());
        Path hint = Path.of(table.location()).resolve("metadata/version-hint.text");
        Files.writeString(hint, "3");

        ValidationException exception = assertThrows(
                ValidationException.class,
                () -> catalog.loadTable(TableIdentifier.of("events")));
        assertTrue(exception.getMessage().contains("version 3"));
    }

    @Test
    void aDirectoryWithoutMetadataIsNotATableAndDropRemovesATable() throws IOException {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        TableIdentifier identifier = TableIdentifier.of("analytics", "events");
        Files.createDirectories(warehouse.resolve("analytics/events"));

        assertFalse(catalog.tableExists(identifier));
        assertThrows(NoSuchTableException.class, () -> catalog.loadTable(identifier));

        catalog.createTable(identifier, Schema.userSchema());
        assertTrue(catalog.tableExists(identifier));
        assertTrue(catalog.dropTable(identifier));
        assertFalse(catalog.tableExists(identifier));
        assertFalse(catalog.dropTable(identifier));
    }

    @Test
    void namespaceDefensivelyCopiesItsLevels() {
        String[] levels = {"prod", "analytics"};
        Namespace namespace = Namespace.of(levels);
        levels[0] = "changed";

        String[] returned = namespace.levels();
        returned[1] = "changed";

        assertEquals("prod.analytics", namespace.toString());
        assertEquals(TableIdentifier.of("prod", "analytics", "events"),
                TableIdentifier.parse("prod.analytics.events"));
    }

    private static TableMetadata withSequenceNumber(TableMetadata base, long sequenceNumber) {
        return new TableMetadata(
                base.formatVersion(),
                base.tableUuid(),
                base.location(),
                sequenceNumber,
                base.currentSchemaId(),
                base.schemas(),
                base.currentSnapshotId(),
                base.snapshots());
    }

    private static String readString(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
