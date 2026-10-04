package com.iceberglearn.schema;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.io.ParquetReader;
import com.iceberglearn.io.ParquetSchemaUtil;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.operations.FileSystemTableOperations;
import com.iceberglearn.operations.TableOperations;
import com.iceberglearn.operations.UpdateSchema;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.table.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage of schema evolution: adding, renaming and deleting columns must append
 * a new schema under a fresh id, switch the current pointer, leave every old data file
 * untouched, and read every file back correctly matched by field id — renamed columns land
 * under their new names, added columns read as null from old files, deleted columns are
 * never read.
 *
 * <p>Mirrors the core scenarios of the real Iceberg TestSchemaUpdate: new columns draw
 * table-level fresh ids (never recycling dropped ids), renames keep the field id, and a
 * conflicting schema-update commit fails fast instead of retrying.
 */
class SchemaEvolutionTest {

    private static final int ID = 1;
    private static final int NAME = 2;
    private static final int AGE = 3;
    private static final int EMAIL = 4;

    @TempDir
    Path warehouse;

    @Test
    void updateSchemaAppendsNewSchemaAndSwitchesCurrentWithoutCreatingSnapshot() {
        Table table = createTable();
        writeUsersAndCommit(table, List.of(user(1, "alice", 30, "alice@demo.com")));
        long snapshotIdBefore = table.currentSnapshot().snapshotId();
        int snapshotsBefore = countSnapshots(table);

        table.updateSchema().addColumn("address", "string").commit();
        table.refresh();

        assertEquals(2, table.metadata().schemas().size());
        assertEquals(1, table.metadata().currentSchemaId());
        // A schema update is a pure metadata change: snapshots and the current pointer are
        // copied verbatim.
        assertEquals(snapshotsBefore, countSnapshots(table));
        assertEquals(snapshotIdBefore, table.currentSnapshot().snapshotId());
        // The historical schema is intact and still reachable.
        assertEquals("name", table.metadata().schema(0).findField(NAME).name());
    }

    @Test
    void addColumnDrawsTableLevelFreshIdAndKeepsOldIds() {
        Table table = createTable();
        table.updateSchema().addColumn("address", "string").commit();
        table.refresh();

        Schema evolved = table.schema();
        assertEquals(5, evolved.fields().size());
        // Surviving columns keep their ids; the new column draws lastColumnId + 1 = 5.
        assertEquals(ID, evolved.findField("id").id());
        assertEquals(NAME, evolved.findField("name").id());
        assertEquals(5, evolved.findField("address").id());
        assertTrue(!evolved.findField("address").required(), "added columns are optional");
    }

    @Test
    void addColumnRejectsExistingNameButAllowsTheNameOfADeletedColumn() {
        Table table = createTable();
        UpdateSchema update = table.updateSchema();
        assertThrows(IllegalArgumentException.class, () -> update.addColumn("name", "string"));
        assertThrows(IllegalArgumentException.class,
                () -> table.updateSchema().addColumn("name", "string").addColumn("name", "string"));

        // A dropped name may come back — under a fresh id (mirrors the official rule).
        table.updateSchema().deleteColumn("email").addColumn("email", "string").commit();
        table.refresh();
        assertEquals(5, table.schema().findField("email").id());

        // Names are per-schema, not historical: a name dropped in an EARLIER evolution can
        // come back in a later one — again under a fresh id, never the old one.
        table.updateSchema().deleteColumn("email").commit();
        table.updateSchema().addColumn("email", "string").commit();
        table.refresh();
        assertEquals(6, table.schema().findField("email").id());
    }

    @Test
    void renameColumnKeepsFieldIdTypeAndNullabilityButChangesOnlyName() {
        Table table = createTable();
        table.updateSchema().renameColumn("name", "username").commit();
        table.refresh();

        Schema.NestedField renamed = table.schema().findField("username");
        assertEquals(NAME, renamed.id());
        assertEquals("string", renamed.type());
        assertTrue(renamed.required());
        assertNull(table.schema().findField("name"));
    }

    @Test
    void renameColumnThrowsForMissingColumnAndForDeletedColumn() {
        Table table = createTable();
        assertThrows(IllegalArgumentException.class,
                () -> table.updateSchema().renameColumn("no_such_column", "x"));
        assertThrows(IllegalArgumentException.class, () -> table.updateSchema()
                .deleteColumn("name")
                .renameColumn("name", "username"));
    }

    @Test
    void deleteColumnRemovesItFromTheNewSchemaOnly() {
        Table table = createTable();
        table.updateSchema().deleteColumn("email").commit();
        table.refresh();

        assertNull(table.schema().findField("email"));
        // The old schema keeps the column — history is append-only.
        assertEquals("email", table.metadata().schema(0).findField(EMAIL).name());
    }

    @Test
    void deleteColumnThrowsForMissingColumnAndForColumnWithStagedUpdates() {
        Table table = createTable();
        assertThrows(IllegalArgumentException.class,
                () -> table.updateSchema().deleteColumn("no_such_column"));
        // The official rule: a column with pending updates (here, a rename) cannot be deleted.
        assertThrows(IllegalArgumentException.class, () -> table.updateSchema()
                .renameColumn("name", "username")
                .deleteColumn("name"));
    }

    @Test
    void droppedIdsAreNeverRecycledEvenAfterDeletingTheHighestColumn() {
        Table table = createTable();
        // address gets id 5; email keeps 4.
        table.updateSchema().addColumn("address", "string").commit();
        table.refresh();
        // Drop the two highest columns: per-schema highestFieldId shrinks to 3.
        table.updateSchema().deleteColumn("email").deleteColumn("address").commit();
        table.refresh();
        assertEquals(3, table.schema().highestFieldId());
        assertEquals(5, table.metadata().lastColumnId());

        // phone must draw the table-level 6, NOT the per-schema 4 — 4 still belongs to the
        // dropped email in every file written before the drop.
        table.updateSchema().addColumn("phone", "string").commit();
        table.refresh();
        assertEquals(6, table.schema().findField("phone").id());
    }

    @Test
    void schemaUpdateCommitConflictFailsFastWithoutRetry() {
        Path tableLocation = warehouse.resolve("users");
        TableOperations ops = new FailOnceRenameOperations(tableLocation);
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        catalog.createTable(TableIdentifier.of("users"), schema(), PartitionSpec.unpartitioned());

        Table table = new Table(TableIdentifier.of("users"), ops);
        // Unlike a snapshot update (which retries through SnapshotUpdate), a schema update
        // whose base was concurrently replaced fails immediately — the caller starts over.
        assertThrows(CommitFailedException.class, () ->
                table.updateSchema().addColumn("address", "string").commit());
    }

    @Test
    void metadataJsonRoundTripsMultipleSchemasWithCurrentSchemaId() {
        Table table = createTable();
        writeUsersAndCommit(table, List.of(user(1, "alice", 30, "alice@demo.com")));
        table.updateSchema().addColumn("address", "string").commit();

        // Reload through the catalog: the second metadata file must carry both schemas.
        Table reloaded = new FileSystemCatalog(warehouse.toString())
                .loadTable(TableIdentifier.of("users"));
        assertEquals(2, reloaded.metadata().schemas().size());
        assertEquals(1, reloaded.metadata().currentSchemaId());
        assertEquals(4, reloaded.metadata().schema(0).fields().size());
        assertEquals(5, reloaded.metadata().schema(1).fields().size());
        assertEquals(5, reloaded.metadata().lastColumnId());
    }

    @Test
    void writerEmbedsFieldIdsIntoTheParquetFileSchema() throws IOException {
        Table table = createTable();
        writeUsersAndCommit(table, List.of(user(1, "alice", 30, "alice@demo.com")));
        Path file = Path.of(table.location()).resolve(firstDataFile(table));

        Map<Integer, String> idToName = ParquetSchemaUtil.fieldIdToName(file);
        assertEquals(4, idToName.size());
        assertEquals("id", idToName.get(ID));
        assertEquals("name", idToName.get(NAME));
        assertEquals("age", idToName.get(AGE));
        assertEquals("email", idToName.get(EMAIL));
    }

    @Test
    void renameReadsOldFilesUnderTheNewNameByFieldId() {
        Table table = createTable();
        writeUsersAndCommit(table, List.of(user(1, "alice", 30, "alice@demo.com")));
        table.updateSchema().renameColumn("name", "username").commit();
        table.refresh();

        List<Map<Integer, Object>> rows = readAll(table);
        assertEquals(1, rows.size());
        // The value lives under the renamed field's id — the file was never touched.
        assertEquals("alice", rows.get(0).get(NAME));
        assertEquals(30L, rows.get(0).get(AGE));
    }

    @Test
    void addedColumnReadsNullFromOldFilesAndValuesFromNewFiles() {
        Table table = createTable();
        writeUsersAndCommit(table, List.of(user(1, "alice", 30, "alice@demo.com")));
        table.updateSchema().addColumn("address", "string").commit();
        table.refresh();

        Map<Integer, Object> eve = user(5, "eve", 32, "eve@demo.com");
        eve.put(5, "杭州");
        writeUsersAndCommit(table, List.of(eve));

        List<Map<Integer, Object>> rows = readAll(table);
        assertEquals(2, rows.size());
        assertNull(rows.get(0).get(5));
        assertEquals("杭州", rows.get(1).get(5));
    }

    @Test
    void deletedColumnsAreIgnoredWhenReadingOldFiles() {
        Table table = createTable();
        writeUsersAndCommit(table, List.of(user(1, "alice", 30, "alice@demo.com")));
        Set<String> oldPaths = dataFilePaths(table);

        table.updateSchema().deleteColumn("email").commit();
        table.refresh();

        Schema readSchema = table.schema();
        assertNull(readSchema.findField("email"));
        for (Map<Integer, Object> row : readAll(table)) {
            assertFalseHasField(row, EMAIL);
        }
        // The old file's bytes still carry the email column — untouched.
        for (String path : oldPaths) {
            assertTrue(Files.exists(Path.of(table.location()).resolve(path)));
        }
    }

    @Test
    void oldDataFilesRemainAtSamePathsAcrossFourEvolutions() {
        Table table = createTable();
        writeUsersAndCommit(table, List.of(user(1, "alice", 30, "alice@demo.com")));
        Set<String> oldPaths = dataFilePaths(table);

        table.updateSchema().addColumn("address", "string").commit();
        table.updateSchema().renameColumn("name", "username").commit();
        table.updateSchema().deleteColumn("email").deleteColumn("address").commit();
        table.updateSchema().addColumn("phone", "string").commit();
        table.refresh();

        for (String path : oldPaths) {
            assertTrue(Files.exists(Path.of(table.location()).resolve(path)),
                    "old data file must remain: " + path);
        }
        assertEquals(5, table.metadata().schemas().size());
    }

    // --- helpers ---------------------------------------------------------------

    private static void assertFalseHasField(Map<Integer, Object> row, int fieldId) {
        assertTrue(!row.containsKey(fieldId), "row must not carry deleted field id " + fieldId);
    }

    private Table createTable() {
        return new FileSystemCatalog(warehouse.toString())
                .createTable(TableIdentifier.of("users"), schema(), PartitionSpec.unpartitioned());
    }

    private void writeUsersAndCommit(Table table, List<Map<Integer, Object>> rows) {
        PartitionedWriter writer = new PartitionedWriter(
                table.schema(), table.spec(), Path.of(table.location()).resolve("data"), 2);
        rows.forEach(writer::write);
        AppendFiles append = table.newAppend();
        writer.complete().forEach(append::appendFile);
        append.commit();
    }

    private List<Map<Integer, Object>> readAll(Table table) {
        List<Map<Integer, Object>> rows = new java.util.ArrayList<>();
        for (DataFile file : table.newScan().planFiles()) {
            rows.addAll(ParquetReader.read(table.schema(),
                    Path.of(table.location()).resolve(file.path())));
        }
        return rows;
    }

    private String firstDataFile(Table table) {
        return table.newScan().planFiles().get(0).path();
    }

    private Set<String> dataFilePaths(Table table) {
        Set<String> paths = new HashSet<>();
        for (DataFile file : table.newScan().planFiles()) {
            paths.add(file.path());
        }
        return paths;
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (var ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(NAME, "name", "string"),
                Schema.NestedField.required(AGE, "age", "long"),
                Schema.NestedField.required(EMAIL, "email", "string")));
    }

    /** Keys are field ids, so a column the current schema lacks is simply absent. */
    private static Map<Integer, Object> user(long id, String name, long age, String email) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(ID, id);
        row.put(NAME, name);
        row.put(AGE, age);
        row.put(EMAIL, email);
        return row;
    }

    /**
     * Same shape as the scan package's FailOnceRenameOperations: fail the first rename so
     * the commit looks like a concurrent conflict.
     */
    private static final class FailOnceRenameOperations extends FileSystemTableOperations {
        private boolean shouldFail = true;

        FailOnceRenameOperations(Path tableLocation) {
            super(tableLocation);
        }

        @Override
        protected void renameToFinal(Path source, Path target, int nextVersion) {
            if (shouldFail) {
                shouldFail = false;
                try {
                    Files.deleteIfExists(source);
                } catch (IOException ignored) {
                    // best-effort cleanup; the temp file is harmless if it stays
                }
                throw new CommitFailedException(
                        "Version %d already exists: %s", nextVersion, target);
            }
            super.renameToFinal(source, target, nextVersion);
        }
    }
}
