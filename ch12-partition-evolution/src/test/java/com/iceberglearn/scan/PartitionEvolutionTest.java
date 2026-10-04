package com.iceberglearn.scan;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestFiles;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.operations.FileSystemTableOperations;
import com.iceberglearn.operations.TableOperations;
import com.iceberglearn.operations.UpdatePartitionSpec;
import com.iceberglearn.partition.PartitionField;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;
import com.iceberglearn.transforms.Hours;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage of partition spec evolution: evolving a table from a day spec to an hour
 * spec must append a new spec under a fresh id, switch the default pointer, leave every old
 * data file and manifest untouched, and make the scan project the same row predicate through
 * each spec group at its own granularity.
 *
 * <p>Mirrors the core scenarios of the real Iceberg TestUpdatePartitionSpec: field ids of
 * surviving fields are preserved, new fields draw table-level fresh ids, and a conflicting
 * spec-update commit fails fast instead of retrying.
 */
class PartitionEvolutionTest {

    private static final int ID = 1;
    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

    @TempDir
    Path warehouse;

    @Test
    void updateSpecAppendsNewSpecAndSwitchesDefaultWithoutCreatingSnapshot() {
        Table table = createDayTable();
        writeRangeAndCommit(table, table.spec(), day(20661), 10, 25);
        long snapshotIdBefore = table.currentSnapshot().snapshotId();
        int snapshotsBefore = countSnapshots(table);

        evolveToHour(table);

        table.refresh();
        assertEquals(2, table.metadata().partitionSpecs().size());
        assertEquals(1, table.metadata().defaultSpecId());
        // A spec update is a pure metadata change: snapshots, the current pointer and the
        // sequence number are all copied verbatim.
        assertEquals(snapshotsBefore, countSnapshots(table));
        assertEquals(snapshotIdBefore, table.currentSnapshot().snapshotId());
    }

    @Test
    void evolvedSpecPreservesUnchangedFieldIdsAndAssignsTableLevelFreshIdToNewField() {
        Table table = createDayBucketTable();
        table.updatePartitionSpec()
                .removeField("event_time_day")
                .addField("event_time", Hours.get())
                .commit();
        table.refresh();

        PartitionSpec evolved = table.metadata().defaultSpec();
        assertEquals(2, evolved.fields().size());

        // bucket(age) survived: same id (1001), same source, same transform.
        PartitionField bucket = evolved.fields().get(0);
        assertEquals("age_bucket", bucket.name());
        assertEquals(1001, bucket.fieldId());
        assertEquals(AGE, bucket.sourceId());

        // hour(event_time) is new: table-level id 1002 (max ever used + 1), never 1000 —
        // 1000 still belongs to the removed day field of spec 0.
        PartitionField hour = evolved.fields().get(1);
        assertEquals("event_time_hour", hour.name());
        assertEquals(1002, hour.fieldId());
        assertEquals(EVENT_TIME, hour.sourceId());
    }

    @Test
    void renameFieldKeepsFieldIdAndSourceIdButChangesName() {
        Table table = createDayTable();
        table.updatePartitionSpec().renameField("event_time_day", "ts_day").commit();
        table.refresh();

        PartitionSpec evolved = table.metadata().defaultSpec();
        PartitionField renamed = evolved.fields().get(0);
        assertEquals("ts_day", renamed.name());
        assertEquals(1000, renamed.fieldId());
        assertEquals(EVENT_TIME, renamed.sourceId());
    }

    @Test
    void removeFieldThrowsForUnknownName() {
        Table table = createDayTable();
        UpdatePartitionSpec update = table.updatePartitionSpec();
        assertThrows(IllegalArgumentException.class,
                () -> update.removeField("no_such_partition_field"));
    }

    @Test
    void addFieldRejectsDuplicateSourceColumnAndTransform() {
        Table table = createDayTable();
        UpdatePartitionSpec update = table.updatePartitionSpec();
        // day(event_time) is still the table's partitioning — adding it again is a duplicate.
        assertThrows(IllegalArgumentException.class,
                () -> update.addField("event_time", com.iceberglearn.transforms.Days.get()));
    }

    @Test
    void addFieldRejectsGeneratedNameCollision() {
        Table table = createDayTable();
        UpdatePartitionSpec update = table.updatePartitionSpec()
                .removeField("event_time_day")
                .addField("event_time", Hours.get());
        // event_time_hour is already staged; adding identity(event_time) would reuse the
        // source column name — but the collision here is hour vs hour again: same name.
        assertThrows(IllegalArgumentException.class,
                () -> update.addField("event_time", Hours.get()));
    }

    @Test
    void specUpdateCommitConflictFailsFastWithoutRetry() {
        Schema schema = schema();
        PartitionSpec daySpec = PartitionSpec.builderFor(schema).day("event_time").build();
        Path tableLocation = warehouse.resolve("events");
        TableOperations ops = new FailOnceRenameOperations(tableLocation);
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        catalog.createTable(TableIdentifier.of("events"), schema, daySpec);

        Table table = new Table(TableIdentifier.of("events"), ops);
        // Unlike a snapshot update (which retries through SnapshotUpdate), a spec update whose
        // base was concurrently replaced fails immediately — the caller starts over.
        assertThrows(CommitFailedException.class, () ->
                table.updatePartitionSpec()
                        .removeField("event_time_day")
                        .addField("event_time", Hours.get())
                        .commit());
    }

    /**
     * Same shape as the operations package's FailOnceWithConflictTableOperations (package
     * private there): fail the first rename so the commit looks like a concurrent conflict.
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

    @Test
    void metadataJsonRoundTripsMultipleSpecsWithDefaultSpecId() {
        Table table = createDayTable();
        writeBatchAndCommit(table, table.spec(), 20661, 10);
        evolveToHour(table);

        // Reload through the catalog: the second metadata file must carry both specs.
        Table reloaded = new FileSystemCatalog(warehouse.toString())
                .loadTable(TableIdentifier.of("events"));
        assertEquals(2, reloaded.metadata().partitionSpecs().size());
        assertEquals(1, reloaded.metadata().defaultSpecId());
        assertNotNull(reloaded.metadata().specsById().get(0));
        assertNotNull(reloaded.metadata().specsById().get(1));
        assertEquals("event_time_day",
                reloaded.metadata().spec(0).fields().get(0).name());
        assertEquals("event_time_hour",
                reloaded.metadata().spec(1).fields().get(0).name());
    }

    @Test
    void appendAfterEvolutionStampsNewManifestWithNewSpecIdAndReusesOldManifest() throws IOException {
        Table table = createDayTable();
        writeBatchAndCommit(table, table.spec(), 20661, 10);

        String oldManifestPath = currentManifests(table).get(0).path();
        ManifestFile oldManifest = currentManifests(table).get(0);
        assertEquals(0, oldManifest.partitionSpecId());

        evolveToHour(table);
        writeBatchAndCommit(table, table.spec(), 20661, 10);

        List<ManifestFile> manifests = currentManifests(table);
        assertEquals(2, manifests.size());
        // The old manifest is reused verbatim — same path, still spec 0.
        assertEquals(oldManifestPath, manifests.get(0).path());
        assertEquals(0, manifests.get(0).partitionSpecId());
        // The new manifest is stamped with the new default spec id.
        assertEquals(1, manifests.get(1).partitionSpecId());
    }

    @Test
    void scanPrunesEachSpecGroupAtItsOwnGranularity() {
        Table table = createDayTable();
        // Day-spec batch on day 20661: the 10:00 file matches the query hour and age; the
        // 10:30 file is the same HOUR but fails age. Plus one file on day 20662.
        writeRangeAndCommit(table, table.spec(), day(20661) + 10 * MICROS_PER_HOUR, 55, 70);
        writeRangeAndCommit(table, table.spec(), day(20661) + 10 * MICROS_PER_HOUR + 30L * 1_000_000L, 10, 25);
        writeRangeAndCommit(table, table.spec(), day(20662) + 9 * MICROS_PER_HOUR, 60, 80);

        evolveToHour(table);

        // Hour-spec batch: the 10:00 file matches everything; the 14:00 file WOULD match the
        // predicate at row level (same day, matching age) — it is only the hour granularity
        // of this spec that can cut it at partition stage.
        writeRangeAndCommit(table, table.spec(), day(20661) + 10 * MICROS_PER_HOUR, 55, 90);
        writeRangeAndCommit(table, table.spec(), day(20661) + 14 * MICROS_PER_HOUR, 55, 90);
        writeRangeAndCommit(table, table.spec(), day(20662) + 9 * MICROS_PER_HOUR, 55, 90);

        // One user predicate over source columns: the 10:00–11:00 hour of day 20661, age > 50.
        Expr rowFilter = Exprs.and(
                Exprs.and(
                        Exprs.greaterThanOrEqual(EVENT_TIME, day(20661) + 10 * MICROS_PER_HOUR),
                        Exprs.lessThan(EVENT_TIME, day(20661) + 11 * MICROS_PER_HOUR)),
                Exprs.greaterThan(AGE, 50));

        List<DataFile> planned = table.newScan().filter(rowFilter).planFiles();

        // Day group: day granularity cannot see hours, so partition pruning keeps BOTH
        // day=20661 files; metrics pruning drops the [10,25] one (3 → 2 → 1). Hour group:
        // partition pruning keeps only hour=495874 — the 14:00 file, which would have
        // survived metrics, is already cut at stage 1 (3 → 1 → 1). One survivor per spec.
        assertEquals(2, planned.size());
        Set<Integer> partitions = new HashSet<>();
        for (DataFile file : planned) {
            partitions.add((Integer) file.partition().get(0));
            assertTrue(((Number) file.lowerBounds().get(AGE)).longValue() > 50);
        }
        assertTrue(partitions.contains(20661), "day-group file must survive (cut at metrics stage)");
        assertTrue(partitions.contains(20661 * 24 + 10), "hour-group file must survive (kept at partition stage)");
    }

    @Test
    void unfilteredScanFlattensAllFilesAcrossBothSpecs() {
        Table table = createDayTable();
        writeRangeAndCommit(table, table.spec(), day(20661), 10, 25);
        writeRangeAndCommit(table, table.spec(), day(20662), 30, 45);
        evolveToHour(table);
        writeRangeAndCommit(table, table.spec(), day(20661), 55, 90);
        writeRangeAndCommit(table, table.spec(), day(20662), 55, 90);

        // No filter: files from both spec groups flatten into one list, manifest order kept.
        List<DataFile> all = table.newScan().planFiles();
        assertEquals(4, all.size());
    }

    @Test
    void oldDataFilesRemainAtSamePathsAfterEvolutionAndNewAppend() {
        Table table = createDayTable();
        writeRangeAndCommit(table, table.spec(), day(20661), 10, 25);
        writeRangeAndCommit(table, table.spec(), day(20662), 30, 45);

        Set<String> oldPaths = new HashSet<>();
        for (DataFile file : table.newScan().planFiles()) {
            oldPaths.add(file.path());
        }

        evolveToHour(table);
        writeRangeAndCommit(table, table.spec(), day(20661), 55, 90);

        // Every pre-evolution file is still on disk at the exact same path, and every one of
        // them is still planned — evolution moved nothing.
        for (String path : oldPaths) {
            Path absolute = Path.of(table.location()).resolve(path);
            assertTrue(Files.exists(absolute), "old data file must remain: " + path);
        }
        Set<String> nowPaths = new HashSet<>();
        for (DataFile file : table.newScan().planFiles()) {
            nowPaths.add(file.path());
        }
        assertTrue(nowPaths.containsAll(oldPaths));
        assertEquals(oldPaths.size() + 1, nowPaths.size());
    }

    // --- helpers ---------------------------------------------------------------

    private static int countSnapshots(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private Table createDayTable() {
        Schema schema = schema();
        PartitionSpec daySpec = PartitionSpec.builderFor(schema).day("event_time").build();
        return new FileSystemCatalog(warehouse.toString())
                .createTable(TableIdentifier.of("events"), schema, daySpec);
    }

    private Table createDayBucketTable() {
        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .day("event_time")
                .bucket("age", 8)
                .build();
        return new FileSystemCatalog(warehouse.toString())
                .createTable(TableIdentifier.of("events"), schema, spec);
    }

    private void evolveToHour(Table table) {
        table.updatePartitionSpec()
                .removeField("event_time_day")
                .addField("event_time", Hours.get())
                .commit();
        table.refresh();
    }

    private void writeBatchAndCommit(Table table, PartitionSpec spec, int epochDay, int hour) {
        writeRangeAndCommit(table, spec, day(epochDay) + hour * MICROS_PER_HOUR, 10, 25);
    }

    private void writeRangeAndCommit(Table table, PartitionSpec spec, long eventTime,
                                     int minAge, int maxAge) {
        PartitionedWriter writer = new PartitionedWriter(
                schema(), spec, Path.of(table.location()).resolve("data"), 2);
        writer.write(row(1, eventTime, minAge));
        writer.write(row(2, eventTime, maxAge));
        AppendFiles append = table.newAppend();
        writer.complete().forEach(append::appendFile);
        append.commit();
    }

    private List<ManifestFile> currentManifests(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        try {
            ManifestList list = ManifestFiles.readManifestList(
                    table.location(), snapshot.manifestListLocation());
            return list.manifests();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")));
    }

    private static long day(int epochDay) {
        return epochDay * MICROS_PER_DAY;
    }

    private static Map<Integer, Object> row(long id, long eventTime, int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(ID, id);
        row.put(2, "user-" + id);
        row.put(EVENT_TIME, eventTime);
        row.put(4, "demo");
        row.put(AGE, age);
        return row;
    }
}
