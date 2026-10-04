package com.iceberglearn.scan;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end: a source-column predicate written against the schema is projected to a partition
 * predicate <em>inside</em> the scan, driving both pruning stages over real on-disk manifests.
 *
 * <p>This mirrors the real Iceberg path — {@code scan.filter(expr).planFiles()} — where projection
 * happens inside ManifestGroup, not at the call site.
 */
class PredicateProjectionScanTest {

    private static final int ID = 1;
    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_DAY = 24L * 60L * 60L * 1_000_000L;

    @TempDir
    Path warehouse;

    @Test
    void scanFilterProjectsAndPrunesOverARealPartitionedTable() {
        Schema schema = new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .day("event_time")
                .build();

        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(TableIdentifier.of("events"), schema, spec);

        // Write real Parquet files under the table's data/ dir, then commit them through the real
        // AppendFiles path: 6 files across 3 days, 2 per day.
        java.nio.file.Path dataDir = warehouse.resolve("events").resolve("data");
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 2);
        writeRange(writer, day(20661), 10, 25);     // part-001, day=20661, age=[10,25]
        writeRange(writer, day(20661), 55, 70);     // part-002, day=20661, age=[55,70]
        writeRange(writer, day(20662), 30, 45);     // part-003, day=20662
        writeRange(writer, day(20662), 60, 80);     // part-004, day=20662
        writeRange(writer, day(20663), 15, 35);     // part-005, day=20663
        writeRange(writer, day(20663), 75, 90);     // part-006, day=20663

        AppendFiles append = table.newAppend();
        for (DataFile file : writer.complete()) {
            append.appendFile(file);
        }
        append.commit();

        // A user predicate written only against source columns: one day, age > 50.
        Expr oneDay = Exprs.and(
                Exprs.greaterThanOrEqual(EVENT_TIME, day(20661)),
                Exprs.lessThan(EVENT_TIME, day(20662)));
        Expr rowFilter = Exprs.and(oneDay, Exprs.greaterThan(AGE, 50));

        List<DataFile> planned = table.newScan().filter(rowFilter).planFiles();

        // Partition pruning keeps day=20661 (two files); metrics pruning drops the file whose age
        // upper bound 25 cannot satisfy age > 50 — leaving only the file with age bounds [55, 70].
        // UUID-based file names are not predictable, so verify by bounds instead.
        assertEquals(1, planned.size());
        assertEquals(55L, ((Number) planned.get(0).lowerBounds().get(AGE)).longValue());
        assertEquals(70L, ((Number) planned.get(0).upperBounds().get(AGE)).longValue());
    }

    @Test
    void unfilteredScanPlansAllLiveFiles() {
        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(TableIdentifier.of("events"), schema, spec);

        PartitionedWriter writer = new PartitionedWriter(
                schema, spec, warehouse.resolve("events").resolve("data"), 2);
        writeRange(writer, day(20661), 10, 25);
        writeRange(writer, day(20662), 30, 45);
        AppendFiles append = table.newAppend();
        writer.complete().forEach(append::appendFile);
        append.commit();

        // No filter set: every live file is returned, unpartitioned projection behaviour.
        List<DataFile> all = table.newScan().planFiles();
        assertEquals(2, all.size());
        assertTrue(table.newScan().filter() == null);
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

    private static void writeRange(PartitionedWriter writer, long eventTime, int minAge, int maxAge) {
        writer.write(row(1, eventTime, minAge));
        writer.write(row(2, eventTime, maxAge));
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
