package com.iceberglearn.partition;

import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.transforms.Bucket;
import com.iceberglearn.transforms.Days;
import com.iceberglearn.transforms.Hours;
import com.iceberglearn.transforms.Identity;
import com.iceberglearn.transforms.Transform;
import com.iceberglearn.transforms.Truncate;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Chapter 8: hidden partitioning — transforms, specs, keys, and the writer. */
class HiddenPartitioningTest {

    private static final long MICROS_PER_DAY = 24L * 60L * 60L * 1_000_000L;
    private static final long BASE = 1785110400000000L; // 2026-07-27 00:00:00 UTC

    private static Schema eventSchema() {
        return new Schema(List.of(
                Schema.NestedField.required(1, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(3, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string")
        ));
    }

    // ---- Transform behavior ----

    @Test
    void identityReturnsTheValueUnchangedAndIsMarkedAsIdentity() {
        assertEquals(123, Identity.get().apply(123));
        assertEquals("x", Identity.get().apply("x"));
        assertNull(Identity.get().apply(null));
        assertTrue(Identity.get().isIdentity());
        assertEquals("identity", Identity.get().toString());
    }

    @Test
    void dayDividesMicrosByOneDay() {
        assertEquals(20661, Days.get().apply(BASE));
        assertEquals(20662, Days.get().apply(BASE + MICROS_PER_DAY));
        assertNull(Days.get().apply(null));
        assertFalse(Days.get().isIdentity());
        assertEquals("day", Days.get().toString());
    }

    @Test
    void hourDividesMicrosByOneHour() {
        assertEquals(495864, Hours.get().apply(BASE));
        assertNull(Hours.get().apply(null));
        assertEquals("hour", Hours.get().toString());
    }

    @Test
    void bucketIsNonNegativeAndBounded() {
        Transform bucket4 = Bucket.get(4);
        // Strings with negative hashcodes still land in range.
        Object electronics = bucket4.apply("electronics");
        assertInstanceOf(Integer.class, electronics);
        int b = (Integer) electronics;
        assertTrue(b >= 0 && b < 4, "bucket out of range: " + b);

        assertEquals(b, bucket4.apply("electronics"), "same input must hash to the same bucket");
        assertNull(bucket4.apply(null));
        assertEquals("bucket[4]", bucket4.toString());

        assertThrows(IllegalArgumentException.class, () -> Bucket.get(0));
    }

    @Test
    void truncateFloorsNumbersAndPrefixesStrings() {
        assertEquals("Ali", Truncate.get(3).apply("Alice"));
        assertEquals("Bob", Truncate.get(5).apply("Bob"));
        assertEquals(20, Truncate.get(10).apply(23));
        // Negative numbers floor toward negative infinity: truncate[10](-23) = -30, not -20.
        assertEquals(-30, Truncate.get(10).apply(-23));
        assertEquals(0L, Truncate.get(10).apply(0L));
        assertNull(Truncate.get(3).apply(null));
        assertEquals("truncate[10]", Truncate.get(10).toString());
    }

    @Test
    void transformFromStringRoundTripsEveryTransform() {
        assertEquals(Identity.get(), Transform.fromString("identity"));
        assertEquals(Days.get(), Transform.fromString("day"));
        assertEquals(Hours.get(), Transform.fromString("hour"));
        assertEquals(Bucket.get(4), Transform.fromString("bucket[4]"));
        assertEquals(Truncate.get(10), Transform.fromString("truncate[10]"));
        assertThrows(IllegalArgumentException.class, () -> Transform.fromString("unknown"));
    }

    // ---- PartitionSpec builder ----

    @Test
    void unpartitionedSpecHasNoFieldsAndIdZero() {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        assertEquals(0, spec.specId());
        assertTrue(spec.fields().isEmpty());
        assertFalse(spec.isPartitioned());
    }

    @Test
    void fieldIdsStartAt1000AndIncrementPerField() {
        Schema schema = eventSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .day("event_time")
                .bucket("category", 4)
                .build();

        assertEquals(1, spec.specId());
        assertEquals(2, spec.fields().size());
        // First partition field id is 1000, second is 1001.
        assertEquals(1000, spec.fields().get(0).fieldId());
        assertEquals(1001, spec.fields().get(1).fieldId());
        // Source ids point at the schema columns, not names.
        assertEquals(3, spec.fields().get(0).sourceId());
        assertEquals(4, spec.fields().get(1).sourceId());
    }

    @Test
    void identityKeepsSourceNameOthersAddSuffix() {
        Schema schema = eventSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("category")
                .day("event_time")
                .hour("event_time")
                .bucket("id", 8)
                .truncate("name", 3)
                .build();

        assertEquals("category", spec.fields().get(0).name());
        assertEquals("event_time_day", spec.fields().get(1).name());
        assertEquals("event_time_hour", spec.fields().get(2).name());
        assertEquals("id_bucket", spec.fields().get(3).name());
        assertEquals("name_trunc", spec.fields().get(4).name());
    }

    @Test
    void builderRejectsUnknownSourceColumn() {
        Schema schema = eventSchema();
        assertThrows(IllegalArgumentException.class, () ->
                PartitionSpec.builderFor(schema).day("no_such_column").build());
    }

    @Test
    void builderRejectsDuplicatePartitionNames() {
        Schema schema = eventSchema();
        // Two day transforms on the same column produce the same name "event_time_day".
        assertThrows(IllegalArgumentException.class, () ->
                PartitionSpec.builderFor(schema)
                        .day("event_time")
                        .day("event_time")
                        .build());
    }

    // ---- PartitionKey ----

    @Test
    void partitionKeyComputesOneValuePerFieldInOrder() {
        Schema schema = eventSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .day("event_time")
                .bucket("category", 4)
                .build();

        PartitionKey key = new PartitionKey(spec);
        key.partition(Map.of(1, 1L, 2, "Alice", 3, BASE, 4, "electronics"));

        assertEquals(2, key.values().size());
        assertEquals(20661, key.values().get(0));
        assertEquals(Bucket.get(4).apply("electronics"), key.values().get(1));
    }

    @Test
    void partitionKeyEqualsHashCodeAreBasedOnValues() {
        Schema schema = eventSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();

        PartitionKey a = new PartitionKey(spec);
        a.partition(Map.of(3, BASE));
        PartitionKey b = new PartitionKey(spec);
        b.partition(Map.of(3, BASE));
        PartitionKey other = new PartitionKey(spec);
        other.partition(Map.of(3, BASE + MICROS_PER_DAY));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, other);
    }

    // ---- PartitionedWriter ----

    @Test
    void writerRoutesRowsByPartitionAndFlushesPerPartition() throws IOException {
        Schema schema = eventSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();

        Path dataDir = Files.createTempDirectory("ch08-test").resolve("data");
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 5);
        // 3 rows on day 20661, 2 on 20662, 1 on 20663 — none fills the buffer, so all
        // are flushed by complete().
        writer.write(row(1, BASE));
        writer.write(row(2, BASE));
        writer.write(row(3, BASE));
        writer.write(row(4, BASE + MICROS_PER_DAY));
        writer.write(row(5, BASE + MICROS_PER_DAY));
        writer.write(row(6, BASE + MICROS_PER_DAY * 2));
        List<DataFile> files = writer.complete();

        // One file per partition (all flushed at complete, none overflowed the 5-row target).
        assertEquals(3, files.size());
        assertTrue(files.stream().allMatch(f -> f.partition().size() == 1));
        assertTrue(files.stream().anyMatch(f -> f.partition().equals(List.of(20661)) && f.recordCount() == 3));
        assertTrue(files.stream().anyMatch(f -> f.partition().equals(List.of(20662)) && f.recordCount() == 2));
        assertTrue(files.stream().anyMatch(f -> f.partition().equals(List.of(20663)) && f.recordCount() == 1));
    }

    @Test
    void writerFlushesABufferWhenItReachesTargetRows() throws IOException {
        Schema schema = eventSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();

        Path dataDir = Files.createTempDirectory("ch08-test").resolve("data");
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 2);
        // Two rows on the same day fill the buffer and flush immediately.
        writer.write(row(1, BASE));
        writer.write(row(2, BASE));
        List<DataFile> files = writer.complete();

        assertEquals(1, files.size());
        assertEquals(2, files.get(0).recordCount());
        assertEquals(List.of(20661), files.get(0).partition());
    }

    @Test
    void sameDataDifferentSpecsProducesFlatPathsButDifferentPartitions() throws IOException {
        Schema schema = eventSchema();
        List<Map<Integer, Object>> rows = List.of(
                row(1, BASE), row(2, BASE),
                row(3, BASE + MICROS_PER_DAY), row(4, BASE + MICROS_PER_DAY));

        PartitionSpec specDay = PartitionSpec.builderFor(schema).day("event_time").build();
        PartitionSpec specBucket = PartitionSpec.builderFor(schema).bucket("category", 4).build();

        List<DataFile> dayFiles = writeAll(schema, specDay, rows);
        List<DataFile> bucketFiles = writeAll(schema, specBucket, rows);

        // Both layouts are flat: every path is data/<uuid>.parquet, no partition directories.
        assertTrue(dayFiles.stream().allMatch(f -> f.path().startsWith("data/")));
        assertTrue(bucketFiles.stream().allMatch(f -> f.path().startsWith("data/")));

        // The day spec produced partition values of 20661/20662.
        assertTrue(dayFiles.stream().anyMatch(f -> f.partition().equals(List.of(20661))));
        assertTrue(dayFiles.stream().anyMatch(f -> f.partition().equals(List.of(20662))));

        // The bucket spec produced partition values of 0..3.
        assertTrue(bucketFiles.stream().allMatch(f -> {
            Object v = f.partition().get(0);
            return v instanceof Integer && (Integer) v >= 0 && (Integer) v < 4;
        }));
    }

    private static Map<Integer, Object> row(int id, long eventTime) {
        return Map.of(1, id, 2, "n" + id, 3, eventTime, 4, "electronics");
    }

    private static List<DataFile> writeAll(Schema schema, PartitionSpec spec, List<Map<Integer, Object>> rows)
            throws IOException {
        Path dataDir = Files.createTempDirectory("ch08-test").resolve("data");
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 5);
        rows.forEach(writer::write);
        return writer.complete();
    }
}
