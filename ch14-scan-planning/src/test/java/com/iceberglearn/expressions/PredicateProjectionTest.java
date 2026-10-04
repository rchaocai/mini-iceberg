package com.iceberglearn.expressions;

import com.iceberglearn.manifests.ManifestGroup;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.transforms.Bucket;
import com.iceberglearn.transforms.Days;
import com.iceberglearn.transforms.Hours;
import com.iceberglearn.transforms.Truncate;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Inclusive projection tests modelled on Apache Iceberg's transform projection tests. */
class PredicateProjectionTest {

    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;

    @Test
    void identityCopiesPredicateToPartitionField() {
        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("age").build();

        Expr projected = Projections.inclusive(spec).project(Exprs.greaterThan(AGE, 50));

        assertEquals("field(1000) > 50", projected.toString());
    }

    @Test
    void dayEqualityTransformsLiteral() {
        PartitionSpec spec = daySpec();
        long noon = 20661L * MICROS_PER_DAY + 10L * MICROS_PER_HOUR;

        Expr projected = Projections.inclusive(spec).project(Exprs.equal(EVENT_TIME, noon));

        assertEquals("field(1000) = 20661", projected.toString());
    }

    @Test
    void dayRangeWidensStrictBoundWithoutDroppingTheBoundaryPartition() {
        PartitionSpec spec = daySpec();
        long tenAm = 20661L * MICROS_PER_DAY + 10L * MICROS_PER_HOUR;

        Expr projected = Projections.inclusive(spec).project(Exprs.greaterThan(EVENT_TIME, tenAm));

        assertEquals("field(1000) >= 20661", projected.toString());
    }

    @Test
    void strictLessThanAtDayBoundaryProjectsToPreviousDay() {
        Expr projected = Projections.inclusive(daySpec())
                .project(Exprs.lessThan(EVENT_TIME, 20662L * MICROS_PER_DAY));

        assertEquals("field(1000) <= 20661", projected.toString());
    }

    @Test
    void dayTransformUsesFloorDivisionBeforeTheEpoch() {
        assertEquals(-1, Days.get().apply(-1L));
        assertEquals(-1, Hours.get().apply(-1L));
    }

    @Test
    void bucketOnlyProjectsEquality() {
        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("age", 4).build();

        Expr equality = Projections.inclusive(spec).project(Exprs.equal(AGE, 50));
        Expr range = Projections.inclusive(spec).project(Exprs.greaterThan(AGE, 50));

        assertEquals("field(1000) = " + Bucket.get(4).apply(50), equality.toString());
        assertSame(AlwaysTrue.INSTANCE, range);
    }

    @Test
    void numericTruncateAdjustsStrictBoundaryBeforeTransforming() {
        PartitionSpec spec = PartitionSpec.builderFor(schema()).truncate("age", 10).build();

        Expr projected = Projections.inclusive(spec).project(Exprs.greaterThan(AGE, 20));

        assertEquals("field(1000) >= 20", projected.toString());
        assertEquals(-30, Truncate.get(10).apply(-23));
    }

    @Test
    void unpartitionedSourceColumnProjectsToAlwaysTrue() {
        Expr projected = Projections.inclusive(daySpec()).project(Exprs.equal(AGE, 50));

        assertSame(AlwaysTrue.INSTANCE, projected);
    }

    @Test
    void notIsRewrittenBeforeProjection() {
        Expr projected = Projections.inclusive(daySpec())
                .project(Exprs.not(Exprs.greaterThan(EVENT_TIME, 20661L * MICROS_PER_DAY)));

        assertEquals("field(1000) <= 20661", projected.toString());
    }

    @Test
    void multipleTransformsForOneSourceAreConjoined() {
        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .day("event_time")
                .hour("event_time")
                .build();
        long ts = 20661L * MICROS_PER_DAY + 3L * MICROS_PER_HOUR;

        Expr projected = Projections.inclusive(spec).project(Exprs.equal(EVENT_TIME, ts));

        assertEquals("(field(1000) = 20661 AND field(1001) = 495867)", projected.toString());
    }

    @Test
    void filterRowsAutomaticallyDrivesBothPruningStages() {
        PartitionSpec spec = daySpec();
        List<DataFile> files = List.of(
                file("part-001", 20661L, 10, 25),
                file("part-002", 20661L, 55, 70),
                file("part-003", 20662L, 60, 80),
                file("part-004", 20663L, 75, 90));

        Expr oneDay = Exprs.and(
                Exprs.greaterThanOrEqual(EVENT_TIME, 20661L * MICROS_PER_DAY),
                Exprs.lessThan(EVENT_TIME, 20662L * MICROS_PER_DAY));
        Expr rowFilter = Exprs.and(oneDay, Exprs.greaterThan(AGE, 50));
        List<DataFile> result = new ManifestGroup(files, spec)
                .filterRows(rowFilter)
                .planFiles();

        assertEquals(List.of("part-002"), result.stream().map(DataFile::path).toList());
    }

    @Test
    void partitionEvaluatorUsesExactNotEqualSemantics() {
        PartitionSpec spec = PartitionSpec.builderFor(schema()).identity("age").build();

        assertFalse(PartitionEvaluator.evaluate(
                Exprs.notEqual(1000, 50), spec, List.of(50)));
        assertTrue(PartitionEvaluator.evaluate(
                Exprs.notEqual(1000, 50), spec, List.of(51)));
    }

    private static PartitionSpec daySpec() {
        return PartitionSpec.builderFor(schema()).day("event_time").build();
    }

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(1, "id", "long"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(AGE, "age", "long")));
    }

    private static DataFile file(String path, long day, int minAge, int maxAge) {
        return DataFile.builder(path)
                .partition(List.of(day))
                .lowerBounds(Map.of(AGE, (long) minAge))
                .upperBounds(Map.of(AGE, (long) maxAge))
                .build();
    }

    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;
}
