package com.iceberglearn.metrics;

import com.iceberglearn.expressions.AlwaysFalse;
import com.iceberglearn.expressions.AlwaysTrue;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.manifests.ManifestGroup;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Chapter 9: data skipping — predicates, metrics pruning, and two-stage planning. */
class DataSkippingTest {

    private static final int AGE = 5; // field id for "age"

    // ---- Leaf predicate semantics: which bound does each one look at? -----

    @Test
    void greaterThanLooksAtUpperBoundStrictly() {
        // file [41, 55], predicate age > 50 → keep (max=55 > 50; file may contain 51..55)
        assertTrue(MetricsFilter.canMatch(Exprs.greaterThan(AGE, 50), file(41, 55)));
        // file [35, 50], predicate age > 50 → skip (max=50 not > 50; 50 itself is not > 50)
        assertFalse(MetricsFilter.canMatch(Exprs.greaterThan(AGE, 50), file(35, 50)));
        // file [10, 25], predicate age > 50 → skip (max=25 < 50)
        assertFalse(MetricsFilter.canMatch(Exprs.greaterThan(AGE, 50), file(10, 25)));
    }

    @Test
    void greaterThanOrEqualLooksAtUpperBoundInclusively() {
        // file [35, 50], predicate age >= 50 → keep (max=50 >= 50)
        assertTrue(MetricsFilter.canMatch(Exprs.greaterThanOrEqual(AGE, 50), file(35, 50)));
        // file [10, 49], predicate age >= 50 → skip (max=49 < 50)
        assertFalse(MetricsFilter.canMatch(Exprs.greaterThanOrEqual(AGE, 50), file(10, 49)));
    }

    @Test
    void lessThanLooksAtLowerBoundStrictly() {
        // file [71, 85], predicate age < 60 → skip (min=71 not < 60)
        assertFalse(MetricsFilter.canMatch(Exprs.lessThan(AGE, 60), file(71, 85)));
        // file [56, 70], predicate age < 60 → keep (min=56 < 60; file may contain 56..59)
        assertTrue(MetricsFilter.canMatch(Exprs.lessThan(AGE, 60), file(56, 70)));
        // file [60, 75], predicate age < 60 → skip (min=60 not < 60)
        assertFalse(MetricsFilter.canMatch(Exprs.lessThan(AGE, 60), file(60, 75)));
    }

    @Test
    void lessThanOrEqualLooksAtLowerBoundInclusively() {
        // file [60, 75], predicate age <= 60 → keep (min=60 <= 60)
        assertTrue(MetricsFilter.canMatch(Exprs.lessThanOrEqual(AGE, 60), file(60, 75)));
        // file [61, 75], predicate age <= 60 → skip (min=61 > 60)
        assertFalse(MetricsFilter.canMatch(Exprs.lessThanOrEqual(AGE, 60), file(61, 75)));
    }

    @Test
    void equalLooksAtBothBounds() {
        // file [10, 25], predicate age = 50 → skip (upper=25 < 50, value above range)
        assertFalse(MetricsFilter.canMatch(Exprs.equal(AGE, 50), file(10, 25)));
        // file [41, 55], predicate age = 50 → keep (50 falls inside [41, 55])
        assertTrue(MetricsFilter.canMatch(Exprs.equal(AGE, 50), file(41, 55)));
        // file [71, 85], predicate age = 50 → skip (lower=71 > 50, value below range)
        assertFalse(MetricsFilter.canMatch(Exprs.equal(AGE, 50), file(71, 85)));
    }

    @Test
    void notEqualNeverPrunes() {
        // Even equal bounds are not treated as proof that every stored value equals the literal.
        assertTrue(MetricsFilter.canMatch(Exprs.notEqual(AGE, 50), file(50, 50)));
        assertTrue(MetricsFilter.canMatch(Exprs.notEqual(AGE, 50), file(10, 25)));
    }

    // ---- Integer / Long normalization via Expr.toComparable -----

    @Test
    void integerLiteralIsLiftedToLongForComparison() {
        // Build a file whose bounds are Long (the on-disk representation), and pass an Integer
        // literal (from `int` boxed by Java) — toComparable must align the two.
        DataFile f = DataFile.builder("data/x.parquet")
                .lowerBounds(Map.of(AGE, 50L))
                .upperBounds(Map.of(AGE, 60L))
                .build();
        // Without the Integer→Long lift, Long.compareTo(Integer) throws ClassCastException.
        assertDoesNotThrow(() -> MetricsFilter.canMatch(Exprs.greaterThan(AGE, 55), f));
        assertTrue(MetricsFilter.canMatch(Exprs.greaterThan(AGE, 55), f));
    }

    // ---- Conservative skipping: missing metrics must keep the file -------

    @Test
    void missingUpperBoundConservativelyKeepsTheFile() {
        // Bounds table present but column absent → getUpperBound returns null → predicate says
        // "might match".
        DataFile noAge = DataFile.builder("data/x.parquet")
                .lowerBounds(Map.of())
                .upperBounds(Map.of())
                .build();
        assertTrue(MetricsFilter.canMatch(Exprs.greaterThan(AGE, 50), noAge));
        assertTrue(MetricsFilter.canMatch(Exprs.lessThan(AGE, 50), noAge));
        assertTrue(MetricsFilter.canMatch(Exprs.equal(AGE, 50), noAge));
    }

    @Test
    void nullBoundsMapsConservativelyKeepTheFile() {
        // Builder leaves lowerBounds/upperBounds at null when not set.
        DataFile nullBounds = DataFile.builder("data/x.parquet").build();
        assertTrue(MetricsFilter.canMatch(Exprs.greaterThan(AGE, 50), nullBounds));
    }

    @Test
    void nullInputsToCanMatchAreTreatedAsKeep() {
        // Defensive: null filter or null file → cannot disprove → keep.
        assertTrue(MetricsFilter.canMatch(null, file(10, 25)));
        assertTrue(MetricsFilter.canMatch(Exprs.greaterThan(AGE, 50), null));
    }

    // ---- Combinators: how AND/OR/NOT propagate "might match" --------------

    @Test
    void andKeepsFileOnlyWhenBothSidesMightMatch() {
        Expr ageBetween30And60 = Exprs.and(Exprs.greaterThan(AGE, 30), Exprs.lessThan(AGE, 60));
        // [56, 70]: gt(30) keep (max=70>30), lt(60) keep (min=56<60) → AND keep
        assertTrue(MetricsFilter.canMatch(ageBetween30And60, file(56, 70)));
        // [60, 75]: gt(30) keep, lt(60) skip (min=60 not <60) → AND skip
        assertFalse(MetricsFilter.canMatch(ageBetween30And60, file(60, 75)));
        // [10, 25]: gt(30) skip (max=25 not >30) → AND short-circuits to skip
        assertFalse(MetricsFilter.canMatch(ageBetween30And60, file(10, 25)));
    }

    @Test
    void orKeepsFileWhenEitherSideMightMatch() {
        Expr ageOutside20To70 = Exprs.or(Exprs.lessThan(AGE, 20), Exprs.greaterThan(AGE, 70));
        // [56, 70]: lt(20) skip (min=56), gt(70) skip (max=70 not >70) → OR skip
        assertFalse(MetricsFilter.canMatch(ageOutside20To70, file(56, 70)));
        // [71, 85]: lt(20) skip, gt(70) keep (min=71>70) → OR keep
        assertTrue(MetricsFilter.canMatch(ageOutside20To70, file(71, 85)));
        // [10, 25]: lt(20) keep (min=10<20) → OR short-circuits to keep
        assertTrue(MetricsFilter.canMatch(ageOutside20To70, file(10, 25)));
    }

    @Test
    void notIsConservativeBecauseMightMatchCannotBeSafelyNegated() {
        assertTrue(MetricsFilter.canMatch(Exprs.not(Exprs.greaterThan(AGE, 50)), file(10, 25)));
        assertTrue(MetricsFilter.canMatch(Exprs.not(Exprs.greaterThan(AGE, 50)), file(41, 55)));
    }

    // ---- Constant folding in Exprs -----

    @Test
    void andWithAlwaysFalseCollapsesToAlwaysFalse() {
        Expr x = Exprs.greaterThan(AGE, 30);
        assertSame(AlwaysFalse.INSTANCE, Exprs.and(x, AlwaysFalse.INSTANCE));
        assertSame(AlwaysFalse.INSTANCE, Exprs.and(AlwaysFalse.INSTANCE, x));
    }

    @Test
    void andWithAlwaysTrueReturnsTheOtherSide() {
        Expr x = Exprs.greaterThan(AGE, 30);
        assertSame(x, Exprs.and(x, AlwaysTrue.INSTANCE));
        assertSame(x, Exprs.and(AlwaysTrue.INSTANCE, x));
    }

    @Test
    void orWithAlwaysTrueCollapsesToAlwaysTrue() {
        Expr x = Exprs.greaterThan(AGE, 30);
        assertSame(AlwaysTrue.INSTANCE, Exprs.or(x, AlwaysTrue.INSTANCE));
        assertSame(AlwaysTrue.INSTANCE, Exprs.or(AlwaysTrue.INSTANCE, x));
    }

    @Test
    void orWithAlwaysFalseReturnsTheOtherSide() {
        Expr x = Exprs.greaterThan(AGE, 30);
        assertSame(x, Exprs.or(x, AlwaysFalse.INSTANCE));
        assertSame(x, Exprs.or(AlwaysFalse.INSTANCE, x));
    }

    @Test
    void notOfConstantsFlipsThem() {
        assertSame(AlwaysFalse.INSTANCE, Exprs.not(AlwaysTrue.INSTANCE));
        assertSame(AlwaysTrue.INSTANCE, Exprs.not(AlwaysFalse.INSTANCE));
    }

    @Test
    void doubleNegationEliminates() {
        Expr x = Exprs.greaterThan(AGE, 30);
        assertSame(x, Exprs.not(Exprs.not(x)));
    }

    @Test
    void alwaysTrueAndAlwaysFalseEvaluateAsConstants() {
        DataFileStats stats = new DataFileStats(Map.of(), Map.of(), Map.of(), Map.of());
        assertTrue(AlwaysTrue.INSTANCE.evaluate(stats));
        assertFalse(AlwaysFalse.INSTANCE.evaluate(stats));
    }

    // ---- End-to-end: MetricsFilter on the demo dataset -----

    @Test
    void ageGt50SkipsFourOfTenFiles() {
        List<DataFile> files = List.of(
                file("data/file-001.parquet", 10, 25),
                file("data/file-002.parquet", 26, 40),
                file("data/file-003.parquet", 41, 55),
                file("data/file-004.parquet", 56, 70),
                file("data/file-005.parquet", 71, 85),
                file("data/file-006.parquet", 86, 100),
                file("data/file-007.parquet", 5, 15),
                file("data/file-008.parquet", 45, 65),
                file("data/file-009.parquet", 35, 50),
                file("data/file-010.parquet", 60, 75)
        );
        Expr gt50 = Exprs.greaterThan(AGE, 50);
        List<DataFile> kept = files.stream()
                .filter(f -> MetricsFilter.canMatch(gt50, f))
                .toList();
        // Skipped: 001 (max=25), 002 (max=40), 007 (max=15), 009 (max=50, not > 50).
        assertEquals(6, kept.size());
        assertTrue(kept.stream().noneMatch(f -> f.path().contains("001")
                || f.path().contains("002")
                || f.path().contains("007")
                || f.path().contains("009")));
    }

    // ---- ManifestGroup: two-stage planning -----

    @Test
    void manifestGroupWithNoFiltersKeepsEverything() {
        ManifestGroup group = new ManifestGroup(List.of(file("a", 1, 10), file("b", 20, 30)),
                PartitionSpec.unpartitioned());
        assertEquals(2, group.planFiles().size());
    }

    @Test
    void manifestGroupWithMetricsFilterOnlySkipsAtMetricsStage() {
        // Unpartitioned spec + metrics filter: stage 1 is skipped, stage 2 does the work.
        List<DataFile> files = List.of(
                file("data/file-001.parquet", 10, 25),
                file("data/file-002.parquet", 41, 55)
        );
        ManifestGroup group = new ManifestGroup(files, PartitionSpec.unpartitioned());
        List<DataFile> kept = group.filterData(Exprs.greaterThan(AGE, 50)).planFiles();
        // Only file-002 survives (max=55 > 50).
        assertEquals(1, kept.size());
        assertEquals("data/file-002.parquet", kept.get(0).path());
    }

    @Test
    void manifestGroupTwoStageKeepsThreeOfSixPartitionedFiles() {
        // 6 partitioned files across 3 days; predicate age > 50.
        // Without a partition filter, stage 1 keeps all 6; stage 2 keeps 3 (002, 004, 006).
        List<DataFile> files = partitionedFiles();
        PartitionSpec spec = PartitionSpec.builderFor(schemaWithAge())
                .withSpecId(1).day("event_time").build();
        ManifestGroup group = new ManifestGroup(files, spec);
        List<DataFile> kept = group.filterData(Exprs.greaterThan(AGE, 50)).planFiles();
        assertEquals(3, kept.size());
        assertTrue(kept.stream().allMatch(f -> f.path().contains("002")
                || f.path().contains("004")
                || f.path().contains("006")));
    }

    @Test
    void manifestGroupNoPartitionFilterStillRunsMetricsStage() {
        // The summary line should report 6 → 6 → 3 when no partition filter is set.
        List<DataFile> files = partitionedFiles();
        PartitionSpec spec = PartitionSpec.builderFor(schemaWithAge())
                .withSpecId(1).day("event_time").build();
        // filterData only — partitionFilter stays null, so stage 1 is skipped by the if-guard.
        List<DataFile> kept = new ManifestGroup(files, spec)
                .filterData(Exprs.greaterThan(AGE, 50))
                .planFiles();
        assertEquals(3, kept.size());
    }

    @Test
    void manifestGroupPartitionFilterPrunesToTwoFilesThenMetricsToOne() {
        // 6 partitioned files across 3 days; partition predicate keeps day 20661 (2 files),
        // then metrics predicate age > 50 keeps 1 file (part-002, age [55, 70]).
        // Funnel: 6 → 2 → 1.
        List<DataFile> files = partitionedFiles();
        PartitionSpec spec = PartitionSpec.builderFor(schemaWithAge())
                .withSpecId(1).day("event_time").build();
        int dayFieldId = spec.fields().get(0).fieldId(); // 1000
        List<DataFile> kept = new ManifestGroup(files, spec)
                .filterPartitions(Exprs.equal(dayFieldId, 20661))
                .filterData(Exprs.greaterThan(AGE, 50))
                .planFiles();
        assertEquals(1, kept.size());
        assertEquals("data/part-002.parquet", kept.get(0).path());
    }

    @Test
    void manifestGroupUnpartitionedSpecSkipsStage1EvenWithPartitionFilter() {
        // Even if the caller sets a partition filter, an unpartitioned spec means there is no
        // partition to evaluate, so stage 1 is skipped.
        List<DataFile> files = List.of(file("a", 1, 10), file("b", 80, 90));
        ManifestGroup group = new ManifestGroup(files, PartitionSpec.unpartitioned())
                .filterPartitions(Exprs.alwaysTrue())
                .filterData(Exprs.greaterThan(AGE, 50));
        // Stage 2 keeps only file "b".
        assertEquals(1, group.planFiles().size());
    }

    // ---- Helpers -----

    private static DataFile file(String path, int ageMin, int ageMax) {
        return DataFile.builder(path)
                .recordCount(100)
                .fileSizeInBytes(10240)
                .lowerBounds(Map.of(AGE, (long) ageMin))
                .upperBounds(Map.of(AGE, (long) ageMax))
                .build();
    }

    /** Convenience for predicate tests that don't care about the path. */
    private static DataFile file(int ageMin, int ageMax) {
        return file("data/age-" + ageMin + "-" + ageMax + ".parquet", ageMin, ageMax);
    }

    private static Schema schemaWithAge() {
        return new Schema(List.of(
                Schema.NestedField.required(1, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(3, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")
        ));
    }

    private static List<DataFile> partitionedFiles() {
        return List.of(
                partitionedFile("data/part-001.parquet", 20661L, 10, 25),
                partitionedFile("data/part-002.parquet", 20661L, 55, 70),
                partitionedFile("data/part-003.parquet", 20662L, 30, 45),
                partitionedFile("data/part-004.parquet", 20662L, 60, 80),
                partitionedFile("data/part-005.parquet", 20663L, 15, 35),
                partitionedFile("data/part-006.parquet", 20663L, 75, 90)
        );
    }

    private static DataFile partitionedFile(String path, long day, int ageMin, int ageMax) {
        return DataFile.builder(path)
                .recordCount(100)
                .fileSizeInBytes(10240)
                .lowerBounds(Map.of(AGE, (long) ageMin))
                .upperBounds(Map.of(AGE, (long) ageMax))
                .partition(List.of(day))
                .build();
    }
}
