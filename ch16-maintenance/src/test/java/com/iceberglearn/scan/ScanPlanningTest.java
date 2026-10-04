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
 * End-to-end coverage for the scan planning pipeline: {@code planFiles()} pruning,
 * {@code planTasks()} bin-packing, and {@link SplitPlanner}'s split/bin-pack behavior
 * in isolation.
 *
 * <p>Mirrors real Iceberg's {@code TestDataTableScan} / {@code TestTableScanUtil} split:
 * the first three tests exercise the integrated scan path against a real partitioned
 * table; the rest poke {@link SplitPlanner} directly with synthetic files so the split
 * and bin-packing logic can be probed at known byte thresholds without writing huge
 * data files.
 */
class ScanPlanningTest {

    private static final int ID = 1;
    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_DAY = 24L * 60L * 60L * 1_000_000L;

    @TempDir
    Path warehouse;

    // --- integrated scan path -------------------------------------------------

    @Test
    void planFilesWithoutFilterReturnsAllLiveFiles() {
        Table table = buildAndWriteSixFiles();

        List<DataFile> planned = table.newScan().planFiles();
        assertEquals(6, planned.size(), "no filter → all six files survive");
    }

    @Test
    void planFilesWithAgeFilterMetricsPrunes() {
        Table table = buildAndWriteSixFiles();

        // age is a data column (not a partition source); partition projection yields
        // AlwaysTrue, so partition pruning is a no-op and only metrics pruning acts.
        Expr ageFilter = Exprs.greaterThan(AGE, 40L);
        List<DataFile> planned = table.newScan().filter(ageFilter).planFiles();

        assertEquals(3, planned.size(), "metrics pruning drops the three low-age files");
        for (DataFile file : planned) {
            long upper = ((Number) file.upperBounds().get(AGE)).longValue();
            assertTrue(upper > 40L, "surviving file must have max(age) > 40");
        }
    }

    @Test
    void planFilesWithCombinedFilterRunsBothPruningStages() {
        Table table = buildAndWriteSixFiles();

        // event_time < day(20662) projects to day_field < 20662 (partition pruning drops
        // days 20662/20663); age > 40 then drops the low-age file in day 20661.
        Expr rowFilter = Exprs.and(
                Exprs.lessThan(EVENT_TIME, day(20662)),
                Exprs.greaterThan(AGE, 40L));
        List<DataFile> planned = table.newScan().filter(rowFilter).planFiles();

        assertEquals(1, planned.size(), "partition pruning → 2, metrics pruning → 1");
        long upper = ((Number) planned.get(0).upperBounds().get(AGE)).longValue();
        assertTrue(upper > 40L);
        assertEquals(20661, planned.get(0).partition().get(0));
    }

    @Test
    void planTasksReturnsSingleCombinedTaskWhenTotalWeightBelowSplitSize() {
        Table table = buildAndWriteSixFiles();

        // All six files are ~1.5 KB each; default splitSize 128 MB; openFileCost 4 MB.
        // Per-chunk weight = max(1.5 KB, 4 MB) = 4 MB; 6 chunks × 4 MB = 24 MB ≪ 128 MB.
        // The bin-packer stuffs every chunk into one CombinedScanTask.
        List<CombinedScanTask> tasks = table.newScan().planTasks();
        assertEquals(1, tasks.size(), "all files fit in one bin");
        assertEquals(6, tasks.get(0).files().size(), "every surviving file is in the task");
    }

    @Test
    void planTasksHonorsFilterAndPacksOnlySurvivors() {
        Table table = buildAndWriteSixFiles();

        Expr ageFilter = Exprs.greaterThan(AGE, 40L);
        List<CombinedScanTask> tasks = table.newScan().filter(ageFilter).planTasks();

        assertEquals(1, tasks.size(), "three survivors still fit in one bin");
        assertEquals(3, tasks.get(0).files().size(), "only the three high-age files");
    }

    @Test
    void planTasksSizeBytesEqualsSumOfFileTaskLengths() {
        Table table = buildAndWriteSixFiles();

        List<CombinedScanTask> tasks = table.newScan().planTasks();
        CombinedScanTask task = tasks.get(0);

        long expected = task.files().stream()
                .mapToLong(FileScanTask::length)
                .sum();
        assertEquals(expected, task.sizeBytes());
    }

    @Test
    void singleRowGroupFileProducesOneChunkStartingAtRowGroupOffset() {
        Table table = buildAndWriteSixFiles();

        // Each test file holds two rows in a single Parquet row group. Splitting therefore
        // takes the offsets-aware path (splitOffsets is non-null and strictly ascending
        // with one entry), producing exactly one chunk per file whose start is the row
        // group's byte offset (right after the 4-byte Parquet magic header) and whose
        // length runs to the file end. This matches real Iceberg's BaseContentScanTask.split
        // for splittable files with split_offsets — the chunk never starts at 0.
        List<CombinedScanTask> tasks = table.newScan().planTasks();
        for (FileScanTask fileTask : tasks.get(0).files()) {
            List<Long> offsets = fileTask.file().splitOffsets();
            assertEquals(1, offsets.size(), "single row group → one offset");
            long expectedStart = offsets.get(0);
            assertEquals(expectedStart, fileTask.start(),
                    "chunk start = first row-group offset, not 0");
            assertEquals(fileTask.file().fileSizeInBytes() - expectedStart, fileTask.length(),
                    "chunk length runs from row-group offset to file end");
        }
    }

    // --- SplitPlanner.splitFiles ----------------------------------------------

    @Test
    void splitFilesProducesSingleChunkForFilesAtOrBelowSplitSize() {
        DataFile small = syntheticFile("small.parquet", 1_000);
        DataFile exact = syntheticFile("exact.parquet", 1_024);

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(small, exact), 1_024);

        assertEquals(2, splits.size());
        assertEquals(0L, splits.get(0).start());
        assertEquals(1_000L, splits.get(0).length());
        assertEquals(0L, splits.get(1).start());
        assertEquals(1_024L, splits.get(1).length());
    }

    @Test
    void splitFilesCutsLargeFileIntoContiguousChunksAtSplitSize() {
        // 5_000 bytes / 1_024 splitSize → chunks [0,1024), [1024,2048), [2048,3072),
        // [3072,4096), [4096,5000). The last chunk is the remainder (904 bytes).
        DataFile large = syntheticFile("large.parquet", 5_000);

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(large), 1_024);

        assertEquals(5, splits.size());
        // Each chunk starts where the previous ended; lengths respect the threshold.
        long expectedStart = 0;
        for (FileScanTask split : splits) {
            assertEquals(expectedStart, split.start());
            expectedStart += split.length();
            assertTrue(split.length() <= 1_024L);
        }
        assertEquals(5_000L, expectedStart, "chunks must cover the whole file");
        assertEquals(904L, splits.get(4).length(), "last chunk is the remainder");
    }

    @Test
    void splitFilesHandlesMultipleFilesInOrder() {
        DataFile a = syntheticFile("a.parquet", 500);
        DataFile b = syntheticFile("b.parquet", 2_500);
        DataFile c = syntheticFile("c.parquet", 800);

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(a, b, c), 1_000);

        // a → 1 chunk; b → 3 chunks ([0,1000),[1000,2000),[2000,2500)); c → 1 chunk.
        assertEquals(5, splits.size());
        // File a's chunk
        assertEquals(a, splits.get(0).file());
        assertEquals(500L, splits.get(0).length());
        // File b's three chunks
        assertEquals(b, splits.get(1).file());
        assertEquals(b, splits.get(2).file());
        assertEquals(b, splits.get(3).file());
        assertEquals(1_000L, splits.get(1).length());
        assertEquals(1_000L, splits.get(2).length());
        assertEquals(500L, splits.get(3).length());
        // File c's chunk
        assertEquals(c, splits.get(4).file());
        assertEquals(800L, splits.get(4).length());
    }

    @Test
    void splitFilesSplitsAtRowGroupBoundariesWhenSplitOffsetsAvailable() {
        // 4 row groups at offsets [4, 1100, 2200, 3300]; file size 4_000. The offsets-aware
        // path (mirroring real Iceberg's OffsetsAwareSplitScanTaskIterator) produces one
        // chunk per row group: chunk[i].start = offsets[i], chunk[i].length = next offset
        // minus this offset (or fileSize minus last offset for the last chunk). splitSize
        // is ignored on this path — row groups are atomic.
        DataFile file = syntheticFileWithOffsets("multi-rg.parquet", 4_000L,
                List.of(4L, 1_100L, 2_200L, 3_300L));

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(file), 128L);

        assertEquals(4, splits.size(), "one chunk per row group");
        // Chunk 0: [4, 1100) → length 1096
        assertEquals(4L, splits.get(0).start());
        assertEquals(1_096L, splits.get(0).length());
        // Chunk 1: [1100, 2200) → length 1100
        assertEquals(1_100L, splits.get(1).start());
        assertEquals(1_100L, splits.get(1).length());
        // Chunk 2: [2200, 3300) → length 1100
        assertEquals(2_200L, splits.get(2).start());
        assertEquals(1_100L, splits.get(2).length());
        // Chunk 3 (last): [3300, 4000) → length 700 = fileSize - last offset
        assertEquals(3_300L, splits.get(3).start());
        assertEquals(700L, splits.get(3).length());
        // The four chunks tile the file exactly: start[0] + sum(lengths) == fileSize.
        long covered = splits.get(0).start();
        for (FileScanTask split : splits) {
            covered += split.length();
        }
        assertEquals(4_000L, covered, "chunks must tile the file from first offset to end");
    }

    @Test
    void splitFilesFallsBackToFixedSizeWhenOffsetsNotStrictlyAscending() {
        // Offsets [4, 4, 1100] are not strictly ascending (the first two are equal), so the
        // offsets-aware path is rejected and the fixed-size fallback runs instead. This
        // mirrors real Iceberg's BaseContentScanTask.split, which falls back to
        // FixedSizeSplitScanTaskIterator when ArrayUtil.isStrictlyAscending returns false.
        DataFile file = syntheticFileWithOffsets("bad-offsets.parquet", 4_000L,
                List.of(4L, 4L, 1_100L));

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(file), 1_000);

        // Fixed-size path: 4_000 bytes / 1_000 splitSize → 4 chunks of 1_000 each.
        assertEquals(4, splits.size());
        assertEquals(0L, splits.get(0).start(), "fixed-size fallback starts at 0");
        assertEquals(1_000L, splits.get(0).length());
    }

    @Test
    void splitFilesIgnoresSplitSizeOnOffsetsAwarePath() {
        // splitSize is irrelevant when row-group offsets are available: row groups are
        // atomic and the planner does not split them further. A tiny splitSize (1 byte)
        // still produces one chunk per row group, not chunks of 1 byte each.
        DataFile file = syntheticFileWithOffsets("multi-rg.parquet", 4_000L,
                List.of(4L, 1_100L, 2_200L, 3_300L));

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(file), 1L);

        assertEquals(4, splits.size(), "one chunk per row group regardless of splitSize");
    }

    // --- SplitPlanner.planTasks (bin-packing) ---------------------------------

    @Test
    void planTasksBinsAllSplitsIntoOneBinWhenTotalWeightFitsSplitSize() {
        // Three 1 KB chunks at openFileCost=4 MB each → total weight 12 MB ≪ 128 MB.
        DataFile a = syntheticFile("a.parquet", 1_000);
        DataFile b = syntheticFile("b.parquet", 1_000);
        DataFile c = syntheticFile("c.parquet", 1_000);

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(a, b, c), 1_000);
        List<CombinedScanTask> combined = SplitPlanner.planTasks(
                splits, 128L * 1024 * 1024, 1, 4L * 1024 * 1024);

        assertEquals(1, combined.size());
        assertEquals(3, combined.get(0).files().size());
    }

    @Test
    void planTasksOpensNewBinWhenNextChunkWouldOverflowSplitSize() {
        // 4 chunks of 4 MB each (openFileCost dominates the 1 KB length);
        // splitSize=10 MB → bin 1 fits 2 chunks (8 MB), 3rd (12 MB) overflows → new bin.
        DataFile a = syntheticFile("a.parquet", 1_000);
        DataFile b = syntheticFile("b.parquet", 1_000);
        DataFile c = syntheticFile("c.parquet", 1_000);
        DataFile d = syntheticFile("d.parquet", 1_000);

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(a, b, c, d), 1_000);
        List<CombinedScanTask> combined = SplitPlanner.planTasks(
                splits, 10L * 1024 * 1024, 1, 4L * 1024 * 1024);

        // 4 MB × 2 = 8 MB ≤ 10 MB; the third chunk would push to 12 MB > 10 MB → close bin.
        assertEquals(2, combined.size());
        assertEquals(2, combined.get(0).files().size());
        assertEquals(2, combined.get(1).files().size());
    }

    @Test
    void planTasksDoesNotProduceEmptyBins() {
        // A single tiny chunk must still produce a single non-empty bin.
        DataFile single = syntheticFile("solo.parquet", 1_000);
        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(single), 1_000);

        List<CombinedScanTask> combined = SplitPlanner.planTasks(
                splits, 1024L, 1, 0L);

        assertEquals(1, combined.size());
        assertEquals(1, combined.get(0).files().size());
    }

    @Test
    void planTasksSizeBytesSumsMemberLengths() {
        DataFile a = syntheticFile("a.parquet", 600);
        DataFile b = syntheticFile("b.parquet", 800);

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(a, b), 1_000);
        // openFileCost=0 means weight == chunk length; 600 + 800 = 1400 > 1024 splitSize,
        // so neither fits the other's bin and two single-file tasks come out. The packer
        // emits the heaviest bin first (800 before 600), so assert by value, not position.
        List<CombinedScanTask> combined = SplitPlanner.planTasks(
                splits, 1024L, 1, 0L);

        assertEquals(2, combined.size());
        List<Long> sizes = combined.stream().map(CombinedScanTask::sizeBytes).sorted().toList();
        assertEquals(List.of(600L, 800L), sizes);
    }

    @Test
    void planTasksEvictsLargestBinWhenLookbackWindowOverflows() {
        // lookback=1 keeps at most one bin in the window; when a second bin opens the
        // heaviest is emitted. Chunks a(800) and c(800) each overflow the bin holding
        // b(300), so each opens its own bin and is emitted as the heaviest; b stays in
        // the window and drains last. Output order [a]=800, [c]=800, [b]=300 is
        // largest-bin-first eviction, not FIFO.
        DataFile a = syntheticFile("a.parquet", 800);
        DataFile b = syntheticFile("b.parquet", 300);
        DataFile c = syntheticFile("c.parquet", 800);

        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(a, b, c), 1_000);
        List<CombinedScanTask> combined = SplitPlanner.planTasks(
                splits, 1_000L, 1, 0L);

        assertEquals(3, combined.size());
        assertEquals(800L, combined.get(0).sizeBytes(), "heaviest bin (a) emitted first");
        assertEquals(800L, combined.get(1).sizeBytes(), "next heaviest (c) emitted next");
        assertEquals(300L, combined.get(2).sizeBytes(), "lightest (b) drains last");
    }

    @Test
    void combinedScanTaskFilesListIsImmutable() {
        DataFile a = syntheticFile("a.parquet", 500);
        List<FileScanTask> splits = SplitPlanner.splitFiles(List.of(a), 1_000);
        List<CombinedScanTask> combined = SplitPlanner.planTasks(
                splits, 1024L, 1, 0L);

        List<FileScanTask> files = combined.get(0).files();
        // record's compact constructor calls List.copyOf; the returned list rejects add.
        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> files.add(splits.get(0)));
    }

    // --- helpers --------------------------------------------------------------

    /**
     * Build a real partitioned table and write six Parquet files across three day
     * partitions, mirroring {@code Main}. Each file holds two rows bracketing an age
     * range so the footer's lower/upper bounds are exact.
     */
    private Table buildAndWriteSixFiles() {
        Schema schema = new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();

        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(TableIdentifier.of("events"), schema, spec);

        Path dataDir = warehouse.resolve("events").resolve("data");
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 2);
        writeBracket(writer, day(20661) + 10L * 60L * 60L * 1_000_000L, 20, 30);
        writeBracket(writer, day(20661) + 11L * 60L * 60L * 1_000_000L, 45, 60);
        writeBracket(writer, day(20662) + 10L * 60L * 60L * 1_000_000L, 25, 35);
        writeBracket(writer, day(20662) + 11L * 60L * 60L * 1_000_000L, 50, 70);
        writeBracket(writer, day(20663) + 10L * 60L * 60L * 1_000_000L, 10, 20);
        writeBracket(writer, day(20663) + 11L * 60L * 60L * 1_000_000L, 55, 80);

        AppendFiles append = table.newAppend();
        for (DataFile file : writer.complete()) {
            append.appendFile(file);
        }
        append.commit();
        return table;
    }

    private void writeBracket(PartitionedWriter writer, long eventTime, int minAge, int maxAge) {
        writer.write(row(1, eventTime, minAge));
        writer.write(row(2, eventTime, maxAge));
    }

    private Map<Integer, Object> row(long id, long eventTime, int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(ID, id);
        row.put(2, "user-" + id);
        row.put(EVENT_TIME, eventTime);
        row.put(4, "demo");
        row.put(AGE, age);
        return row;
    }

    private static long day(int epochDay) {
        return epochDay * MICROS_PER_DAY;
    }

    /**
     * Build a {@link DataFile} with a synthetic size and no bounds. Only {@code path} and
     * {@code fileSizeInBytes} matter for {@link SplitPlanner} testing; the other fields
     * are left null/empty so the test focuses on byte arithmetic, not real Parquet reads.
     * The lack of {@code splitOffsets} forces {@link SplitPlanner#splitFiles} onto its
     * fixed-size fallback path.
     */
    private static DataFile syntheticFile(String path, long size) {
        return DataFile.builder(path)
                .fileSizeInBytes(size)
                .recordCount(0)
                .build();
    }

    /**
     * Build a {@link DataFile} carrying explicit row-group {@code splitOffsets}, the analog
     * of a real Parquet file's footer offsets. Drives {@link SplitPlanner#splitFiles} onto
     * its offsets-aware path so the test can probe row-group-boundary splitting directly,
     * without depending on where Parquet happens to land row groups on disk.
     */
    private static DataFile syntheticFileWithOffsets(String path, long size, List<Long> splitOffsets) {
        return DataFile.builder(path)
                .fileSizeInBytes(size)
                .recordCount(0)
                .splitOffsets(splitOffsets)
                .build();
    }
}
