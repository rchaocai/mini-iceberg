package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.scan.CombinedScanTask;
import com.iceberglearn.scan.FileScanTask;
import com.iceberglearn.scan.SplitPlanner;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Chapter 14: scan planning — turn pruned files into balanced tasks for an engine. */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    private static final int ID = 1;
    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

    /**
     * Rows written per file. Big enough that with the small row-group size below each file
     * lands multiple row groups, which is what makes row-group-boundary splitting visible
     * in step 7. The size here is sized for the printed output, not for realism.
     */
    private static final int ROWS_PER_FILE = 500;

    /**
     * Target Parquet row-group size in bytes. Set small (1 KB) so each file lands ~20 row
     * groups; the resulting chunk list is then long enough to show the row-group-boundary
     * pattern clearly without being unreadable. Production systems leave this at the default
     * 128 MB; the teaching value here is in showing what split_offsets does, not in
     * matching production sizes.
     */
    private static final int ROW_GROUP_SIZE = 1024;

    /** Parquet page size; small so a row group holds more than one page per column. */
    private static final int PAGE_SIZE = 64;

    /** Number of chunks per file to print in full before truncating with "..." in step 7. */
    private static final int CHUNKS_TO_PRINT_IN_FULL = 3;

    /** Locate the project root so data lands in the project's data/ directory regardless of cwd. */
    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch14-scan-planning"))) {
            return cwd.resolve("data").resolve("ch14");
        }
        return cwd.getParent().resolve("data").resolve("ch14");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);

        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();

        System.out.println("[步骤1] 建表：events，分区策略 day(event_time)");
        Path warehouse = BASE_DIR.resolve("warehouse");
        Files.createDirectories(warehouse);
        TableIdentifier tableId = TableIdentifier.of("events");
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(tableId, schema, spec);
        System.out.println("  " + spec.toString().replace("\n", "\n  "));

        // Three day partitions × two files each. Each file writes ROWS_PER_FILE rows whose
        // ages bracket [minAge, maxAge], so the footer's lower/upper bounds make the file's
        // age window exact. The pattern deliberately gives every day one prunable file
        // (low max age) and one surviving file (high max age), so both partition pruning
        // and metrics pruning have work to do in later steps. Writing many rows per file
        // (rather than two) is what makes each file land multiple Parquet row groups, which
        // is what makes step 7's row-group-boundary splitting visible.
        System.out.println("\n[步骤2] 写入 6 个 Parquet 文件，每文件 "
                + ROWS_PER_FILE + " 行、rowGroupSize=" + ROW_GROUP_SIZE + "B，每文件多个 row group");
        Path dataDir = Path.of(table.location()).resolve("data");
        writeFile(table, spec, dataDir, day(20661) + 10 * MICROS_PER_HOUR, 20, 30);
        writeFile(table, spec, dataDir, day(20661) + 11 * MICROS_PER_HOUR, 45, 60);
        writeFile(table, spec, dataDir, day(20662) + 10 * MICROS_PER_HOUR, 25, 35);
        writeFile(table, spec, dataDir, day(20662) + 11 * MICROS_PER_HOUR, 50, 70);
        writeFile(table, spec, dataDir, day(20663) + 10 * MICROS_PER_HOUR, 10, 20);
        writeFile(table, spec, dataDir, day(20663) + 11 * MICROS_PER_HOUR, 55, 80);
        System.out.println("  6 个文件已落盘，覆盖 day 20661/20662/20663 各 2 个");

        System.out.println("\n[步骤3] planFiles() 不带过滤：6 个候选文件，逐个打印其 day 分区与 age 指标");
        List<DataFile> allFiles = table.newScan().planFiles();
        printFiles(allFiles, "  ");

        // Pure data-column filter: projection onto day(event_time) yields AlwaysTrue (age is
        // not a partition source), so partition pruning holds at 6. Metrics pruning then
        // discards every file whose max(age) <= 40 — three survivors, one per day.
        System.out.println("\n[步骤4] 带 age > 40 谓词：谓词不含分区列，分区裁剪 no-op，指标裁剪跳过 max(age) <= 40 的文件");
        Expr ageFilter = Exprs.greaterThan(AGE, 40L);
        List<DataFile> ageSurviving = table.newScan().filter(ageFilter).planFiles();
        printFiles(ageSurviving, "  ");

        // Combined filter: event_time < day(20662) ∧ age > 40. The event_time side projects
        // onto day < 20662, so partition pruning drops everything in days 20662 and 20663
        // before metrics even runs. Of the two survivors in day 20661, only the high-age
        // file passes age > 40. This is the funnel a real query hits: 6 → 2 → 1.
        System.out.println("\n[步骤5] 带 event_time < day(20662) ∧ age > 40 谓词：分区裁剪砍掉 day 20662/20663，指标裁剪再砍掉低 age 文件");
        Expr rowFilter = Exprs.and(
                Exprs.lessThan(EVENT_TIME, day(20662)),
                Exprs.greaterThan(AGE, 40L));
        List<DataFile> combinedSurviving = table.newScan().filter(rowFilter).planFiles();
        printFiles(combinedSurviving, "  ");

        // planTasks() runs planFiles() then feeds the survivors through SplitPlanner. Each
        // surviving file now has multiple row groups, so splitFiles produces one chunk per
        // row group (chunk.start = row-group startingPos, never mid-row-group). With default
        // splitSize 128 MB all those chunks still bin-pack into one CombinedScanTask.
        System.out.println("\n[步骤6] planTasks() 默认 128 MB splitSize：每个 row group 一个 chunk，全部 bin-pack 进一个 CombinedScanTask");
        List<CombinedScanTask> tasks = table.newScan().filter(ageFilter).planTasks();
        printTasks(tasks, "  ");

        // Splitting demo: feed the same surviving files through SplitPlanner.splitFiles
        // directly. Each file's chunks now start at the row-group offsets read from the
        // Parquet footer (splitOffsets in the manifest) — not at byte-threshold boundaries.
        // The splitSize argument is ignored on this path: row groups are atomic and the
        // planner never splits one further.
        System.out.println("\n[步骤7] 演示 split：每个文件按 row group 边界切成多个 FileScanTask（chunk 起点即 row group 起始偏移）");
        List<FileScanTask> splits = SplitPlanner.splitFiles(ageSurviving, SplitPlanner.DEFAULT_SPLIT_SIZE);
        printSplits(splits, "  ");

        System.out.println("\n[步骤8] 总结：scan planning 四步与三块拼图");
        System.out.println("  1. 快照定位 → 2. 列出文件 → 3. 两级裁剪（planFiles）→ 4. split + bin-pack（planTasks）");
        System.out.println("  第 4/9/10 章的三块拼图（快照隔离、数据跳过、谓词投影）在这里被接成一条完整流水线。");
        System.out.println("  ScanTask 是规划与执行的桥梁：执行引擎只消费 task，不接触 manifest/metadata。");
        System.out.println("  切分按 row group 边界（footer 的 split_offsets）；");
        System.out.println("  字节阈值切分仅在 splitOffsets 缺失时作为回退分支。");
    }

    // --- helpers ---------------------------------------------------------------

    private static void printFiles(List<DataFile> files, String indent) {
        for (DataFile file : files) {
            int rowGroups = file.splitOffsets() != null ? file.splitOffsets().size() : 0;
            System.out.println(indent + file.path()
                    + "  partition=" + file.partition()
                    + "  size=" + file.fileSizeInBytes() + "B"
                    + "  records=" + file.recordCount()
                    + "  rowGroups=" + rowGroups
                    + "  age=[" + bound(file, true) + ", " + bound(file, false) + "]");
        }
        System.out.println(indent + "共 " + files.size() + " 个文件");
    }

    private static void printTasks(List<CombinedScanTask> tasks, String indent) {
        for (int i = 0; i < tasks.size(); i++) {
            CombinedScanTask task = tasks.get(i);
            // Group FileScanTasks by file for a compact per-file summary; one row group
            // per chunk means the chunk list is too long to print line by line.
            Map<DataFile, List<FileScanTask>> byFile = new LinkedHashMap<>();
            for (FileScanTask fileTask : task.files()) {
                byFile.computeIfAbsent(fileTask.file(), f -> new ArrayList<>()).add(fileTask);
            }
            System.out.println(indent + "CombinedScanTask #" + (i + 1)
                    + "  sizeBytes=" + task.sizeBytes()
                    + "  files=" + byFile.size()
                    + "  totalChunks=" + task.files().size());
            for (Map.Entry<DataFile, List<FileScanTask>> e : byFile.entrySet()) {
                long fileTotal = e.getValue().stream().mapToLong(FileScanTask::length).sum();
                int rowGroups = e.getKey().splitOffsets() != null
                        ? e.getKey().splitOffsets().size() : 0;
                System.out.println(indent + "  " + e.getKey().path()
                        + "  chunks=" + e.getValue().size()
                        + "  bytes=" + fileTotal
                        + "  rowGroups=" + rowGroups);
            }
        }
        System.out.println(indent + "共 " + tasks.size() + " 个 CombinedScanTask");
    }

    private static void printSplits(List<FileScanTask> splits, String indent) {
        Map<DataFile, List<FileScanTask>> byFile = new LinkedHashMap<>();
        for (FileScanTask split : splits) {
            byFile.computeIfAbsent(split.file(), f -> new ArrayList<>()).add(split);
        }
        for (Map.Entry<DataFile, List<FileScanTask>> e : byFile.entrySet()) {
            DataFile file = e.getKey();
            List<FileScanTask> chunks = e.getValue();
            int rowGroupCount = file.splitOffsets() != null ? file.splitOffsets().size() : 0;
            System.out.println(indent + file.path()
                    + "  size=" + file.fileSizeInBytes() + "B"
                    + "  rowGroups=" + rowGroupCount
                    + "  → 切成 " + chunks.size() + " 个 chunk（每 row group 一个）:");
            int showCount = Math.min(chunks.size(), CHUNKS_TO_PRINT_IN_FULL);
            for (int i = 0; i < showCount; i++) {
                FileScanTask chunk = chunks.get(i);
                System.out.println(indent + "  chunk[" + i + "]  start=" + chunk.start()
                        + "  length=" + chunk.length()
                        + (i == 0 ? "  ← 第 1 个 row group 起始偏移" : ""));
            }
            if (chunks.size() > showCount) {
                System.out.println(indent + "  ... 共 " + chunks.size() + " 个 chunk，已省略中间 "
                        + (chunks.size() - showCount - 1) + " 个");
                FileScanTask last = chunks.get(chunks.size() - 1);
                System.out.println(indent + "  chunk[" + (chunks.size() - 1) + "]  start=" + last.start()
                        + "  length=" + last.length()
                        + "  ← 最后一个 row group");
            }
        }
        System.out.println(indent + "共 " + splits.size() + " 个 FileScanTask");
    }

    /** Format the lower (or upper) bound of the age column, or "?" if missing. */
    private static Object bound(DataFile file, boolean lower) {
        Map<Integer, Object> bounds = lower ? file.lowerBounds() : file.upperBounds();
        return bounds != null && bounds.containsKey(AGE) ? bounds.get(AGE) : "?";
    }

    /**
     * Write one file holding {@code ROWS_PER_FILE} rows whose ages interpolate from
     * {@code minAge} to {@code maxAge}. The first row is exactly {@code minAge} (gives the
     * footer's lower bound) and the last is exactly {@code maxAge} (gives the upper bound);
     * rows in between fill the range so several Parquet row groups land per file.
     */
    private static void writeFile(Table table, PartitionSpec spec, Path dataDir,
                                  long eventTime, int minAge, int maxAge) {
        PartitionedWriter writer = new PartitionedWriter(
                schema(), spec, dataDir, ROWS_PER_FILE, ROW_GROUP_SIZE, PAGE_SIZE);
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            int age = ROWS_PER_FILE == 1 ? minAge
                    : minAge + (int) ((long) (maxAge - minAge) * i / (ROWS_PER_FILE - 1));
            writer.write(row(i + 1, eventTime, age));
        }
        AppendFiles append = table.newAppend();
        for (DataFile file : writer.complete()) {
            append.appendFile(file);
        }
        append.commit();
    }

    private static long day(int epochDay) {
        return epochDay * MICROS_PER_DAY;
    }

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")));
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

    private static void cleanup(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
