package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.expressions.And;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.expressions.GreaterThan;
import com.iceberglearn.expressions.LessThan;
import com.iceberglearn.expressions.Or;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.manifests.ManifestGroup;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metrics.MetricsFilter;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Chapter 10: data skipping — read no data file until the manifest says it might match. */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    // The "age" column lives at field id 5 — the same id we'll use in every predicate.
    private static final int AGE_FIELD_ID = 5;

    // Step-1 schema: 1=id/long, 2=name/string, 5=age/long. Bounds come from real rows.
    private static final Schema ROW_SCHEMA = new Schema(List.of(
            Schema.NestedField.required(1, "id", "long"),
            Schema.NestedField.required(2, "name", "string"),
            Schema.NestedField.required(AGE_FIELD_ID, "age", "long")
    ));

    private static final long MICROS_PER_DAY = 24L * 60L * 60L * 1_000_000L;

    /** Locate the project root so data lands in iceberg-java/data/ch10 regardless of cwd. */
    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch10-data-skipping"))) {
            return cwd.resolve("data").resolve("ch10");
        }
        return cwd.getParent().resolve("data").resolve("ch10");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);
        System.out.println("[步骤1] 写入行收集统计，得到 10 个 DataFile（不同的年龄范围）");
        List<DataFile> files = createTestFiles();
        printFiles(files);

        System.out.println("\n[步骤2] 演示单个谓词的指标裁剪");
        Expr gtFilter = Exprs.greaterThan(AGE_FIELD_ID, 50);
        System.out.println("\n  查询谓词: " + gtFilter);
        System.out.println("\n  按指标裁剪：");
        runMetricsOnly(gtFilter, files);

        System.out.println("\n[步骤3] 演示复杂谓词（AND）");
        Expr andFilter = Exprs.and(Exprs.greaterThan(AGE_FIELD_ID, 30), Exprs.lessThan(AGE_FIELD_ID, 60));
        System.out.println("\n  查询谓词: " + andFilter);
        System.out.println("\n  按指标裁剪：");
        runMetricsOnly(andFilter, files);

        System.out.println("\n[步骤4] 演示 OR 谓词");
        Expr orFilter = Exprs.or(Exprs.lessThan(AGE_FIELD_ID, 20), Exprs.greaterThan(AGE_FIELD_ID, 70));
        System.out.println("\n  查询谓词: " + orFilter);
        System.out.println("\n  按指标裁剪：");
        runMetricsOnly(orFilter, files);

        System.out.println("\n[步骤5] 演示两级裁剪（分区裁剪 + 指标裁剪）");
        demonstrateTwoStagePruning();

        System.out.println("\n[步骤6] 演示保守跳过原则");
        demonstrateConservativeSkipping();

    }

    // ---- Step 1: write 10 files through PartitionedWriter ------------------

    private static List<DataFile> createTestFiles() throws IOException {
        Path dataDir = BASE_DIR.resolve("demo").resolve("data");
        Files.createDirectories(dataDir);
        PartitionedWriter writer = new PartitionedWriter(
                ROW_SCHEMA, PartitionSpec.unpartitioned(), dataDir, 2);

        writeAgeRange(writer, 10, 25);
        writeAgeRange(writer, 26, 40);
        writeAgeRange(writer, 41, 55);
        writeAgeRange(writer, 56, 70);
        writeAgeRange(writer, 71, 85);
        writeAgeRange(writer, 86, 100);
        writeAgeRange(writer, 5, 15);
        writeAgeRange(writer, 45, 65);
        writeAgeRange(writer, 35, 50);
        writeAgeRange(writer, 60, 75);

        return writer.complete();
    }

    private static void writeAgeRange(PartitionedWriter writer, int minAge, int maxAge) {
        writer.write(ageRow(minAge));
        writer.write(ageRow(maxAge));
    }

    private static Map<Integer, Object> ageRow(int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(1, (long) age);
        row.put(2, "user-" + age);
        row.put(AGE_FIELD_ID, age);
        return row;
    }

    private static void printFiles(List<DataFile> files) {
        System.out.println("\n  创建了 " + files.size() + " 个 DataFile：");
        for (DataFile f : files) {
            System.out.printf("    %s: age=[%s, %s]%n",
                    f.path(), f.lowerBounds().get(AGE_FIELD_ID), f.upperBounds().get(AGE_FIELD_ID));
        }
    }

    // ---- Steps 2–4: metrics-only pruning ----------------------------------

    private static void runMetricsOnly(Expr filter, List<DataFile> files) {
        for (DataFile f : files) {
            boolean canMatch = MetricsFilter.canMatch(filter, f);
            String verdict = canMatch ? "✓ 保留" : "✗ 跳过";
            String reason = explainVerdict(filter, f);
            System.out.printf("    %s: age=[%s, %s]   → %s%s%n",
                    f.path(),
                    f.lowerBounds().get(AGE_FIELD_ID),
                    f.upperBounds().get(AGE_FIELD_ID),
                    verdict,
                    reason.isEmpty() ? "" : "（" + reason + "）");
        }
    }

    /** Produce a one-line explanation of why the filter kept/skipped this file. */
    private static String explainVerdict(Expr filter, DataFile f) {
        long min = (Long) f.lowerBounds().get(AGE_FIELD_ID);
        long max = (Long) f.upperBounds().get(AGE_FIELD_ID);
        if (filter instanceof GreaterThan gt) {
            long v = ((Number) gt.value()).longValue();
            return canMatchVerbose(filter, f)
                    ? "max=" + max + " > " + v + "，可能有匹配"
                    : "max=" + max + " ≤ " + v;
        }
        if (filter instanceof LessThan lt) {
            long v = ((Number) lt.value()).longValue();
            return canMatchVerbose(filter, f)
                    ? "min=" + min + " < " + v + "，可能有匹配"
                    : "min=" + min + " ≥ " + v;
        }
        if (filter instanceof And and) {
            boolean l = MetricsFilter.canMatch(and.left(), f);
            boolean r = MetricsFilter.canMatch(and.right(), f);
            if (l && r) {
                return "左右都可能命中";
            }
            return "左边" + (l ? "可能" : "不可能") + "，右边" + (r ? "可能" : "不可能");
        }
        if (filter instanceof Or or) {
            boolean l = MetricsFilter.canMatch(or.left(), f);
            boolean r = MetricsFilter.canMatch(or.right(), f);
            if (l || r) {
                return "至少一边可能命中（左" + (l ? "✓" : "✗") + " 右" + (r ? "✓" : "✗") + "）";
            }
            return "两边都不可能命中";
        }
        return "";
    }

    private static boolean canMatchVerbose(Expr filter, DataFile f) {
        return MetricsFilter.canMatch(filter, f);
    }

    // ---- Step 5: two-stage pruning with a real PartitionedWriter --------------------------
    //
    // Each of the 3 day-partitions gets two batches of rows with disjoint age ranges; with
    // targetRowsPerFile = 2 each batch flushes its own DataFile, yielding 6 files whose
    // collected bounds reproduce the original demo ranges.

    private static void demonstrateTwoStagePruning() throws IOException {
        Schema schema = new Schema(List.of(
                Schema.NestedField.required(1, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(3, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE_FIELD_ID, "age", "long")
        ));

        PartitionSpec specDay = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .day("event_time")
                .build();

        System.out.println("\n  PartitionSpec: " + specDay.toString().replace("\n", "\n  "));

        // Write + commit through the real catalog path: the files live in a snapshot on disk,
        // and the scan below reads them back from the manifest — no in-memory DataFile list.
        Path warehouse = BASE_DIR.resolve("warehouse");
        Files.createDirectories(warehouse);
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(TableIdentifier.of("events"), schema, specDay);
        Path dataDir = Path.of(table.location()).resolve("data");
        AppendFiles append = table.newAppend();
        for (DataFile f : createPartitionedTestFiles(schema, specDay, dataDir)) {
            append.appendFile(f);
        }
        append.commit();
        List<DataFile> partitionedFiles = table.newScan().planFiles();

        System.out.println("\n  带分区的测试文件（跨越3天，从快照读回）:");
        for (DataFile f : partitionedFiles) {
            System.out.printf("    %s: partition=%s, age=[%s, %s]%n",
                    f.path(), f.partition(),
                    f.lowerBounds().get(AGE_FIELD_ID), f.upperBounds().get(AGE_FIELD_ID));
        }

        // Partition predicate: keep only files in day 20661.
        // The partition field ID (1000) comes from the PartitionSpec.
        int dayFieldId = specDay.fields().get(0).fieldId();
        Expr partitionEq20661 = Exprs.equal(dayFieldId, 20661);

        // Data predicate: age > 50
        Expr ageGt50 = Exprs.greaterThan(AGE_FIELD_ID, 50);

        System.out.println("\n  查询: event_time_day = 20661 AND age > 50");
        System.out.println("  分区谓词: Equal(" + dayFieldId + ", 20661) — 只保留 day=20661 的文件");
        System.out.println("  数据谓词: GreaterThan(" + AGE_FIELD_ID + ", 50) — 再按 age 范围过滤\n");

        ManifestGroup group = new ManifestGroup(partitionedFiles, specDay);
        List<DataFile> result = group.filterPartitions(partitionEq20661)
                .filterData(ageGt50)
                .planFiles();

        System.out.println("\n  最终需要读取的文件:");
        for (DataFile f : result) {
            System.out.println("    ✓ " + f.path());
        }
    }

    private static List<DataFile> createPartitionedTestFiles(Schema schema, PartitionSpec spec, Path dataDir) {
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 2);

        long day20661 = 20661L * MICROS_PER_DAY;
        long day20662 = 20662L * MICROS_PER_DAY;
        long day20663 = 20663L * MICROS_PER_DAY;

        // Two batches per partition; each batch's two rows flush into one file with the batch's
        // age min/max as the collected bounds.
        writePartitionedRow(writer, day20661, 10, 25);
        writePartitionedRow(writer, day20661, 55, 70);
        writePartitionedRow(writer, day20662, 30, 45);
        writePartitionedRow(writer, day20662, 60, 80);
        writePartitionedRow(writer, day20663, 15, 35);
        writePartitionedRow(writer, day20663, 75, 90);

        return writer.complete();
    }

    private static void writePartitionedRow(PartitionedWriter writer, long eventTime,
                                            int ageMin, int ageMax) {
        writer.write(row(1, "u-a", eventTime, "x", ageMin));
        writer.write(row(2, "u-b", eventTime, "x", ageMax));
    }

    private static Map<Integer, Object> row(long id, String name, long eventTime,
                                            String category, int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(1, id);
        row.put(2, name);
        row.put(3, eventTime);
        row.put(4, category);
        row.put(AGE_FIELD_ID, age);
        return row;
    }

    // ---- Step 6: conservative skipping ------------------------------------

    private static void demonstrateConservativeSkipping() {
        DataFile noStatsFile = DataFile.builder("data/no-stats.parquet")
                .recordCount(100)
                .fileSizeInBytes(10240)
                .lowerBounds(Map.of())   // empty map: table exists, but no column stats
                .upperBounds(Map.of())
                .build();

        boolean canMatch = MetricsFilter.canMatch(Exprs.greaterThan(AGE_FIELD_ID, 50), noStatsFile);

        System.out.println("\n  文件: " + noStatsFile.path() + "（没有统计信息）");
        System.out.println("  裁剪结果: " + (canMatch ? "✓ 保留" : "✗ 跳过") + "（保守跳过）");
        System.out.println("\n  原因：没有统计信息时，无法确定是否有匹配数据，必须保守地保留。");
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
