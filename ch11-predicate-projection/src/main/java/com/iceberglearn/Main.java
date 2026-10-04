package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.expressions.Projections;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
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

/** Chapter 11: automatically project row predicates to partition predicates. */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

    /** Locate the project root so data lands in iceberg-java/data/ch11 regardless of cwd. */
    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch11-predicate-projection"))) {
            return cwd.resolve("data").resolve("ch11");
        }
        return cwd.getParent().resolve("data").resolve("ch11");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);

        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .day("event_time")
                .build();

        // A real warehouse directory on disk: the table's metadata tree (snapshots, manifests,
        // manifest-list) is written and read back through the catalog — no in-memory DataFiles.
        Path warehouse = BASE_DIR.resolve("warehouse");
        Files.createDirectories(warehouse);
        TableIdentifier id = TableIdentifier.of("events");

        System.out.println("[步骤1] 建一张真实的分区表（catalog 落盘 metadata）");
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(id, schema, spec);
        System.out.println("  " + spec.toString().replace("\n", "\n  "));

        // PartitionedWriter writes real Parquet files under the table's data/ directory and reads
        // each file's footer for true column statistics; committing them through AppendFiles writes
        // a manifest + snapshot to the table's metadata directory.
        System.out.println("\n[步骤2] 写 6 个文件（3 天 × 2 文件），经 AppendFiles 提交成快照");
        java.nio.file.Path dataDir = java.nio.file.Path.of(table.location()).resolve("data");
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 2);
        writeRange(writer, 20661L * MICROS_PER_DAY, 10, 25);
        writeRange(writer, 20661L * MICROS_PER_DAY + 12L * MICROS_PER_HOUR, 55, 70);
        writeRange(writer, 20662L * MICROS_PER_DAY, 30, 45);
        writeRange(writer, 20662L * MICROS_PER_DAY + 12L * MICROS_PER_HOUR, 60, 80);
        writeRange(writer, 20663L * MICROS_PER_DAY, 15, 35);
        writeRange(writer, 20663L * MICROS_PER_DAY + 12L * MICROS_PER_HOUR, 75, 90);
        AppendFiles append = table.newAppend();
        for (DataFile file : writer.complete()) {
            append.appendFile(file);
        }
        append.commit();

        long dayStart = 20661L * MICROS_PER_DAY;
        long nextDayStart = 20662L * MICROS_PER_DAY;
        Expr oneDay = Exprs.and(
                Exprs.greaterThanOrEqual(EVENT_TIME, dayStart),
                Exprs.lessThan(EVENT_TIME, nextDayStart));
        Expr rowFilter = Exprs.and(oneDay, Exprs.greaterThan(AGE, 50));

        System.out.println("\n[步骤3] 用户只写源列谓词");
        System.out.println("  row filter: " + rowFilter);
        System.out.println("  用户不知道 partition field-id，也不用计算 epoch day。");

        // Projection itself is visible here for teaching, but in the real path it happens inside
        // ManifestGroup — scan.filter() never exposes a partition predicate to the caller.
        Expr partitionFilter = Projections.inclusive(spec).project(rowFilter);
        System.out.println("\n[步骤4] inclusive projection 自动生成分区谓词");
        System.out.println("  partition filter: " + partitionFilter);
        System.out.println("  event_time >= 当天零点  -> day >= 20661");
        System.out.println("  event_time <  次日零点  -> day <= 20661");
        System.out.println("  age > 50 没有分区字段   -> true");

        System.out.println("\n[步骤5] 一条 row filter 驱动 scan 的两级裁剪（读真实快照）");
        List<DataFile> planned = table.newScan().filter(rowFilter).planFiles();
        System.out.println("\n  最终读取:");
        for (DataFile file : planned) {
            System.out.println("    " + file.path());
        }

        demonstrateRangeBoundary(spec, dayStart);
        demonstrateBucketFallback(schema);
    }

    private static void demonstrateRangeBoundary(PartitionSpec spec, long dayStart) {
        long tenAm = dayStart + 10L * MICROS_PER_HOUR;
        Expr source = Exprs.greaterThan(EVENT_TIME, tenAm);
        Expr projected = Projections.inclusive(spec).project(source);

        System.out.println("\n[步骤6] 为什么 > 会投影成 >=");
        System.out.println("  source:    " + source);
        System.out.println("  projected: " + projected);
        System.out.println("  10 点之后的行仍在 day=20661；保留整天才能保证不漏数据。");
    }

    private static void demonstrateBucketFallback(Schema schema) {
        PartitionSpec bucketSpec = PartitionSpec.builderFor(schema).bucket("age", 16).build();
        Expr equality = Projections.inclusive(bucketSpec).project(Exprs.equal(AGE, 50));
        Expr range = Projections.inclusive(bucketSpec).project(Exprs.greaterThan(AGE, 50));

        System.out.println("\n[步骤7] bucket 只能安全投影等值谓词");
        System.out.println("  age = 50  -> " + equality);
        System.out.println("  age > 50  -> " + range + "（hash bucket 不保序，无法按范围裁剪）");
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

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(1, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")));
    }

    private static void writeRange(PartitionedWriter writer, long eventTime, int minAge, int maxAge) {
        writer.write(row(1, eventTime, minAge));
        writer.write(row(2, eventTime, maxAge));
    }

    private static Map<Integer, Object> row(long id, long eventTime, int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(1, id);
        row.put(2, "user-" + id);
        row.put(EVENT_TIME, eventTime);
        row.put(4, "demo");
        row.put(AGE, age);
        return row;
    }
}
