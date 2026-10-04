package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionField;
import com.iceberglearn.partition.PartitionKey;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.partition.PartitionUtil;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;
import com.iceberglearn.transforms.Bucket;
import com.iceberglearn.transforms.Days;
import com.iceberglearn.transforms.Hours;
import com.iceberglearn.transforms.Identity;
import com.iceberglearn.transforms.Truncate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Chapter 9: hidden partitioning — the same rows, three specs, one flat layout. */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    // 2026-07-27 00:00:00 UTC, expressed in microseconds since the epoch (Iceberg TimestampType).
    private static final long BASE_TIME_MICROS = 1785110400000000L;
    private static final long MICROS_PER_DAY = 24L * 60L * 60L * 1_000_000L;

    /** Locate the project root so data lands in iceberg-java/data/ch09 regardless of cwd. */
    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        // From project root: cwd contains the ch09 module directory
        if (Files.isDirectory(cwd.resolve("ch09-hidden-partitioning"))) {
            return cwd.resolve("data").resolve("ch09");
        }
        // From module directory: parent is the project root
        return cwd.getParent().resolve("data").resolve("ch09");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);
        Schema schema = new Schema(List.of(
                Schema.NestedField.required(1, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(3, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string")
        ));

        System.out.println("[步骤1] 定义 Schema 和 PartitionSpec");
        System.out.println("  Schema: " + schema);

        PartitionSpec specDay = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .day("event_time")
                .build();
        System.out.println("\n  PartitionSpec (按天分区):");
        System.out.println("    " + specDay.toString().replace("\n", "\n    "));

        List<Map<Integer, Object>> rows = sampleRows();
        System.out.println("\n[步骤2] 准备输入数据（8行，跨越3天，顺序故意打乱）");
        for (Map<Integer, Object> row : rows) {
            int day = (int) Days.get().apply(row.get(3));
            System.out.printf("  id=%s, name=%s, day=%d, category=%s%n",
                    row.get(1), row.get(2), day, row.get(4));
        }

        System.out.println("\n[步骤3] 用 PartitionedWriter 写入（每5行刷一个文件）");
        Path demoDataDir = BASE_DIR.resolve("demo").resolve("data");
        Files.createDirectories(demoDataDir);
        PartitionedWriter writer = new PartitionedWriter(schema, specDay, demoDataDir, 5);
        for (Map<Integer, Object> row : rows) {
            writer.write(row);
        }
        List<DataFile> files = writer.complete();

        System.out.println("\n[步骤4] 生成的 DataFile 清单（按分区分组）");
        printFilesGroupedByPartition(files);

        System.out.println("\n[步骤5] 提交到 catalog 并读回：分区归属真正落进快照");
        commitAndReadBack(schema, specDay, rows);

        System.out.println("\n[步骤6] 逐字段演示 PartitionKey 计算");
        demonstratePartitionKey(schema, specDay);

        System.out.println("\n[步骤7] 五种 Transform 的实际效果");
        demonstrateTransforms();

        System.out.println("\n[步骤8] 同一份数据，三种 spec，文件路径都是 data/ 下的 UUID 名");
        demonstrateSameDataDifferentSpecs(schema, rows);

        System.out.println("\n[步骤9] Hive 分区 vs Iceberg 隐藏分区");
        printHiveVsIcebergComparison();
    }

    private static List<Map<Integer, Object>> sampleRows() {
        return List.of(
                row(1, "Alice", BASE_TIME_MICROS, "electronics"),
                row(2, "Bob", BASE_TIME_MICROS, "books"),
                row(3, "Charlie", BASE_TIME_MICROS + MICROS_PER_DAY, "electronics"),
                row(4, "David", BASE_TIME_MICROS + MICROS_PER_DAY, "books"),
                row(5, "Eve", BASE_TIME_MICROS + MICROS_PER_DAY * 2, "electronics"),
                row(6, "Frank", BASE_TIME_MICROS + MICROS_PER_DAY * 2, "books"),
                row(7, "Grace", BASE_TIME_MICROS, "electronics"),
                row(8, "Henry", BASE_TIME_MICROS + MICROS_PER_DAY, "books")
        );
    }

    private static Map<Integer, Object> row(int id, String name, long eventTime, String category) {
        // Field id -> value, the same shape a Parquet row carries internally.
        Map<Integer, Object> r = new LinkedHashMap<>();
        r.put(1, id);
        r.put(2, name);
        r.put(3, eventTime);
        r.put(4, category);
        return r;
    }

    private static void printFilesGroupedByPartition(List<DataFile> files) {
        // Bucket files by their partition tuple so the reader sees one group per partition,
        // regardless of the order HashMap happened to flush them in.
        Map<List<Object>, List<DataFile>> byPartition = new LinkedHashMap<>();
        for (DataFile f : files) {
            byPartition.computeIfAbsent(f.partition(), k -> new java.util.ArrayList<>()).add(f);
        }
        for (Map.Entry<List<Object>, List<DataFile>> entry : byPartition.entrySet()) {
            System.out.println("  ── 分区: " + entry.getKey());
            for (DataFile f : entry.getValue()) {
                System.out.printf("    └── 文件: %s, 行数: %d%n", f.path(), f.recordCount());
            }
        }
    }

    /**
     * Close the write→read loop: build a real partitioned table, write the rows into the table's
     * own data directory, commit them through AppendFiles, then read back through the scan. The
     * partition tuple on each DataFile survives the manifest/snapshot round-trip — that is what
     * makes the partition "hidden but real": it lives in metadata, not in a directory path.
     */
    private static void commitAndReadBack(Schema schema, PartitionSpec spec, List<Map<Integer, Object>> rows)
            throws IOException {
        String warehouse = BASE_DIR.resolve("warehouse").toString();
        Table table = new FileSystemCatalog(warehouse)
                .createTable(TableIdentifier.of("events"), schema, spec);

        Path dataDir = Path.of(table.location()).resolve("data");
        PartitionedWriter writer = new PartitionedWriter(schema, spec, dataDir, 5);
        for (Map<Integer, Object> row : rows) {
            writer.write(row);
        }

        AppendFiles append = table.newAppend();
        for (DataFile file : writer.complete()) {
            append.appendFile(file);
        }
        append.commit();

        System.out.println("  从快照读回的文件（partition tuple 来自 manifest 元数据）:");
        for (DataFile file : table.newScan().planFiles()) {
            System.out.printf("    %s  partition=%s  行数=%d%n",
                    file.path(), file.partition(), file.recordCount());
        }
        System.out.println("  观察写入时路由出的 partition tuple，读回后原样保留在 DataFile 元数据中。");
    }

    private static void demonstratePartitionKey(Schema schema, PartitionSpec specDay) {
        Map<Integer, Object> sample = row(1, "Alice", BASE_TIME_MICROS, "electronics");
        System.out.println("  输入行: " + sample);
        System.out.println("  spec: " + specDay.toString().replace("\n", " "));

        PartitionKey key = new PartitionKey(specDay);
        key.partition(sample);
        System.out.println("  PartitionKey.partition(row) 逐字段计算:");
        for (int i = 0; i < specDay.fields().size(); i++) {
            PartitionField field = specDay.fields().get(i);
            Object source = sample.get(field.sourceId());
            Object transformed = PartitionUtil.transformValue(field.transform(), source);
            System.out.printf("    field[%d]: sourceId=%d, sourceValue=%s, transform=%s, 结果=%s%n",
                    i, field.sourceId(), source, field.transform(), transformed);
        }
        System.out.println("  最终 PartitionKey: " + key);
    }

    private static void demonstrateTransforms() {
        long t = BASE_TIME_MICROS;
        System.out.printf("  identity(123)         = %s%n", Identity.get().apply(123));
        System.out.printf("  day(%d)        = %s%n", t, Days.get().apply(t));
        System.out.printf("  hour(%d)       = %s%n", t, Hours.get().apply(t));
        System.out.printf("  bucket[4](\"electronics\") = %s%n", Bucket.get(4).apply("electronics"));
        System.out.printf("  bucket[4](\"books\")       = %s%n", Bucket.get(4).apply("books"));
        System.out.printf("  truncate[3](\"Alice\")     = %s%n", Truncate.get(3).apply("Alice"));
        System.out.printf("  truncate[10](23)         = %s%n", Truncate.get(10).apply(23));
        System.out.printf("  truncate[10](-23)        = %s%n", Truncate.get(10).apply(-23));
    }

    private static void demonstrateSameDataDifferentSpecs(Schema schema, List<Map<Integer, Object>> rows)
            throws IOException {
        PartitionSpec specIdentity = PartitionSpec.builderFor(schema).withSpecId(2).identity("category").build();
        PartitionSpec specDay = PartitionSpec.builderFor(schema).withSpecId(1).day("event_time").build();
        PartitionSpec specBucket = PartitionSpec.builderFor(schema).withSpecId(3).bucket("category", 4).build();

        System.out.println("  spec-day:        " + specDay.toString().replace("\n", " "));
        System.out.println("  spec-identity:   " + specIdentity.toString().replace("\n", " "));
        System.out.println("  spec-bucket[4]:  " + specBucket.toString().replace("\n", " "));

        Path baseDir = BASE_DIR.resolve("specs");
        for (Map.Entry<String, PartitionSpec> e : Map.of(
                "spec-day", specDay, "spec-identity", specIdentity, "spec-bucket", specBucket).entrySet()) {
            Path dataDir = baseDir.resolve(e.getKey()).resolve("data");
            Files.createDirectories(dataDir);
            PartitionedWriter w = new PartitionedWriter(schema, e.getValue(), dataDir, 5);
            for (Map<Integer, Object> r : rows) {
                w.write(r);
            }
            List<DataFile> fs = w.complete();
            System.out.println("\n  [" + e.getKey() + "] 写出的文件:");
            for (DataFile f : fs) {
                System.out.printf("    %s  partition=%s  行数=%d%n", f.path(), f.partition(), f.recordCount());
            }
        }
        System.out.println("\n  观察: 三个 spec 写出的文件路径都是 data/ 下的 UUID 名 (无分区目录);");
        System.out.println("        差异只在 partition tuple —— 分区是元数据里的声明，不是物理目录。");
    }

    private static void printHiveVsIcebergComparison() {
        System.out.println("  Hive 分区:");
        System.out.println("    /dt=2026-07-27/part-000.parquet  <- Alice, Bob, Grace");
        System.out.println("    /dt=2026-07-28/part-000.parquet  <- Charlie, David, Henry");
        System.out.println("    /dt=2026-07-29/part-000.parquet  <- Eve, Frank");
        System.out.println("    分区体现在: 文件路径; 用户必须: 显式写 dt 列, 查询带 WHERE dt='...'");
        System.out.println();
        System.out.println("  Iceberg 隐藏分区:");
        System.out.println("    data/<uuid>.parquet  partition=[20661]  <- Alice, Bob, Grace");
        System.out.println("    data/<uuid>.parquet  partition=[20662]  <- Charlie, David, Henry");
        System.out.println("    data/<uuid>.parquet  partition=[20663]  <- Eve, Frank");
        System.out.println("    分区体现在: DataFile.partition (元数据); 用户无需写分区列");
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
