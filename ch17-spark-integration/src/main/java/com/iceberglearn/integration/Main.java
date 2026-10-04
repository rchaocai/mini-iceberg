package com.iceberglearn.integration;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.scan.CombinedScanTask;
import com.iceberglearn.scan.FileScanTask;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.catalyst.expressions.Attribute;
import com.sparklearn.sql.catalyst.expressions.GreaterThan;
import com.sparklearn.sql.catalyst.expressions.Literal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Chapter 17 demo — connecting mini-Iceberg to mini-Spark through the
 * IcebergScanRDD adapter. The storage side (IcebergScanSource) and the
 * engine side (SparkContext + SQLContext + DataFrame) are independent
 * projects; the 100-line IcebergScanRDD is the only bridge between them.
 *
 * <p>Four steps demonstrate the contract:
 * <ol>
 *   <li>Step 0: initialise the table and write 3 batches of data</li>
 *   <li>Step 1: full table scan via IcebergScanRDD (300 rows)</li>
 *   <li>Step 2: predicate pushdown — storage-side file pruning + engine-side row filtering</li>
 *   <li>Step 3: DataFrame aggregation (groupBy city, count)</li>
 *   <li>Step 4: collect records (age > 70)</li>
 * </ol>
 */
public class Main {

    private static final int ID = 1;
    private static final int NAME = 2;
    private static final int AGE = 3;
    private static final int CITY = 4;

    private static final int ROWS = 100;
    private static final int ROW_GROUP_SIZE = 1024;
    private static final int PAGE_SIZE = 64;

    /** Three batches: lowercase key, display city, min age, max age. */
    private static final String[] BATCH_KEYS  = {"beijing", "shanghai", "shenzhen"};
    private static final String[] BATCH_CITIES = {"Beijing", "Shanghai", "Shenzhen"};
    private static final int[]    BATCH_MIN_AGE = {20, 40, 50};
    private static final int[]    BATCH_MAX_AGE = {40, 60, 80};

    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch17-spark-integration"))) {
            return cwd.resolve("data").resolve("ch17");
        }
        return cwd.getParent().resolve("data").resolve("ch17");
    }

    public static void main(String[] args) throws IOException {
        Path baseDir = resolveBaseDir();
        cleanup(baseDir);

        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.unpartitioned();

        // -------------------- Step 0: init table and write 3 batches --------------------
        System.out.println("[Step 0] 初始化表并写入数据");
        Path warehouse = baseDir.resolve("warehouse");
        Files.createDirectories(warehouse);
        TableIdentifier tableId = TableIdentifier.of("orders");
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(tableId, schema, spec);

        Path dataDir = Path.of(table.location()).resolve("data");
        for (int batch = 0; batch < BATCH_KEYS.length; batch++) {
            List<DataFile> files = writeBatch(dataDir, schema, spec,
                    batch * 100L + 1, BATCH_CITIES[batch],
                    BATCH_MIN_AGE[batch], BATCH_MAX_AGE[batch]);
            appendFiles(table, files);
            Snapshot snap = table.currentSnapshot();
            System.out.printf("  写入 %s: %d 行 (age %d-%d) -> snapshot %d%n",
                    BATCH_KEYS[batch], ROWS,
                    BATCH_MIN_AGE[batch], BATCH_MAX_AGE[batch],
                    snap.snapshotId());
        }
        List<DataFile> allFiles = table.newScan().planFiles();
        long totalRows = allFiles.stream().mapToLong(DataFile::recordCount).sum();
        System.out.printf("  表初始化完成: %d 个文件, %d 行数据%n",
                allFiles.size(), totalRows);

        // -------------------- mini-Spark engine steps --------------------
        try (SparkContext sparkContext = new SparkContext(2)) {
            SQLContext sqlContext = new SQLContext(sparkContext);
            IcebergScanSource source = new IcebergScanSource(table);

            // -------------------- Step 1: full table scan --------------------
            System.out.println("\n[Step 1] mini-Spark 通过 IcebergScanRDD 读 mini-Iceberg 表");
            System.out.println("  存储侧: IcebergScanSource (表格式的契约入口)");
            System.out.println("  桥接层: IcebergScanRDD (ScanTask -> mini-Spark Partition)");
            System.out.println("  引擎侧: SparkContext + SQLContext + DataFrame");
            System.out.println("  查询: SELECT COUNT(*) FROM orders");
            IcebergScanRDD rdd = new IcebergScanRDD(sparkContext, source);
            DataFrame orders = sqlContext.createDataFrame(
                    "orders", rdd, source.sparkSchema());
            long count = orders.count();
            System.out.printf("  结果: %d 行%n", count);
            System.out.printf("  验证: 总行数 %d == %d %s%n",
                    count, totalRows, count == totalRows ? "OK" : "FAIL");

            // -------------------- Step 2: predicate pushdown --------------------
            System.out.println("\n[Step 2] 谓词下推: DataFrame.filter() 自动推入存储侧");
            System.out.println("  查询: SELECT COUNT(*) FROM orders WHERE age > 50");

            // 展示文件级裁剪效果（教学对比，不影响实际查询）
            System.out.println("  (1) 存储侧文件级裁剪 (metrics-based pruning):");
            Expr icebergFilter = Exprs.greaterThan(AGE, 50);
            List<CombinedScanTask> fullTasks = source.scan(null);
            List<CombinedScanTask> prunedTasks = source.scan(icebergFilter);
            Set<String> keptPaths = new HashSet<>();
            for (CombinedScanTask task : prunedTasks) {
                for (FileScanTask ft : task.files()) {
                    keptPaths.add(ft.file().path());
                }
            }
            System.out.println("  文件级 metrics 分析:");
            for (DataFile df : allFiles) {
                Object lo = df.lowerBounds() != null ? df.lowerBounds().get(AGE) : "?";
                Object hi = df.upperBounds() != null ? df.upperBounds().get(AGE) : "?";
                boolean kept = keptPaths.contains(df.path());
                System.out.printf("    %s: age[%s..%s] -> %s%n",
                        Path.of(df.path()).getFileName(), lo, hi,
                        kept ? "保留" : "跳过 (metrics 裁剪)");
            }
            System.out.printf("  ScanTask: %d -> %d (裁剪掉 %d 个)%n",
                    fullTasks.size(), prunedTasks.size(),
                    fullTasks.size() - prunedTasks.size());

            // (2) 实际查询：DataFrame.filter() 自动下推，不手动传 filter
            System.out.println("  (2) 引擎侧自动下推 + 行级过滤:");
            IcebergScanRDD prunedRDD = new IcebergScanRDD(sparkContext, source);
            DataFrame prunedOrders = sqlContext.createDataFrame(
                    "orders_filtered", prunedRDD, source.sparkSchema());
            DataFrame filtered = prunedOrders
                    .filter(new GreaterThan(new Attribute("age"), new Literal(50)));
            // 打印优化前后的逻辑计划，验证 PushFilterIntoScan 的效果
            System.out.println("  优化前后计划对比:");
            System.out.println(filtered.explainString());
            long exactCount = filtered.count();
            System.out.printf("  引擎结果: %d 行 (age > 50)%n", exactCount);
            System.out.printf("  验证: 实际 age > 50 的行数 = %d %s%n",
                    exactCount, exactCount > 0 ? "OK" : "FAIL");

            // -------------------- Step 3: groupBy aggregation --------------------
            System.out.println("\n[Step 3] mini-Spark 的 DataFrame 聚合");
            System.out.println("  查询: SELECT city, COUNT(*) FROM orders GROUP BY city");
            IcebergScanRDD aggRDD = new IcebergScanRDD(sparkContext, source);
            DataFrame aggOrders = sqlContext.createDataFrame(
                    "orders_agg", aggRDD, source.sparkSchema());
            List<Row> cityCounts = aggOrders.groupBy("city").count().collect();
            for (Row row : cityCounts) {
                System.out.println("  " + row);
            }

            // -------------------- Step 4: collect records --------------------
            System.out.println("\n[Step 4] 收集记录: 引擎看到的是 Row");
            System.out.println("  查询: SELECT * FROM orders WHERE age > 70");
            IcebergScanRDD collectRDD = new IcebergScanRDD(sparkContext, source);
            DataFrame collectDF = sqlContext.createDataFrame(
                    "orders_gt70", collectRDD, source.sparkSchema());
            List<Row> records = collectDF
                    .filter(new GreaterThan(new Attribute("age"), new Literal(70)))
                    .collect();
            System.out.printf("  收集到 %d 条记录 (age > 70):%n", records.size());
            for (Row row : records) {
                System.out.printf("    id=%s, name=%s, age=%s, city=%s%n",
                        row.get(0), row.get(1), row.get(2), row.get(3));
            }

            // -------------------- Summary -----------------------------------
            System.out.println("\n[Summary] 两个独立项目通过 100 行适配器对接:");
            System.out.println("  - 存储侧 (mini-Iceberg): 元数据树 + 快照 + scan planning");
            System.out.println("  - 桥接层 (IcebergScanRDD): ScanTask -> Partition, Parquet -> Row");
            System.out.println("  - 引擎侧 (mini-Spark): RDD + DAGScheduler + DataFrame + Catalyst");
            System.out.println("  引擎不认识文件, 只认识表格式 -- 这就是湖仓一体的本质。");
        }
    }

    // --- helpers ---------------------------------------------------------------

    private static List<DataFile> writeBatch(Path dataDir, Schema schema, PartitionSpec spec,
                                             long idStart, String city, int minAge, int maxAge) {
        PartitionedWriter writer = new PartitionedWriter(
                schema, spec, dataDir, ROWS, ROW_GROUP_SIZE, PAGE_SIZE);
        for (int i = 0; i < ROWS; i++) {
            int age = minAge + (int) ((long) (maxAge - minAge) * i / (ROWS - 1));
            writer.write(row(idStart + i, city, age));
        }
        return writer.complete();
    }

    private static void appendFiles(Table table, List<DataFile> files) {
        AppendFiles append = table.newAppend();
        for (DataFile file : files) {
            append.appendFile(file);
        }
        append.commit();
    }

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(NAME, "name", "string"),
                Schema.NestedField.required(AGE, "age", "int"),
                Schema.NestedField.required(CITY, "city", "string")));
    }

    private static Map<Integer, Object> row(long id, String city, int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(ID, id);
        row.put(NAME, "user_" + id);
        row.put(AGE, age);
        row.put(CITY, city);
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
