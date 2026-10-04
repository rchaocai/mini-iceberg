package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.DeleteFile;
import com.iceberglearn.metadata.FileContent;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Advanced writes — delete, overwrite, and row-delta on top of the append-only
 * path built in earlier chapters. Each operation produces a new snapshot whose
 * {@code summary.operation} records what kind of write it was.
 */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    private static final int ID = 1;
    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

    /** Rows written per file in the baseline append. */
    private static final int ROWS_PER_FILE = 500;
    private static final int ROW_GROUP_SIZE = 1024;
    private static final int PAGE_SIZE = 64;

    /** Locate the project root so data lands in the project's data/ directory regardless of cwd. */
    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch15-advanced-write"))) {
            return cwd.resolve("data").resolve("ch15");
        }
        return cwd.getParent().resolve("data").resolve("ch15");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);

        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();

        System.out.println("[步骤1] 建表：events，分区策略 day(event_time)，写 6 个 Parquet 文件");
        Path warehouse = BASE_DIR.resolve("warehouse");
        Files.createDirectories(warehouse);
        TableIdentifier tableId = TableIdentifier.of("events");
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(tableId, schema, spec);
        System.out.println("  " + spec.toString().replace("\n", "\n  "));

        // 3 day partitions × 2 files each: every day has one low-age file (upper bound ≤ 40)
        // and one high-age file (lower bound > 40). This split is what makes strict matching
        // visible in later steps — strict age < 40 selects the low files, strict age > 40
        // selects the high files. All six files go into one append snapshot so the snapshot
        // history stays compact: one append, one delete, one overwrite, one row-delta.
        Path dataDir = Path.of(table.location()).resolve("data");
        List<DataFile> baselineFiles = new ArrayList<>();
        baselineFiles.addAll(writeParquetFile(spec, dataDir, day(20661) + 10 * MICROS_PER_HOUR, 20, 30));
        baselineFiles.addAll(writeParquetFile(spec, dataDir, day(20661) + 11 * MICROS_PER_HOUR, 45, 60));
        baselineFiles.addAll(writeParquetFile(spec, dataDir, day(20662) + 10 * MICROS_PER_HOUR, 25, 35));
        baselineFiles.addAll(writeParquetFile(spec, dataDir, day(20662) + 11 * MICROS_PER_HOUR, 50, 70));
        baselineFiles.addAll(writeParquetFile(spec, dataDir, day(20663) + 10 * MICROS_PER_HOUR, 10, 20));
        baselineFiles.addAll(writeParquetFile(spec, dataDir, day(20663) + 11 * MICROS_PER_HOUR, 55, 80));
        appendFiles(table, baselineFiles);

        Snapshot appendSnapshot = table.currentSnapshot();
        System.out.println("  6 个文件已落盘，append snapshot=" + appendSnapshot.snapshotId()
                + " operation=" + appendSnapshot.operation());

        System.out.println("\n[步骤2] planFiles() 基线：6 个文件，三分区各两个");
        printFiles(table.newScan().planFiles(), "  ");

        // --- Delete ----------------------------------------------------------
        // strict age < 40 selects files whose upper bound < 40 → the three low-age files.
        // After commit, planFiles() returns only the three high-age files. The physical
        // Parquet files stay on disk — deletion is a metadata operation.
        System.out.println("\n[步骤3] newDelete().deleteFromRowFilter(age < 40)：strict 选中 upperBound(age) < 40 的文件");
        Expr deleteFilter = Exprs.lessThan(AGE, 40L);
        table.newDelete().deleteFromRowFilter(deleteFilter).commit();

        Snapshot deleteSnapshot = table.currentSnapshot();
        System.out.println("  delete snapshot=" + deleteSnapshot.snapshotId()
                + " operation=" + deleteSnapshot.operation()
                + " parentId=" + deleteSnapshot.parentId());
        System.out.println("  planFiles() 剩余：");
        printFiles(table.newScan().planFiles(), "    ");

        long filesOnDisk = countParquetFiles(dataDir);
        System.out.println("  物理文件仍在磁盘：data/ 下 " + filesOnDisk + " 个 .parquet（删除只动元数据）");

        // --- Time travel -----------------------------------------------------
        // A scan bound to the pre-delete snapshot still sees all six files — deletion is
        // reversible because the old manifest list is untouched; only the current pointer
        // moved. This is the snapshot-isolation guarantee in action.
        System.out.println("\n[步骤4] 时间旅行：useSnapshot(appendSnapshot) 看删除前的 6 个文件");
        List<DataFile> historical = table.newScan().useSnapshot(appendSnapshot.snapshotId()).planFiles();
        printFiles(historical, "  ");
        System.out.println("  删除可逆：旧 manifest 未被改写，新 manifest 只挂 DELETED 条目");

        // --- Overwrite -------------------------------------------------------
        // strict age > 40 selects files whose lower bound > 40 → the three high-age files.
        // addFile appends one new file. The committed snapshot's operation is overwrite
        // because both sides are present.
        System.out.println("\n[步骤5] newOverwrite().overwriteByRowFilter(age > 40).addFile(newFile)");
        DataFile newFile = writeParquetFile(spec, dataDir,
                day(20663) + 12 * MICROS_PER_HOUR, 60, 75).get(0);
        table.newOverwrite()
                .overwriteByRowFilter(Exprs.greaterThan(AGE, 40L))
                .addFile(newFile)
                .commit();

        Snapshot overwriteSnapshot = table.currentSnapshot();
        System.out.println("  overwrite snapshot=" + overwriteSnapshot.snapshotId()
                + " operation=" + overwriteSnapshot.operation()
                + " parentId=" + overwriteSnapshot.parentId());
        System.out.println("  planFiles() 剩余：");
        printFiles(table.newScan().planFiles(), "    ");

        // --- Row Delta -------------------------------------------------------
        // A delete file does not remove a data file; it encodes rows to drop. The new
        // snapshot carries a DATA manifest (the inserted rows) AND a DELETES manifest
        // (the delete file). planDeleteFiles() returns the delete file for the engine
        // to apply; this implementation does not apply the deletes itself.
        System.out.println("\n[步骤6] newRowDelta().addRows(dataFile).addDeletes(deleteFile)");
        DataFile insertFile = writeParquetFile(spec, dataDir,
                day(20663) + 13 * MICROS_PER_HOUR, 65, 85).get(0);
        // A position-delete marker file: encodes "drop N rows of insertFile by ordinal".
        // The manifest plumbing is the same whether or not the delete file contains
        // actual row positions.
        DeleteFile deleteFile = DeleteFile.builder(
                        "data/delete-" + UUID.randomUUID() + ".pos-delete.parquet",
                        FileContent.POSITION_DELETES)
                .recordCount(5L)
                .referencedDataFile(insertFile.path())
                .partition(insertFile.partition())
                .build();
        table.newRowDelta()
                .addRows(insertFile)
                .addDeletes(deleteFile)
                .commit();

        Snapshot rowDeltaSnapshot = table.currentSnapshot();
        System.out.println("  row-delta snapshot=" + rowDeltaSnapshot.snapshotId()
                + " operation=" + rowDeltaSnapshot.operation()
                + " parentId=" + rowDeltaSnapshot.parentId());

        List<DeleteFile> plannedDeletes = table.newScan().planDeleteFiles();
        System.out.println("  planDeleteFiles() 返回 " + plannedDeletes.size() + " 个 DeleteFile：");
        for (DeleteFile df : plannedDeletes) {
            System.out.println("    " + df.path()
                    + "  content=" + df.content()
                    + "  records=" + df.recordCount()
                    + "  referencedDataFile=" + abbreviatePath(df.referencedDataFile()));
        }

        // --- Snapshot operations ---------------------------------------------
        System.out.println("\n[步骤7] 所有 snapshot 的 operation 字段：append / delete / overwrite 三值各跑一次");
        for (Snapshot s : table.snapshots()) {
            System.out.println("  snapshot=" + s.snapshotId()
                    + "  operation=" + s.operation()
                    + "  sequenceNumber=" + s.sequenceNumber()
                    + "  parentId=" + s.parentId());
        }

        System.out.println("\n[步骤8] 总结");
        System.out.println("  删除是元数据操作：DELETED 条目 + 两遍扫描，物理文件不动；");
        System.out.println("  严格匹配与包含匹配对偶：inclusive 看可能命中→保留，strict 看全部命中→删除；");
        System.out.println("  三种写入共用同一条提交链路：写新 manifest、挂新子树、一次原子提交。");
    }

    // --- helpers ---------------------------------------------------------------

    private static void printFiles(List<DataFile> files, String indent) {
        for (DataFile file : files) {
            System.out.println(indent + abbreviatePath(file.path())
                    + "  partition=" + file.partition()
                    + "  records=" + file.recordCount()
                    + "  age=[" + bound(file, true) + ", " + bound(file, false) + "]");
        }
        System.out.println(indent + "共 " + files.size() + " 个文件");
    }

    private static Object bound(DataFile file, boolean lower) {
        Map<Integer, Object> bounds = lower ? file.lowerBounds() : file.upperBounds();
        return bounds != null && bounds.containsKey(AGE) ? bounds.get(AGE) : "?";
    }

    private static String abbreviatePath(String path) {
        if (path == null) {
            return "<null>";
        }
        int idx = path.lastIndexOf('/');
        return idx >= 0 ? path.substring(idx + 1) : path;
    }

    private static long countParquetFiles(Path dataDir) throws IOException {
        try (var paths = Files.walk(dataDir)) {
            return paths.filter(p -> p.toString().endsWith(".parquet")).count();
        }
    }

    /**
     * Write one file holding {@code ROWS_PER_FILE} rows whose ages interpolate from
     * {@code minAge} to {@code maxAge}. The first row is exactly {@code minAge} (gives the
     * footer's lower bound) and the last is exactly {@code maxAge} (gives the upper bound).
     *
     * <p>Returns the {@link DataFile}s without committing anything — the caller decides
     * whether to commit them via {@code newAppend}, hand them to {@code newOverwrite}, or
     * pass them to {@code newRowDelta} as inserted rows.
     */
    private static List<DataFile> writeParquetFile(PartitionSpec spec, Path dataDir,
                                                   long eventTime, int minAge, int maxAge) {
        PartitionedWriter writer = new PartitionedWriter(
                schema(), spec, dataDir, ROWS_PER_FILE, ROW_GROUP_SIZE, PAGE_SIZE);
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            int age = ROWS_PER_FILE == 1 ? minAge
                    : minAge + (int) ((long) (maxAge - minAge) * i / (ROWS_PER_FILE - 1));
            writer.write(row(i + 1, eventTime, age));
        }
        return writer.complete();
    }

    /** Commit {@code files} as one append snapshot. */
    private static void appendFiles(Table table, List<DataFile> files) {
        AppendFiles append = table.newAppend();
        for (DataFile file : files) {
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
