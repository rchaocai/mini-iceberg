package com.iceberglearn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.io.UnpartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

/** Chapter 5: each scan binds to one immutable snapshot — readers never see mid-commit state. */
public class Main {
    private static final String WAREHOUSE = "../data/ch05/warehouse";

    public static void main(String[] args) throws Exception {
        Path warehouse = Path.of(WAREHOUSE).toAbsolutePath().normalize();
        cleanup(warehouse);

        FileSystemCatalog catalog = new FileSystemCatalog(WAREHOUSE);
        TableIdentifier identifier = TableIdentifier.of("analytics", "users");
        Table table = catalog.createTable(identifier, Schema.userSchema());

        System.out.println("[步骤1] 连续 append 两批数据，生成 snapshot 1 和 2");
        appendBatch(table, 0, 2);
        long firstSnapshotId = table.currentSnapshot().snapshotId();
        System.out.println("  snapshot 1 → 2 files (id=" + firstSnapshotId + ")");
        appendBatch(table, 2, 1);
        long secondSnapshotId = table.currentSnapshot().snapshotId();
        System.out.println("  snapshot 2 → 3 files (id=" + secondSnapshotId + ")");

        System.out.println("\n[步骤2] 默认扫描读取当前快照 (snapshot 2)");
        printScan("newScan()", table.newScan());

        System.out.println("\n[步骤3] useSnapshot(1) 绑定历史快照");
        printScan("useSnapshot(" + firstSnapshotId + ")", table.newScan().useSnapshot(firstSnapshotId));

        System.out.println("\n[步骤4] 查询进行期间，Writer 发布 snapshot 3（模拟并发）");
        Table readerTable = catalog.loadTable(identifier);
        long readSnapshotId = readerTable.currentSnapshot().snapshotId();
        System.out.println("  Reader 绑定 snapshot " + readSnapshotId);

        List<DataFile> plannedAtStart = readerTable.newScan().useSnapshot(readSnapshotId).planFiles();
        System.out.println("  Reader 开始时规划 " + plannedAtStart.size() + " 个文件");

        ExecutorService writerExecutor = Executors.newSingleThreadExecutor();
        try {
            writerExecutor.submit(() -> {
                try {
                    Table writerTable = catalog.loadTable(identifier);
                    appendBatch(writerTable, 3, 1);
                    System.out.println("  Writer 发布了 snapshot 3");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).get();
        } finally {
            writerExecutor.shutdown();
        }

        readerTable.refresh();
        System.out.println("  --- 提交完成后验证 ---");
        System.out.println("  Reader 表的当前快照 = " + readerTable.currentSnapshot().snapshotId());
        System.out.println("  但 Reader 查询绑定的快照仍 = " + readSnapshotId);
        List<DataFile> plannedAfterCommit = readerTable.newScan()
                .useSnapshot(readSnapshotId).planFiles();
        System.out.println("  Reader 重新规划仍是 " + plannedAfterCommit.size() + " 个文件 — 读隔离");

        System.out.println("\n[步骤5] 新查询读取新的当前快照 (snapshot 3)");
        Table latestTable = catalog.loadTable(identifier);
        printScan("newScan()", latestTable.newScan());

        System.out.println("\n结果：一次查询只认一个 snapshot。"
                + "写者发布新版本，旧查询仍沿着旧引用树读取。");
    }

    private static void appendBatch(Table table, int fileIndex, int fileCount) {
        var append = table.newAppend();
        for (int i = 0; i < fileCount; i++) {
            DataFile file = writeDataFile(Schema.userSchema(),
                    Path.of(table.location()).resolve("data"), fileIndex + i);
            append.appendFile(file);
        }
        append.commit();
    }

    private static DataFile writeDataFile(Schema schema, Path dataDir, int fileIndex) {
        UnpartitionedWriter writer = new UnpartitionedWriter(schema, dataDir, 100);
        for (int row = 0; row < 100; row++) {
            long id = fileIndex * 100L + row + 1;
            Map<Integer, Object> r = new LinkedHashMap<>();
            r.put(1, id);
            r.put(2, "user_" + id);
            r.put(3, 20 + (int) (id % 40));
            r.put(4, "user" + id + "@example.com");
            writer.write(r);
        }
        return writer.complete().get(0);
    }

    private static void printScan(String label, com.iceberglearn.scan.TableScan scan) {
        List<DataFile> files = scan.planFiles();
        System.out.println("  " + label + " → snapshot " + scan.snapshot().snapshotId()
                + ", files = " + files.size());
        for (DataFile file : files) {
            System.out.println("    " + file.path() + " (" + file.recordCount() + " rows)");
        }
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
