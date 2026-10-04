package com.iceberglearn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.io.UnpartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.scan.TableScan;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;



/** Chapter 7: select a historical snapshot by ID or timestamp. */
public class Main {
    private static final String WAREHOUSE = "../data/ch07/warehouse";

    public static void main(String[] args) throws Exception {
        Path warehouse = Path.of(WAREHOUSE).toAbsolutePath().normalize();
        cleanup(warehouse);

        FileSystemCatalog catalog = new FileSystemCatalog(WAREHOUSE);
        TableIdentifier identifier = TableIdentifier.of("analytics", "users");
        Table table = catalog.createTable(identifier, Schema.userSchema());

        System.out.println("[步骤1] 连续提交三个 Snapshot");
        append(table, writeDataFile(Schema.userSchema(),
                Path.of(table.location()).resolve("data"), 0));
        long firstSnapshotId = table.currentSnapshot().snapshotId();
        long firstTime = table.currentSnapshot().timestampMillis();
        waitUntilAfter(firstTime);

        append(table, writeDataFile(Schema.userSchema(),
                Path.of(table.location()).resolve("data"), 1));
        long secondSnapshotId = table.currentSnapshot().snapshotId();
        long secondTime = table.currentSnapshot().timestampMillis();
        waitUntilAfter(secondTime);

        append(table, writeDataFile(Schema.userSchema(),
                Path.of(table.location()).resolve("data"), 2));
        long thirdTime = table.currentSnapshot().timestampMillis();

        System.out.println("  snapshot " + firstSnapshotId + " became current at " + firstTime);
        System.out.println("  snapshot " + secondSnapshotId + " became current at " + secondTime);
        System.out.println("  snapshot " + table.currentSnapshot().snapshotId() + " became current at " + thirdTime);

        table.rollbackTo(firstSnapshotId);
        long rollbackTime = table.history().get(table.history().size() - 1).timestampMillis();
        System.out.println("  rollback at " + rollbackTime + " → snapshot " + firstSnapshotId);

        Table reader = catalog.loadTable(identifier);
        System.out.println("\n[步骤2] 回滚后默认扫描读取当前 Snapshot");
        printScan("newScan()", reader.newScan());

        System.out.println("\n[步骤3] useSnapshot 直接选择第二个 Snapshot");
        printScan("useSnapshot(second)", reader.newScan().useSnapshot(secondSnapshotId));

        System.out.println("\n[步骤4] asOfTime 选择当时的当前 Snapshot");
        printScan("asOfTime(firstTime)", reader.newScan().asOfTime(firstTime));
        printScan("asOfTime(secondTime)", reader.newScan().asOfTime(secondTime));
        printScan("asOfTime(rollbackTime)", reader.newScan().asOfTime(rollbackTime));

        System.out.println("\n结果：时间旅行没有另一条读取路径；它只改变扫描起点，"
                + "后面仍沿 Snapshot → manifest-list → manifest → DataFile 读取。");
    }

    private static void append(Table table, DataFile file) {
        table.newAppend().appendFile(file).commit();
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

    private static void printScan(String label, TableScan scan) {
        List<String> paths = scan.planFiles().stream().map(DataFile::path).toList();
        System.out.println("  " + label + " → snapshot " + scan.snapshot().snapshotId());
        System.out.println("    files = " + paths);
    }

    private static void waitUntilAfter(long timestampMillis) {
        while (System.currentTimeMillis() <= timestampMillis) {
            Thread.onSpinWait();
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
