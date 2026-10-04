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
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

/** Chapter 4: the write path — how rows become visible data through 5 stages. */
public class Main {
    private static final String WAREHOUSE = "../data/ch04/warehouse";

    public static void main(String[] args) throws Exception {
        Path warehouse = Path.of(WAREHOUSE).toAbsolutePath().normalize();
        cleanup(warehouse);

        FileSystemCatalog catalog = new FileSystemCatalog(WAREHOUSE);
        TableIdentifier identifier = TableIdentifier.of("analytics", "users");
        Table table = catalog.createTable(identifier, Schema.userSchema());

        System.out.println("[步骤1] 建表后是一张空表");
        printVisibleState(table);

        System.out.println("\n[步骤2] 写出 Parquet 数据文件（落盘但不可见）");
        DataFile dataFile = writeDataFile(Schema.userSchema(),
                Path.of(table.location()).resolve("data"), 0);
        System.out.println("  data file = " + dataFile.path());
        System.out.println("  文件已落盘，但 currentSnapshotId = null，查询看不到它");
        printVisibleState(table);

        System.out.println("\n[步骤3] AppendFiles.commit() — 走完写入路径 5 阶段");
        System.out.println("  阶段1 (已完成): 写 data files → Parquet 落盘");
        System.out.println("  阶段2-4: 写 manifest → 写 manifest-list → 创建 snapshot");
        System.out.println("  阶段5: commit metadata.json");
        table.newAppend().appendFile(dataFile).commit();
        printVisibleState(table);

        System.out.println("\n[步骤4] 第二批 append — manifest 复用");
        DataFile secondFile = writeDataFile(Schema.userSchema(),
                Path.of(table.location()).resolve("data"), 1);
        table.newAppend().appendFile(secondFile).commit();
        printVisibleState(table);

        System.out.println("\n[步骤5] 快照链");
        for (Snapshot snapshot : table.metadata().snapshots()) {
            Long parent = snapshot.parentId();
            System.out.printf("  snapshot-%d (parent=%s, operation=%s)%n",
                    snapshot.snapshotId(), parent, snapshot.operation());
        }

        System.out.println("\n[步骤6] 写入路径产出的文件");
        System.out.println("  data/ 目录:");
        listFiles(Path.of(table.location()).resolve("data"));
        System.out.println("  metadata/ 目录:");
        listFiles(Path.of(table.location()).resolve("metadata"));

        System.out.println("\n结果：写入 = 写 data files + 挂新 manifest 子树 + 换 metadata 指针。");
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

    private static void printVisibleState(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        System.out.println("  current metadata = "
                + Path.of(table.metadataFileLocation()).getFileName());
        System.out.println("  current snapshot = "
                + (snapshot == null ? "null" : snapshot.snapshotId()));
    }

    private static void listFiles(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            System.out.println("    (目录不存在)");
            return;
        }
        try (var files = Files.list(dir)) {
            files.map(p -> "    " + p.getFileName())
                    .sorted()
                    .forEach(System.out::println);
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
