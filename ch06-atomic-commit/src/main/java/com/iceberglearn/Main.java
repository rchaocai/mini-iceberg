package com.iceberglearn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.exceptions.SimulatedCrashException;
import com.iceberglearn.io.UnpartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.operations.FailOnceBeforeRenameTableOperations;
import com.iceberglearn.operations.FileSystemTableOperations;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;



/** Chapter 6: commit a new table version with one atomic rename. */
public class Main {
    private static final String WAREHOUSE = "../data/ch06/warehouse";

    public static void main(String[] args) throws Exception {
        Path warehouse = Path.of(WAREHOUSE).toAbsolutePath().normalize();
        cleanup(warehouse);

        FileSystemCatalog catalog = new FileSystemCatalog(WAREHOUSE);
        TableIdentifier identifier = TableIdentifier.of("analytics", "users");
        Table createdTable = catalog.createTable(identifier, Schema.userSchema());
        FileSystemTableOperations operations = new FailOnceBeforeRenameTableOperations(
                Path.of(createdTable.location()));
        Table table = new Table(identifier, operations);

        System.out.println("[步骤1] 建表后是一张空表");
        printVisibleState(catalog, identifier);

        System.out.println("\n[步骤2] 写出一个 Parquet 文件");
        DataFile dataFile = writeDataFile(Schema.userSchema(),
                Path.of(table.location()).resolve("data"), 0);
        System.out.println("  data file = " + dataFile.path());
        System.out.println("  文件已经落盘，但还没有加入表");
        printVisibleState(catalog, identifier);

        System.out.println("\n[步骤3] 通过 AppendFiles 提交，并在 atomic rename 前模拟崩溃");
        AppendFiles append = table.newAppend().appendFile(dataFile);
        try {
            append.commit();
        } catch (SimulatedCrashException e) {
            System.out.println("  caught = " + e.getMessage());
        }
        printVisibleState(catalog, identifier);

        System.out.println("\n[步骤4] 重试同一个 AppendFiles");
        append.commit();
        long firstSnapshotId = table.currentSnapshot().snapshotId();
        printVisibleState(catalog, identifier);

        System.out.println("\n[步骤5] 删除 version-hint，仍能找到已提交版本");
        Files.deleteIfExists(operations.versionHintFile());
        printVisibleState(catalog, identifier);

        System.out.println("\n[步骤6] 再提交一个数据文件");
        DataFile secondDataFile = writeDataFile(Schema.userSchema(),
                Path.of(table.location()).resolve("data"), 1);
        table.newAppend().appendFile(secondDataFile).commit();
        printVisibleState(catalog, identifier);

        System.out.println("\n[步骤7] rollbackTo 发布新的 metadata 版本");
        table.rollbackTo(firstSnapshotId);
        printVisibleState(catalog, identifier);

        System.out.println("\nmetadata 目录:");
        try (var files = Files.list(Path.of(table.location()).resolve("metadata"))) {
            files.map(path -> path.getFileName().toString())
                    .sorted()
                    .forEach(file -> System.out.println("  " + file));
        }

        System.out.println("\n结果：append 和 rollback 都先构造完整的 TableMetadata，"
                + "最后通过一次 atomic rename 生效。");
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

    private static void printVisibleState(FileSystemCatalog catalog, TableIdentifier identifier) {
        Table reader = catalog.loadTable(identifier);
        Snapshot snapshot = reader.currentSnapshot();
        System.out.println("  current metadata = "
                + Path.of(reader.metadataFileLocation()).getFileName());
        System.out.println("  current snapshot = "
                + (snapshot == null ? "null" : snapshot.snapshotId()));
        System.out.println("  visible data files = " + reader.newScan().planFiles().size());
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
