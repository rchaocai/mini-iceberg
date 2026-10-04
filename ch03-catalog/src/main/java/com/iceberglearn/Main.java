package com.iceberglearn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import com.iceberglearn.catalog.Catalog;
import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

/** Chapter 3: resolve a logical table name to the current metadata tree. */
public class Main {
    private static final String WAREHOUSE = "../data/ch03/warehouse";

    public static void main(String[] args) throws Exception {
        Path warehouse = Paths.get(WAREHOUSE).toAbsolutePath().normalize();
        cleanup(warehouse);

        Catalog catalog = new FileSystemCatalog(WAREHOUSE);
        TableIdentifier identifier = TableIdentifier.of("analytics", "events");

        System.out.println("[步骤1] 用逻辑名字创建一张空表");
        Table created = catalog.createTable(identifier, Schema.userSchema());
        System.out.println("  table = " + created.name());
        System.out.println("  location = " + created.location());
        System.out.println("  metadata = " + created.metadataFileLocation());
        System.out.println("  current snapshot = " + created.metadata().currentSnapshotId());

        System.out.println("\n[步骤2] 只凭表名重新加载");
        Table loaded = catalog.loadTable(TableIdentifier.parse("analytics.events"));
        System.out.println("  schema = " + loaded.schema());
        System.out.println("  table uuid = " + loaded.metadata().tableUuid());

        System.out.println("\n[步骤3] 模拟 version-hint 落后");
        Path metadataDir = Paths.get(loaded.location()).resolve("metadata");
        Files.copy(
                metadataDir.resolve("v1.metadata.json"),
                metadataDir.resolve("v2.metadata.json"),
                StandardCopyOption.COPY_ATTRIBUTES);
        Files.writeString(metadataDir.resolve("version-hint.text"), "1");
        Table refreshed = catalog.loadTable(identifier);
        System.out.println("  hint = 1");
        System.out.println("  实际加载 = " + Paths.get(refreshed.metadataFileLocation()).getFileName());

        System.out.println("\n[步骤4] 删除 hint 后仍能恢复");
        Files.delete(metadataDir.resolve("version-hint.text"));
        Table recovered = catalog.loadTable(identifier);
        System.out.println("  扫描恢复 = " + Paths.get(recovered.metadataFileLocation()).getFileName());

        System.out.println("\n[步骤5] 普通目录不等于一张表");
        Files.createDirectories(warehouse.resolve("analytics/not-a-table"));
        System.out.println("  analytics.not-a-table exists = "
                + catalog.tableExists(TableIdentifier.of("analytics", "not-a-table")));

        System.out.println("\n读取入口:");
        System.out.println("  analytics.events");
        System.out.println("    → " + recovered.location());
        System.out.println("    → " + recovered.metadataFileLocation());
        System.out.println("    → current snapshot = " + recovered.metadata().currentSnapshotId());
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
