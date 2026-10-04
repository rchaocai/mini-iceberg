package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.io.ParquetReader;
import com.iceberglearn.io.ParquetSchemaUtil;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.TableMetadata;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Chapter 13: evolve the schema (add / rename / delete columns) with zero data movement. */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    private static final int ID = 1;
    private static final int NAME = 2;
    private static final int AGE = 3;
    private static final int EMAIL = 4;

    /** Locate the project root so data lands in iceberg-java/data/ch13 regardless of cwd. */
    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch13-schema-evolution"))) {
            return cwd.resolve("data").resolve("ch13");
        }
        return cwd.getParent().resolve("data").resolve("ch13");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);

        System.out.println("[步骤1] 建表：schema v0（id, name, age, email）");
        Path warehouse = BASE_DIR.resolve("warehouse");
        Files.createDirectories(warehouse);
        TableIdentifier tableId = TableIdentifier.of("users");
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(tableId, schemaV0(), PartitionSpec.unpartitioned());
        printSchemas("  ", table.metadata());

        System.out.println("\n[步骤2] 按 v0 写第一批 4 行（2 个文件，1 次提交）");
        writeUsers(table, List.of(
                user(1, "alice", 30, "alice@demo.com"),
                user(2, "bob", 25, "bob@demo.com"),
                user(3, "carol", 35, "carol@demo.com"),
                user(4, "dave", 28, "dave@demo.com")));
        Set<String> oldFilePaths = new HashSet<>();
        for (DataFile file : table.newScan().planFiles()) {
            oldFilePaths.add(file.path());
        }
        System.out.println("  老文件已落盘: " + oldFilePaths.size() + " 个");
        String firstFile = oldFilePaths.iterator().next();
        System.out.println("  老文件的 Parquet schema（field-id 已在写入时嵌进文件本体）:");
        printFileSchema(Path.of(table.location()).resolve(firstFile));

        // The pain: Hive's ALTERs are all metadata-only and cheap — add reads NULL, drop
        // masks the column — but a RENAME breaks the by-name alignment between the table
        // definition and the files, so keeping the business correct means a full rewrite.
        System.out.println("\n[步骤3] 痛点：ALTER 都是元数据操作，唯独改名后按名读取失联");
        System.out.println("  Hive: ALTER TABLE users CHANGE COLUMN name username STRING;  -- 一秒完成");
        System.out.println("  老文件里的列叫 name，新表定义里叫 username，按名对不上 —— 查询读出空值。");
        System.out.println("  加列读 NULL、删列屏蔽读取都勉强可用；改名要业务正确只能全表重写。");

        System.out.println("\n[步骤4] 演进一（加列）：addColumn(address) → schema-id 0→1");
        System.out.println("  schema 演进前:");
        printSchemas("    ", table.metadata());
        long snapshotIdBefore = table.currentSnapshot().snapshotId();
        table.updateSchema()
                .addColumn("address", "string")
                .commit();
        table.refresh();
        System.out.println("  schema 演进后:");
        printSchemas("    ", table.metadata());
        System.out.println("  加列的效果: address 拿到全新 field-id=5，老四列 id 原样不动");
        System.out.println("  快照数: " + snapshotCount(table) + "（schema 更新不产生新快照）");
        System.out.println("  当前快照仍是 " + table.currentSnapshot().snapshotId()
                + "（演进前 " + snapshotIdBefore + "）");

        System.out.println("\n[步骤5] 按 v1 写第二批 2 行（新文件带 address 列）");
        Map<Integer, Object> eve = user(5, "eve", 32, "eve@demo.com");
        eve.put(5, "杭州");
        Map<Integer, Object> frank = user(6, "frank", 40, "frank@demo.com");
        frank.put(5, "上海");
        writeUsers(table, List.of(eve, frank));
        readAll("v1", table);

        System.out.println("\n[步骤6] 演进二（改名）：renameColumn(name → username) → schema-id 1→2");
        table.updateSchema()
                .renameColumn("name", "username")
                .commit();
        table.refresh();
        printSchemas("    ", table.metadata());
        System.out.println("  再读全量数据（列名来自当前 schema，取值按 field-id）:");
        readAll("v2", table);
        System.out.println("  老文件里 name 列的 alice/bob/... 原样出现在 username 下 ——");
        System.out.println("  field-id=2 没变，变的只是 schema 里 2 号列的标签。");

        System.out.println("\n[步骤7] 演进三（删列）：deleteColumn(email) + deleteColumn(address) → schema-id 2→3");
        table.updateSchema()
                .deleteColumn("email")
                .deleteColumn("address")
                .commit();
        table.refresh();
        printSchemas("    ", table.metadata());
        System.out.println("  再读全量数据:");
        readAll("v3", table);
        System.out.println("  email/address 从读取结果消失；老文件里那两列的字节还在磁盘上，");
        System.out.println("  只是当前 schema 里没有它们的 field-id，读取根本不会去取。");

        System.out.println("\n[步骤8] 演进四（再加列）：id 只发新号，绝不回收");
        int preAddHighest = table.schema().highestFieldId();
        System.out.println("  当前 schema 的 highestFieldId = " + preAddHighest
                + "（删列后会下降，不能当分配基准）");
        System.out.println("  表级 lastColumnId = " + table.metadata().lastColumnId()
                + "（全历史 max，删除不影响它）");
        table.updateSchema()
                .addColumn("phone", "string")
                .commit();
        table.refresh();
        printSchemas("    ", table.metadata());
        System.out.println("  phone 拿到 field-id=" + table.schema().findField("phone").id());
        System.out.println("  反例: 若按 per-schema highestFieldId+1 分配会拿 " + (preAddHighest + 1)
                + " —— 复用已删 email 的 id，老文件 email 的字节会被当成 phone 读出来。");

        System.out.println("\n[步骤9] 验证：老数据零移动");
        int moved = 0;
        for (String path : oldFilePaths) {
            if (!Files.exists(Path.of(table.location()).resolve(path))) {
                moved++;
            }
        }
        System.out.println("  演进前老文件: " + oldFilePaths.size() + " 个，全部仍在原路径: "
                + (moved == 0 ? "是" : "否"));
        System.out.println("  重写/移动的旧文件数: " + moved);
        System.out.println("  schemas 共 " + table.metadata().schemas().size()
                + " 份声明（v0 + 加列 + 改名 + 删列 + 再加列），数据文件一个没动。");

        System.out.println("\n[步骤10] 总结：schema 演进三要素");
        System.out.println("  1. schema-id 版本化历史：schemas 列表追加不覆盖，currentSchemaId 指针切换。");
        System.out.println("  2. field-id 是列的永久身份证：新列拿全新 id，幸存列保留原 id，跨 schema 绝不复用。");
        System.out.println("  3. 文件自描述：field-id 写入时嵌进 Parquet schema，读取按 id 配对，名字只是标签。");
        System.out.println("  改名是改映射，删列是停止引用，加列是补一个 null —— 都不需要碰数据。");
    }

    // --- helpers ---------------------------------------------------------------

    /** Print the schemas list plus the current pointer — the before/after view of evolution. */
    private static void printSchemas(String indent, TableMetadata metadata) {
        for (Schema schema : metadata.schemas()) {
            String fields = schema.fields().stream()
                    .map(f -> f.name() + "(" + f.id() + ")")
                    .collect(Collectors.joining(", "));
            System.out.println(indent + "schema-id=" + schema.schemaId() + ": " + fields);
        }
        System.out.println(indent + "current-schema-id: " + metadata.currentSchemaId());
    }

    /** Print a file's embedded field ids, read back from the Parquet footer. */
    private static void printFileSchema(Path file) throws IOException {
        String names = ParquetSchemaUtil.fieldIdToName(file).entrySet().stream()
                .map(e -> e.getValue() + "=" + e.getKey())
                .collect(Collectors.joining(", "));
        System.out.println("    footer 读回 列名=field-id: " + names);
    }

    /** Read every live file under the current schema and print the projected rows. */
    private static void readAll(String label, Table table) {
        Schema readSchema = table.schema();
        List<Map<Integer, Object>> rows = new ArrayList<>();
        for (DataFile file : table.newScan().planFiles()) {
            rows.addAll(ParquetReader.read(readSchema,
                    Path.of(table.location()).resolve(file.path())));
        }
        String header = readSchema.fields().stream()
                .map(Schema.NestedField::name)
                .collect(Collectors.joining(" | "));
        System.out.println("  读全量数据（" + label + "，共 " + rows.size() + " 行）:");
        System.out.println("    " + header);
        for (Map<Integer, Object> row : rows) {
            String line = readSchema.fields().stream()
                    .map(f -> String.valueOf(row.get(f.id())))
                    .collect(Collectors.joining(" | "));
            System.out.println("    " + line);
        }
    }

    private static void writeUsers(Table table, List<Map<Integer, Object>> rows) {
        PartitionedWriter writer = new PartitionedWriter(
                table.schema(), table.spec(), Path.of(table.location()).resolve("data"), 2);
        rows.forEach(writer::write);
        AppendFiles append = table.newAppend();
        writer.complete().forEach(append::appendFile);
        append.commit();
    }

    private static Schema schemaV0() {
        return new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(NAME, "name", "string"),
                Schema.NestedField.required(AGE, "age", "long"),
                Schema.NestedField.required(EMAIL, "email", "string")));
    }

    /** Keys are field ids, so a column the current schema lacks is simply absent. */
    private static Map<Integer, Object> user(long id, String name, long age, String email) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(ID, id);
        row.put(NAME, name);
        row.put(AGE, age);
        row.put(EMAIL, email);
        return row;
    }

    private static int snapshotCount(Table table) {
        int count = 0;
        for (var ignored : table.snapshots()) {
            count++;
        }
        return count;
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
