package com.iceberglearn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

/**
 * 第一章演示入口：从一堆 Parquet 文件说起
 * 
 * 演示数据湖的三个痛点：
 * 1. 部分写入提前可见：写入一半失败，已写文件仍在表目录中
 * 2. 无表级 schema：每个文件都有 schema，但缺少统一的演进规则
 * 3. 并发更新丢失：文件名不冲突，表的当前文件列表仍会被覆盖
 */
public class Main {

    private static final String DATA_DIR = "./data/ch01";

    public static void main(String[] args) throws Exception {
        System.out.println("=== 第1章：从一堆 Parquet 文件说起 ===");
        System.out.println();

        // 清理旧数据
        cleanup();

        // 演示1：部分写入提前可见
        demoPartialWrite();
        System.out.println();

        // 演示2：文件 Schema 不等于表 Schema
        demoMissingTableSchema();
        System.out.println();

        // 演示3：并发提交丢失更新
        demoLostUpdate();
        System.out.println();

        System.out.println("=== 痛点总结 ===");
        System.out.println("1. 部分写入提前可见：失败的写入没有统一发布边界");
        System.out.println("2. 无表级 schema：文件能各自描述自己，却没人定义整张表的 schema");
        System.out.println("3. 无并发提交：文件名不冲突，多个写者仍可能覆盖彼此的表状态");
        System.out.println();
        System.out.println("→ 这就是为什么需要「表格式」来管理这些文件！");
    }

    /**
     * 演示1：部分写入提前可见
     * 模拟写入过程中抛出异常，已写文件仍被目录扫描看到
     */
    private static void demoPartialWrite() throws IOException {
        System.out.println("[演示1] 部分写入提前可见");
        System.out.println("--------");

        Path dir = Paths.get(DATA_DIR, "partial-write");
        Files.createDirectories(dir);

        // 模拟一批数据，计划写3个文件
        List<User> users1 = List.of(
                new User(1, "Alice", 28, "alice@example.com"),
                new User(2, "Bob", 32, "bob@example.com")
        );
        List<User> users2 = List.of(
                new User(3, "Charlie", 35, "charlie@example.com"),
                new User(4, "David", 29, "david@example.com")
        );
        List<User> users3 = List.of(
                new User(5, "Eve", 26, "eve@example.com"),
                new User(6, "Frank", 31, "frank@example.com")
        );
        int expectedRecords = users1.size() + users2.size() + users3.size();

        try {
            // 写入第一个文件 - 成功
            String file1 = dir.resolve("data_0.parquet").toString();
            ParquetFileWriter.writeUsers(users1, file1);
            System.out.println("✓ 写入 data_0.parquet (2条记录)");

            // 写入第二个文件 - 成功
            String file2 = dir.resolve("data_1.parquet").toString();
            ParquetFileWriter.writeUsers(users2, file2);
            System.out.println("✓ 写入 data_1.parquet (2条记录)");

            // 写入第三个文件 - 模拟失败
            System.out.println("✗ 写入 data_2.parquet 时抛出异常！");
            throw new RuntimeException("网络中断，写入失败！");

        } catch (RuntimeException e) {
            System.out.println("  异常：" + e.getMessage());
        }

        // 验证：已写入的文件仍然存在，数据不完整
        System.out.println();
        System.out.println("验证结果：");
        List<String> files = listParquetFiles(dir);
        System.out.println("  已存在的文件：" + files);

        int totalRecords = 0;
        for (String file : files) {
            List<User> users = ParquetFileReader.readUsers(dir.resolve(file).toString());
            totalRecords += users.size();
            System.out.println("  - " + file + ": " + users.size() + " 条记录");
        }
        System.out.println("  总记录数：" + totalRecords + "（预期 " + expectedRecords + " 条）");
        System.out.println("  ⚠️ 问题：失败写入没有统一的发布边界，已写文件被目录扫描提前看见。");
    }

    /**
     * 演示2：文件 schema 不等于表 schema
     */
    private static void demoMissingTableSchema() throws IOException {
        System.out.println("[演示2] 文件 Schema 不等于表 Schema");
        System.out.println("------------------------------");

        Path dir = Paths.get(DATA_DIR, "no-schema");
        Files.createDirectories(dir);

        // 用 schema v1 写入第一个文件（4 个字段：id, name, age, email）
        List<User> users1 = List.of(
                new User(1, "Alice", 28, "alice@example.com"),
                new User(2, "Bob", 32, "bob@example.com")
        );
        String file1 = dir.resolve("data_v1.parquet").toString();
        ParquetFileWriter.writeUsers(users1, file1);
        System.out.println("✓ 用 schema v1 写入 data_v1.parquet (4 字段: id, name, age, email)");

        // 用 schema v2 写入第二个文件（5 个字段：多了 address）
        List<User> users2 = List.of(
                new User(3, "Charlie", 35, "charlie@example.com"),
                new User(4, "David", 29, "david@example.com")
        );
        String file2 = dir.resolve("data_v2.parquet").toString();
        ParquetFileWriter.writeUsersV2(users2, file2);
        System.out.println("✓ 用 schema v2 写入 data_v2.parquet (5 字段: + address)");

        System.out.println();
        System.out.println("每个文件都能说清自己的字段：");

        List<String> files = listParquetFiles(dir);

        for (String file : files) {
            List<String> fields = ParquetFileReader.readSchemaFields(dir.resolve(file).toString());
            System.out.println("  " + file + " -> " + fields);
        }

        System.out.println();
        System.out.println("下游决定：老文件缺少 address 时先补成 null。两个文件因此都能读：");
        for (String file : files) {
            List<String[]> results = ParquetFileReader.readWithAddress(dir.resolve(file).toString());
            for (String[] row : results) {
                System.out.println("  " + file + ": id=" + row[0] + ", address=" + row[1]);
            }
        }

        System.out.println();
        System.out.println("⚠️ 文件格式没有告诉我们：补 null 是正确规则，还是应该拒绝这次读取。");
        System.out.println("  它也没有定义这张表当前到底是 4 列还是 5 列。");
        System.out.println("  根本原因：每个文件只有自己的 schema，目录里没有统一的表级 schema 和版本。");
    }

    /**
     * 演示3：两个写者使用不同文件名，仍会覆盖彼此看到的表状态
     */
    private static void demoLostUpdate() throws IOException {
        System.out.println("[演示3] 并发提交丢失更新");
        System.out.println("----------------------");

        Path dir = Paths.get(DATA_DIR, "concurrent");
        Files.createDirectories(dir);

        ParquetFileWriter.writeUsers(List.of(
                new User(1, "Alice", 28, "alice@example.com"),
                new User(2, "Bob", 32, "bob@example.com")
        ), dir.resolve("base.parquet").toString());

        Path currentFiles = dir.resolve("current-files.txt");
        writeTableState(currentFiles, List.of("base.parquet"));
        System.out.println("初始表状态：[base.parquet]（2 条记录）");

        // 用固定的交错顺序复现并发执行，避免线程调度让教学输出不稳定。
        List<String> writer1Observed = readTableState(currentFiles);
        List<String> writer2Observed = readTableState(currentFiles);

        ParquetFileWriter.writeUsers(List.of(
                new User(3, "Charlie", 35, "charlie@example.com"),
                new User(4, "David", 29, "david@example.com")
        ), dir.resolve("writer-1.parquet").toString());

        ParquetFileWriter.writeUsers(List.of(
                new User(5, "Eve", 26, "eve@example.com"),
                new User(6, "Frank", 31, "frank@example.com")
        ), dir.resolve("writer-2.parquet").toString());

        System.out.println("两个写者都基于同一个旧状态完成了各自的数据文件。");

        List<String> writer1State = append(writer1Observed, "writer-1.parquet");
        writeTableState(currentFiles, writer1State);
        System.out.println("✓ 写者1 发布表状态：" + writer1State);

        List<String> writer2State = append(writer2Observed, "writer-2.parquet");
        writeTableState(currentFiles, writer2State);
        System.out.println("✓ 写者2 发布表状态：" + writer2State);

        System.out.println();
        System.out.println("验证结果：");
        List<String> visibleFiles = readTableState(currentFiles);
        int visibleRecords = 0;
        for (String file : visibleFiles) {
            visibleRecords += ParquetFileReader.readUsers(dir.resolve(file).toString()).size();
        }
        System.out.println("  磁盘上的 Parquet 文件：base.parquet, writer-1.parquet, writer-2.parquet");
        System.out.println("  当前表状态：" + visibleFiles);
        System.out.println("  当前可见记录数：" + visibleRecords + "（预期 6 条）");
        System.out.println("  writer-1.parquet 仍在磁盘上，却从当前表状态中消失了。");

        System.out.println();
        System.out.println("⚠️ 问题不只是文件名冲突，而是两个写者覆盖了彼此发布的表状态。");
        System.out.println("  需要一种提交机制，在发布前检查自己基于的旧状态是否仍是当前状态。");
    }

    private static List<String> readTableState(Path path) throws IOException {
        return Files.readAllLines(path, StandardCharsets.UTF_8);
    }

    private static void writeTableState(Path path, List<String> files) throws IOException {
        Files.write(path, files, StandardCharsets.UTF_8);
    }

    private static List<String> append(List<String> files, String file) {
        return java.util.stream.Stream.concat(files.stream(), java.util.stream.Stream.of(file)).toList();
    }

    private static List<String> listParquetFiles(Path dir) throws IOException {
        try (Stream<Path> paths = Files.list(dir)) {
            return paths
                    .filter(path -> path.toString().endsWith(".parquet"))
                    .map(path -> path.getFileName().toString())
                    .sorted()
                    .toList();
        }
    }

    /**
     * 清理旧数据目录
     */
    private static void cleanup() throws IOException {
        Path dataDir = Paths.get(DATA_DIR);
        if (Files.exists(dataDir)) {
            List<Path> paths;
            try (Stream<Path> walk = Files.walk(dataDir)) {
                paths = walk.sorted((a, b) -> b.compareTo(a)).toList();
            }
            for (Path path : paths) {
                Files.delete(path);
            }
        }
    }
}
