package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.Exprs;
import com.iceberglearn.expressions.Projections;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestFiles;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;
import com.iceberglearn.transforms.Hours;

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

/** Chapter 12: evolve the partition spec from day to hour with zero data movement. */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

    /** Locate the project root so data lands in iceberg-java/data/ch12 regardless of cwd. */
    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch12-partition-evolution"))) {
            return cwd.resolve("data").resolve("ch12");
        }
        return cwd.getParent().resolve("data").resolve("ch12");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);

        Schema schema = schema();
        // The first spec of a table starts at spec-id 0 (the official INITIAL_SPEC_ID).
        PartitionSpec daySpec = PartitionSpec.builderFor(schema)
                .day("event_time")
                .build();

        Path warehouse = BASE_DIR.resolve("warehouse");
        Files.createDirectories(warehouse);
        TableIdentifier id = TableIdentifier.of("events");

        System.out.println("[步骤1] 建表：第一个分区策略 day(event_time)，spec-id=0");
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(id, schema, daySpec);
        System.out.println("  " + daySpec.toString().replace("\n", "\n  "));

        System.out.println("\n[步骤2] 按 day spec 写第一批 3 个文件（每次写入独立提交，共 3 个快照）");
        Path dataDir = Path.of(table.location()).resolve("data");
        writeRange(table, daySpec, dataDir, day(20661) + 10 * MICROS_PER_HOUR, 55, 70);
        writeRange(table, daySpec, dataDir, day(20661) + 10 * MICROS_PER_HOUR + 30L * 1_000_000L, 10, 25);
        writeRange(table, daySpec, dataDir, day(20662) + 9 * MICROS_PER_HOUR, 60, 80);
        Set<String> oldFilePaths = new HashSet<>();
        for (DataFile file : table.newScan().planFiles()) {
            oldFilePaths.add(file.path());
        }
        System.out.println("  老文件已落盘: " + oldFilePaths.size() + " 个");

        // The pain: a user asking for ONE hour still gets a whole DAY back from the
        // partition stage — day granularity simply cannot see hours.
        System.out.println("\n[步骤3] 痛点：想按小时查，分区谓词却只能保到天");
        Expr oneHour = Exprs.and(
                Exprs.greaterThanOrEqual(EVENT_TIME, day(20661) + 10 * MICROS_PER_HOUR),
                Exprs.lessThan(EVENT_TIME, day(20661) + 11 * MICROS_PER_HOUR));
        Expr dayPartitionFilter = Projections.inclusive(daySpec).project(oneHour);
        System.out.println("  源列谓词:   " + oneHour);
        System.out.println("  day 投影:   " + dayPartitionFilter);
        System.out.println("  10:00-11:00 的一小时，两个文件都在 day=20661 —— 分区裁剪一个都裁不掉。");

        System.out.println("\n[步骤4] 分区演进一（删除 + 新增）：removeField(day) + addField(hour)");
        System.out.println("  分区演进前 partition-specs:");
        printSpecs(table.metadata());
        long snapshotIdBefore = table.currentSnapshot().snapshotId();
        table.updatePartitionSpec()
                .removeField("event_time_day")
                .addField("event_time", Hours.get())
                .commit();
        table.refresh();
        System.out.println("  分区演进后 partition-specs:");
        printSpecs(table.metadata());
        System.out.println("  删除的效果: spec-id=0 里 day 字段原样保留 —— 删除只影响新 spec，老数据照常按它解读");
        System.out.println("  新增的效果: hour 字段拿到全新 field-id=1001（1000 已被 day 占用，永不回收）");
        System.out.println("  快照数: " + snapshotCount(table) + "（spec 更新不产生新快照）");
        System.out.println("  当前快照仍是 " + table.currentSnapshot().snapshotId()
                + "（演进前 " + snapshotIdBefore + "）");

        System.out.println("\n[步骤5] 按 hour spec（table.spec()）写第二批 3 个文件");
        writeRange(table, table.spec(), dataDir, day(20661) + 10 * MICROS_PER_HOUR, 55, 90);
        writeRange(table, table.spec(), dataDir, day(20661) + 14 * MICROS_PER_HOUR, 55, 90);
        writeRange(table, table.spec(), dataDir, day(20662) + 9 * MICROS_PER_HOUR, 55, 90);
        ManifestList manifestList = ManifestFiles.readManifestList(
                table.location(), table.currentSnapshot().manifestListLocation());
        System.out.println("  最新快照的 manifest-list（manifest 的组织单位是提交，第一批 3 次提交各写一个）:");
        for (ManifestFile manifest : manifestList.manifests()) {
            System.out.println("    " + manifest.path()
                    + "  partition-spec-id=" + manifest.partitionSpecId());
        }
        System.out.println("  老 manifest 原样复用（spec 0），新 manifest 标记 spec 1 —— 写入侧零改动。");

        System.out.println("\n[步骤6] 查询：一条源列谓词，两组文件各自投影裁剪");
        Expr rowFilter = Exprs.and(oneHour, Exprs.greaterThan(AGE, 50));
        System.out.println("  row filter: " + rowFilter);
        System.out.println("  用户不知道 spec 演进发生过，也不知道两个 spec 的 field-id。");
        List<DataFile> planned = table.newScan().filter(rowFilter).planFiles();
        System.out.println("  最终读取:");
        for (DataFile file : planned) {
            System.out.println("    " + file.path() + "  partition=" + file.partition());
        }
        System.out.println("  day 组只裁到天：两个 day=20661 文件都进了指标裁剪，靠 age 裁掉一个。");
        System.out.println("  hour 组裁到小时：14:00 的文件在分区裁剪就出局，指标裁剪根本不用看。");

        System.out.println("\n[步骤7] 验证：老数据零移动");
        int rewritten = 0;
        List<String> allPaths = new ArrayList<>();
        for (DataFile file : table.newScan().planFiles()) {
            allPaths.add(file.path());
        }
        for (String path : oldFilePaths) {
            if (!Files.exists(Path.of(table.location()).resolve(path))) {
                rewritten++;
            }
        }
        System.out.println("  演进前老文件: " + oldFilePaths.size() + " 个，全部仍在原路径: "
                + (rewritten == 0 ? "是" : "否"));
        System.out.println("  现在全量文件: " + allPaths.size() + " 个（老 " + oldFilePaths.size()
                + " + 新 " + (allPaths.size() - oldFilePaths.size()) + "）");
        System.out.println("  重写/移动的旧文件数: " + rewritten);

        System.out.println("\n[步骤8] 分区演进二（重命名）：renameField 只换名字，field-id 不变");
        table.updatePartitionSpec()
                .renameField("event_time_hour", "event_hour")
                .commit();
        table.refresh();
        System.out.println("  分区演进后 partition-specs:");
        printSpecs(table.metadata());
        System.out.println("  重命名的效果: spec-id=2 与 spec-id=1 的字段 field-id 都是 1001 ——");
        System.out.println("  同一个字段换了名字，manifest 和 tuple 里只有 id 和值，没有名字。");
        System.out.println("  快照数: " + snapshotCount(table) + "（重命名同样是纯元数据变更）");
        System.out.println("  再跑同一条查询，结果应当与步骤 6 完全一致:");
        List<DataFile> plannedAfterRename = table.newScan().filter(rowFilter).planFiles();
        for (DataFile file : plannedAfterRename) {
            System.out.println("    " + file.path() + "  partition=" + file.partition());
        }
        System.out.println("  改名对已落盘的数据零影响，读取结果一个文件都不变。");

        System.out.println("\n[步骤9] 总结：演进三要素");
        System.out.println("  1. spec-id 版本化历史：specs 列表追加不覆盖，defaultSpecId 指针切换。");
        System.out.println("  2. field-id 是表级资源：新字段拿全新 id，幸存字段保留原 id，跨 spec 绝不复用。");
        System.out.println("  3. manifest 的 partition-spec-id：读端按 manifest 分组，各组用各自的 spec 投影谓词。");
        System.out.println("  分区是声明，不是物理布局 —— 换声明不需要动数据。");
    }

    /** Print the specs list plus the default pointer — the before/after view of evolution. */
    private static void printSpecs(TableMetadata metadata) {
        for (PartitionSpec spec : metadata.partitionSpecs()) {
            System.out.println("    spec-id=" + spec.specId() + ": "
                    + spec.toString().replace("\n", " ").trim());
        }
        System.out.println("    default-spec-id: " + metadata.defaultSpecId());
    }

    private static void writeRange(Table table, PartitionSpec spec, Path dataDir,
                                   long eventTime, int minAge, int maxAge) {
        PartitionedWriter writer = new PartitionedWriter(schema(), spec, dataDir, 2);
        writer.write(row(1, eventTime, minAge));
        writer.write(row(2, eventTime, maxAge));
        AppendFiles append = table.newAppend();
        for (DataFile file : writer.complete()) {
            append.appendFile(file);
        }
        append.commit();
    }

    private static int snapshotCount(Table table) {
        int count = 0;
        for (var ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private static long day(int epochDay) {
        return epochDay * MICROS_PER_DAY;
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

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(1, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")));
    }

    private static Map<Integer, Object> row(long id, long eventTime, int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(1, id);
        row.put(2, "user-" + id);
        row.put(EVENT_TIME, eventTime);
        row.put(4, "demo");
        row.put(AGE, age);
        return row;
    }
}
