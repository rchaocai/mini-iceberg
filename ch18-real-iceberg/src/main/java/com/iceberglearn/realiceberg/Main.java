package com.iceberglearn.realiceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iceberglearn.io.GenericParquetWriter;
import com.iceberglearn.io.ParquetFooterStats;
import com.iceberglearn.schema.Schema;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * 第 18 章对照 Apache Iceberg 的演示入口。
 *
 * <p>四个 Step 与书稿 18.2 四个小节一一对应：
 * Step 0 用真实 Iceberg {@link HadoopTables#create} 建空表；
 * Step 1 把真实版产出的 vN.metadata.json 按字段与 mini-Iceberg 对齐打印；
 * Step 2 复用 mini-Iceberg 的 Parquet 写入器与 footer 统计读取，喂给真实版
 *   {@link org.apache.iceberg.DataFile} Builder，再走 {@code newAppend().appendFile().commit()}；
 * Step 3 读写入后新的 metadata.json：抽取 snapshot、解出 manifest-list 路径并验证
 *   Avro magic 4 字节。
 */
public final class Main {

    /** 真实 Iceberg 1.10.2 的 Schema：字段 id、名字、类型三元组与 mini 版一一对应。 */
    private static final org.apache.iceberg.Schema REAL_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "city", Types.StringType.get())
    );

    /** 供 mini-Iceberg 的 ParquetWriter/FooterStats 复用：字段 id/名字/类型一一对应。 */
    private static final Schema MINI_SCHEMA = new Schema(List.of(
            new Schema.NestedField(1, "id", "long", true),
            new Schema.NestedField(2, "name", "string", true),
            new Schema.NestedField(3, "age", "int", true),
            new Schema.NestedField(4, "city", "string", false)
    ));

    /** 表根目录：data/real-iceberg/orders */
    private static final Path TABLE_ROOT = Paths.get("data", "real-iceberg", "orders");
    /** 写入数据文件放在表根目录下的 data/ 子目录，符合真实 Iceberg 默认数据目录习惯。 */
    private static final Path DATA_DIR = TABLE_ROOT.resolve("data");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Main() {
    }

    public static void main(String[] args) throws IOException {
        PathHelper.cleanDir(TABLE_ROOT);
        Files.createDirectories(DATA_DIR);

        step0CreateTable();
        step1ReadEmptyMetadata();
        step2WriteAndAppend();
        step3ReadSnapshotAndManifest();
    }

    /** Step 0：HadoopTables.create 建空 orders 表。 */
    private static void step0CreateTable() {
        System.out.println("[Step 0] 用真实 Iceberg API 建表");

        HadoopTables tables = new HadoopTables();
        Table table = tables.create(
                REAL_SCHEMA, PartitionSpec.unpartitioned(), TABLE_ROOT.toString()
        );

        System.out.printf("  表位置            : %s%n", table.location());
        System.out.printf("  format-version    : %d%n", ((HasTableOperations) table).operations().current().formatVersion());
        System.out.printf("  table-uuid        : %s%n", ((HasTableOperations) table).operations().current().uuid());
        System.out.printf("  current-schema-id : %d (字段数: %d)%n",
                ((HasTableOperations) table).operations().current().currentSchemaId(),
                table.schema().columns().size());
        System.out.println();
    }

    /** Step 1：挑 vN.metadata.json 最新版，按顶层 11 项与 mini-Iceberg MetadataFile 并排打印。 */
    private static void step1ReadEmptyMetadata() throws IOException {
        System.out.println("[Step 1] 读取 metadata.json 顶层字段（空表）");

        Path metadataFile = PathHelper.findLatestMetadata(TABLE_ROOT);
        JsonNode root = MAPPER.readTree(metadataFile.toFile());

        System.out.printf("  最新元数据文件    : %s%n", metadataFile.getFileName());
        System.out.printf("  format-version    = %-3d       ↔ mini MetadataFile.formatVersion%n",
                root.path("format-version").asInt());
        System.out.printf("  table-uuid        = %s          (mini-Iceberg 省略)%n",
                abbrev(root.path("table-uuid").asText()));
        System.out.printf("  location          = %-32s ↔ mini MetadataFile.location%n",
                root.path("location").asText());
        System.out.printf("  last-sequence-number = %-2d    ↔ mini MetadataFile.lastSequenceNumber%n",
                root.path("last-sequence-number").asLong());
        System.out.printf("  last-column-id    = %-3d       ↔ mini MetadataFile.lastSchemaId (概念对应)%n",
                root.path("last-column-id").asInt());
        System.out.printf("  current-schema-id = %-3d       ↔ mini MetadataFile.currentSchemaId%n",
                root.path("current-schema-id").asInt());
        System.out.printf("  schemas 元素数    = %-3d       ↔ mini MetadataFile.schemas%n",
                root.path("schemas").size());
        System.out.printf("  default-spec-id   = %-3d       ↔ mini MetadataFile.defaultSpecId%n",
                root.path("default-spec-id").asInt());
        System.out.printf("  partition-specs 元素数 = %-2d   ↔ mini MetadataFile.specs%n",
                root.path("partition-specs").size());
        System.out.printf("  current-snapshot-id = %-3d     ↔ mini MetadataFile.currentSnapshotId (-1 = 空表)%n",
                root.path("current-snapshot-id").asLong());
        System.out.printf("  snapshots 元素数  = %-3d       ↔ mini MetadataFile.snapshots (空表为 0)%n",
                root.path("snapshots").size());
        System.out.println();
    }

    /** Step 2：用 mini 版写 100 行北京 age 20–40 的 Parquet，读 footer stats，再 newAppend().commit()。 */
    private static void step2WriteAndAppend() throws IOException {
        System.out.println("[Step 2] 追加数据（newAppend.commit）");

        Path parquetPath = DATA_DIR.resolve(String.format(
                "orders-beijing-20to40-%s.parquet", PathHelper.timestampTag()));

        List<Map<Integer, Object>> rows = generateRecords(1, 100, "Beijing", 20, 40);
        GenericParquetWriter.write(MINI_SCHEMA, rows, parquetPath);
        long fileSize = Files.size(parquetPath);

        // 读 footer 拿 recordCount/fileSizeInBytes/lowerBounds/upperBounds，和第 9 章 scan planning
        // 读取方式完全一致。这些字段随后原样喂给真实 Iceberg 的 DataFiles.Builder。
        ParquetFooterStats.FileMetrics metrics = ParquetFooterStats.read(MINI_SCHEMA, parquetPath);
        com.iceberglearn.metadata.DataFile miniDataFile = com.iceberglearn.metadata.DataFile
                .builder(parquetPath.toString())
                .format("parquet")
                .recordCount(metrics.recordCount())
                .fileSizeInBytes(metrics.fileSizeInBytes())
                .lowerBounds(metrics.lowerBounds())
                .upperBounds(metrics.upperBounds())
                .valueCounts(metrics.valueCounts())
                .nullValueCounts(metrics.nullValueCounts())
                .partition(List.of())
                .splitOffsets(metrics.splitOffsets())
                .build();

        org.apache.iceberg.DataFile realDataFile = org.apache.iceberg.DataFiles
                .builder(PartitionSpec.unpartitioned())
                .withPath(parquetPath.toString())
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(miniDataFile.recordCount())
                .withFileSizeInBytes(miniDataFile.fileSizeInBytes())
                .withMetrics(new MetricsAdapter(miniDataFile).toIcebergMetrics())
                .build();

        HadoopTables tables = new HadoopTables();
        Table table = tables.load(TABLE_ROOT.toString());
        table.newAppend()
                .appendFile(realDataFile)
                .commit();

        JsonNode root = MAPPER.readTree(PathHelper.findLatestMetadata(TABLE_ROOT).toFile());
        long snapshotId = root.path("current-snapshot-id").asLong();
        String manifestList = snapshotNode(root, snapshotId).path("manifest-list").asText();

        System.out.printf("  写入 Parquet     : %d 行, %d bytes%n",
                miniDataFile.recordCount(), fileSize);
        System.out.printf("  snapshot-id      : %d%n", snapshotId);
        System.out.printf("  manifest-list    : %s%n", manifestList);
        System.out.println();
    }

    /** Step 3：抽取 snapshot 字段 + 校验 manifest-list 是 Avro 二进制。 */
    private static void step3ReadSnapshotAndManifest() throws IOException {
        System.out.println("[Step 3] Snapshot 字段与 manifest-list 文件头检查");

        JsonNode root = MAPPER.readTree(PathHelper.findLatestMetadata(TABLE_ROOT).toFile());
        long snapshotId = root.path("current-snapshot-id").asLong();
        JsonNode snap = snapshotNode(root, snapshotId);

        System.out.printf("  snapshot-id        = %d   ↔ mini Snapshot.snapshotId%n",
                snap.path("snapshot-id").asLong());
        System.out.printf("  parent-snapshot-id = %-26s ↔ mini Snapshot.parentId%n",
                snap.hasNonNull("parent-snapshot-id")
                        ? String.valueOf(snap.path("parent-snapshot-id").asLong())
                        : "null");
        System.out.printf("  timestamp-ms       = %d         ↔ mini Snapshot.timestampMillis%n",
                snap.path("timestamp-ms").asLong());
        String manifestList = snap.path("manifest-list").asText();
        // manifest-list location 通常直接以 "metadata/..." 开头；若 location 前缀是 absolute 表目录下
        // 的相对写法，则按表根解析；两种写法对 "以 metadata 开头的路径" 打印结果一致。
        Path manifestListPath = resolveAgainstTable(manifestList);
        System.out.printf("  manifest-list      = %s  ↔ mini Snapshot.manifestListLocation%n",
                PathHelper.abbrev(manifestListPath));
        System.out.printf("  sequence-number    = %d                     ↔ mini Snapshot.sequenceNumber%n",
                snap.path("sequence-number").asLong());

        byte[] head = new byte[4];
        try (InputStream in = Files.newInputStream(manifestListPath)) {
            int read = in.read(head);
            if (read < head.length) {
                throw new IOException("manifest-list too short to contain Avro header: " + manifestList);
            }
        }
        String hex = String.format("%02X %02X %02X %02X", head[0], head[1], head[2], head[3]);
        long manifestFileSize = Files.size(manifestListPath);
        System.out.println();
        System.out.printf("  manifest-list 前 4 字节 : %s (Avro magic: 'O' 'b' 'j' 0x01)%n", hex);
        System.out.printf("  manifest-list 文件大小  : %d bytes（二进制 Avro）%n", manifestFileSize);
    }

    // --------------------------- helpers ---------------------------

    /** 生成 n 行 id 从 startId 起、指定 city、age 在 [ageMin, ageMax] 闭区间均匀分布的记录。 */
    private static List<Map<Integer, Object>> generateRecords(
            long startId, int n, String city, int ageMin, int ageMax) {
        List<Map<Integer, Object>> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long id = startId + i;
            String name = "user-" + id;
            int age = ageMin + i % Math.max(1, ageMax - ageMin + 1);
            rows.add(Map.of(1, id, 2, name, 3, age, 4, city));
        }
        return rows;
    }

    /** 在 metadata.json 中按 snapshot-id 找对应快照对象。 */
    private static JsonNode snapshotNode(JsonNode root, long snapshotId) {
        Iterator<JsonNode> it = root.path("snapshots").elements();
        Iterable<JsonNode> iterable = () -> it;
        return StreamSupport.stream(iterable.spliterator(), false)
                .filter(s -> s.path("snapshot-id").asLong() == snapshotId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "snapshot id " + snapshotId + " not found in snapshots list"));
    }

    /** manifest-list location 通常是绝对路径 "…/table/metadata/snap-…avro"；已绝对化的不重复拼前缀。 */
    private static Path resolveAgainstTable(String location) {
        Path p = Paths.get(location);
        if (p.isAbsolute()) return p;
        Path candidate = TABLE_ROOT.resolve(location);
        if (Files.exists(candidate)) return candidate;
        // 兜底：location 可能来自另一台机器写出的绝对路径，本工程实际运行时 HadoopTables
        // 统一写成绝对路径，所以这条分支一般不走；保留作防御。
        return p;
    }

    /** 长 uuid 只展示前几段，避免一行撑爆。 */
    private static String abbrev(String uuid) {
        if (uuid == null || uuid.length() <= 18) return uuid;
        return uuid.substring(0, 18) + "-...";
    }
}
