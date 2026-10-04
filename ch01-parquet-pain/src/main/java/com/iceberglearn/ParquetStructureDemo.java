package com.iceberglearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet 文件结构探索 Demo
 * 用 ParquetFileReader 打开文件，逐层展示 Footer / Row Group / Column Chunk / Page
 */
public class ParquetStructureDemo {

    private static final String FILE_PATH = "./data/ch01/structure/users.parquet";

    public static void main(String[] args) throws Exception {
        System.out.println("=== Parquet 文件结构探索 ===");
        System.out.println();

        // 第一步：写入一个 Parquet 文件
        writeSampleFile();

        // 第二步：用 ParquetFileReader 逐层探索文件结构
        exploreStructure();
    }

    /**
     * 写入一个包含 1000 条记录的 Parquet 文件
     */
    private static void writeSampleFile() throws IOException {
        List<User> users = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            users.add(new User(i, "user_" + i, 20 + (i % 40), "user" + i + "@example.com"));
        }

        java.nio.file.Path dir = java.nio.file.Paths.get(FILE_PATH).getParent();
        if (!java.nio.file.Files.exists(dir)) {
            java.nio.file.Files.createDirectories(dir);
        }

        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(FILE_PATH));
        ParquetFileWriter.writeUsers(users, FILE_PATH);

        long fileSize = java.nio.file.Files.size(java.nio.file.Paths.get(FILE_PATH));
        System.out.println("写入完成：" + FILE_PATH);
        System.out.println("文件大小：" + fileSize + " bytes (" + String.format("%.2f", fileSize / 1024.0) + " KB)");
        System.out.println("记录数：1000");
        System.out.println();
    }

    /**
     * 用 ParquetFileReader 逐层探索文件结构
     */
    private static void exploreStructure() throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(FILE_PATH);

        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {

            // ========== 1. Footer：文件级元数据 ==========
            System.out.println("========== 1. Footer（文件级元数据）==========");
            System.out.println();

            ParquetMetadata metadata = reader.getFooter();
            MessageType schema = metadata.getFileMetaData().getSchema();

            System.out.println("【Schema】文件自带的 schema：");
            List<Type> fields = schema.getFields();
            for (Type field : fields) {
                System.out.println("  - " + field.getName() + " : " + field.asPrimitiveType().getPrimitiveTypeName());
            }
            System.out.println();

            System.out.println("【CreatedBy】写入工具：" + metadata.getFileMetaData().getCreatedBy());
            System.out.println();

            // ========== 2. Row Group：行组 ==========
            List<BlockMetaData> rowGroups = metadata.getBlocks();
            System.out.println("========== 2. Row Group（行组）==========");
            System.out.println();
            System.out.println("行组数量：" + rowGroups.size());
            System.out.println();

            for (int i = 0; i < rowGroups.size(); i++) {
                BlockMetaData rowGroup = rowGroups.get(i);
                System.out.println("---- Row Group " + i + " ----");
                System.out.println("  行数：" + rowGroup.getRowCount());
                System.out.println("  总大小：" + rowGroup.getTotalByteSize() + " bytes");
                System.out.println("  列块数量：" + rowGroup.getColumns().size());
                System.out.println();

                // ========== 3. Column Chunk：列块 ==========
                System.out.println("  ===== 3. Column Chunk（列块）=====");
                List<ColumnChunkMetaData> columns = rowGroup.getColumns();
                for (ColumnChunkMetaData col : columns) {
                    System.out.println("  ---- Column: " + col.getPath().toDotString() + " ----");
                    System.out.println("    类型：" + col.getType());
                    System.out.println("    压缩方式：" + col.getCodec());
                    System.out.println("    编码方式：" + col.getEncodings());
                    System.out.println("    值数量：" + col.getValueCount());
                    System.out.println("    null 数量：" + col.getStatistics().getNumNulls());
                    System.out.println("    min 值：" + col.getStatistics().minAsString());
                    System.out.println("    max 值：" + col.getStatistics().maxAsString());
                    System.out.println("    压缩后大小：" + col.getTotalSize() + " bytes");
                    System.out.println("    起始偏移：" + col.getStartingPos() + " bytes");
                    System.out.println();
                }
            }

            // ========== 4. Page：页（概念说明）==========
            System.out.println("========== 4. Page（页）==========");
            System.out.println();
            System.out.println("Page 是 Column Chunk 内部的最小读写单元。");
            System.out.println("每个 Column Chunk 包含一个或多个 Page，Page 内的数据经过编码和压缩。");
            System.out.println("从上面的 Column Chunk 元数据可以看到编码方式（如 PLAIN, RLE, DICTIONARY）,");
            System.out.println("这些编码就是应用在 Page 级别的。");
            System.out.println();
            System.out.println("(Page 级别的逐页探索需要更底层的 API，这里只展示到 Column Chunk 级别。)");
            System.out.println();

            System.out.println("========== 总结 ==========");
            System.out.println();
            System.out.println("Parquet 文件从外到内的层次：");
            System.out.println("  文件 → Row Group（行组）→ Column Chunk（列块）→ Page（页）");
            System.out.println("  Footer 存在文件末尾，记录 Schema + 每个 Row Group/Column Chunk 的元数据");
            System.out.println();
            System.out.println("读取时，先读 Footer 拿到元数据，再按需跳到指定的 Column Chunk 读取 Page。");
            System.out.println("不需要的列根本不会从磁盘加载到内存——这就是列裁剪的底层原理。");
        }
    }
}
