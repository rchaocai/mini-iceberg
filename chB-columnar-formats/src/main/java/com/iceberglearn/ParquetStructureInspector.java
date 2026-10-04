package com.iceberglearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

public final class ParquetStructureInspector {

    private ParquetStructureInspector() {
    }

    public static Inspection inspect(java.nio.file.Path file, long amountThreshold) throws IOException {
        printFileEnvelope(file);

        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(file.toString()), new Configuration()))) {
            ParquetMetadata metadata = reader.getFooter();

            System.out.println("\n=== 2. Footer 中的 Schema ===");
            for (Type field : metadata.getFileMetaData().getSchema().getFields()) {
                System.out.println("- " + field.getName() + " : "
                        + field.asPrimitiveType().getPrimitiveTypeName());
            }
            System.out.println("Created by: " + metadata.getFileMetaData().getCreatedBy());

            List<BlockMetaData> rowGroups = metadata.getBlocks();
            long allColumnsBytes = 0;
            long projectedBytes = 0;
            long statusBytes = 0;
            long emailBytes = 0;
            int matchingRowGroups = 0;

            System.out.println("\n=== 3. Row Group 与 Column Chunk ===");
            System.out.println("Row Group 数量: " + rowGroups.size());

            for (int index = 0; index < rowGroups.size(); index++) {
                BlockMetaData rowGroup = rowGroups.get(index);
                boolean mayMatch = mayContainAmount(rowGroup, amountThreshold);
                if (mayMatch) {
                    matchingRowGroups++;
                }

                System.out.println("\nRow Group " + index
                        + " | rows=" + rowGroup.getRowCount()
                        + " | uncompressed=" + rowGroup.getTotalByteSize() + " bytes"
                        + " | amount_cents >= " + amountThreshold + " ? " + (mayMatch ? "可能" : "跳过"));

                for (ColumnChunkMetaData column : rowGroup.getColumns()) {
                    String name = column.getPath().toDotString();
                    long compressed = column.getTotalSize();
                    allColumnsBytes += compressed;

                    if (Set.of("status", "amount_cents").contains(name)) {
                        projectedBytes += compressed;
                    }
                    if (name.equals("status")) {
                        statusBytes += compressed;
                    }
                    if (name.equals("customer_email")) {
                        emailBytes += compressed;
                    }

                    String min = column.getStatistics().isEmpty()
                            ? "n/a"
                            : column.getStatistics().minAsString();
                    String max = column.getStatistics().isEmpty()
                            ? "n/a"
                            : column.getStatistics().maxAsString();

                    System.out.println("  " + name
                            + " | type=" + column.getType()
                            + " | codec=" + column.getCodec()
                            + " | encodings=" + column.getEncodings()
                            + " | values=" + column.getValueCount()
                            + " | min=" + min
                            + " | max=" + max
                            + " | compressed=" + compressed
                            + " | offset=" + column.getStartingPos());
                }
            }

            return new Inspection(
                    rowGroups.size(),
                    matchingRowGroups,
                    allColumnsBytes,
                    projectedBytes,
                    statusBytes,
                    emailBytes);
        }
    }

    private static boolean mayContainAmount(BlockMetaData rowGroup, long threshold) {
        return rowGroup.getColumns().stream()
                .filter(column -> column.getPath().toDotString().equals("amount_cents"))
                .findFirst()
                .map(column -> {
                    if (column.getStatistics().isEmpty()) {
                        return true;
                    }
                    try {
                        return Long.parseLong(column.getStatistics().maxAsString()) >= threshold;
                    } catch (NumberFormatException ignored) {
                        return true;
                    }
                })
                .orElse(true);
    }

    private static void printFileEnvelope(java.nio.file.Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        String headMagic = new String(bytes, 0, 4, StandardCharsets.US_ASCII);
        String tailMagic = new String(bytes, bytes.length - 4, 4, StandardCharsets.US_ASCII);
        int footerLength = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();

        System.out.println("=== 1. 文件外壳 ===");
        System.out.println("开头 magic: " + headMagic);
        System.out.println("结尾 magic: " + tailMagic);
        System.out.println("Footer 长度: " + footerLength + " bytes");
        System.out.println("文件总大小: " + bytes.length + " bytes");
    }

    public record Inspection(
            int rowGroupCount,
            int matchingRowGroupCount,
            long allColumnsBytes,
            long projectedBytes,
            long statusBytes,
            long emailBytes) {
    }
}
