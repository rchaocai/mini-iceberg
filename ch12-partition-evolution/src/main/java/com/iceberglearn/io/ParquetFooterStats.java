package com.iceberglearn.io;

import com.iceberglearn.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads per-column statistics back from a Parquet file's footer, the mini analog of real Iceberg's
 * {@code FileAppender.metrics()} (which reads the Parquet footer's column statistics).
 *
 * <p>Real Iceberg collects metrics while writing bytes; here the writer ({@link GenericParquetWriter})
 * writes bytes only, and this class reads the resulting footer to populate {@code DataFile} bounds
 * and counts — so the file on disk and its descriptor always agree.
 *
 * <p>Numeric columns are normalized to {@code Long} (Integer literals lifted) so bounds match the
 * type the predicate comparison side expects; string columns stay as {@code String}. A column with
 * no non-null statistics is omitted from the bounds maps, mirroring the conservative "keep the
 * file" behaviour readers rely on.
 */
public final class ParquetFooterStats {

    private ParquetFooterStats() {
    }

    /** Read file-level metrics: bounds/counts aggregated across all row groups, keyed by field id. */
    public static FileMetrics read(Schema schema, java.nio.file.Path file) throws IOException {
        Map<String, Integer> nameToId = new HashMap<>();
        Map<String, String> nameToType = new HashMap<>();
        for (Schema.NestedField field : schema.fields()) {
            nameToId.put(field.name(), field.id());
            nameToType.put(field.name(), field.type());
        }

        Map<Integer, Object> lowerBounds = new LinkedHashMap<>();
        Map<Integer, Object> upperBounds = new LinkedHashMap<>();
        Map<Integer, Long> valueCounts = new LinkedHashMap<>();
        Map<Integer, Long> nullValueCounts = new LinkedHashMap<>();
        long rowCount = 0;

        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(file.toString()), new Configuration()))) {
            ParquetMetadata footer = reader.getFooter();
            List<BlockMetaData> rowGroups = footer.getBlocks();

            for (BlockMetaData rowGroup : rowGroups) {
                rowCount = Math.max(rowCount, rowGroup.getRowCount());
                for (ColumnChunkMetaData column : rowGroup.getColumns()) {
                    String name = column.getPath().toDotString();
                    Integer fieldId = nameToId.get(name);
                    if (fieldId == null) {
                        continue;
                    }
                    String type = nameToType.get(name);

                    long values = column.getValueCount();
                    valueCounts.merge(fieldId, values, Long::sum);

                    if (column.getStatistics() == null || column.getStatistics().isEmpty()) {
                        continue;
                    }
                    // null count from statistics when available
                    long nulls = column.getStatistics().getNumNulls();
                    nullValueCounts.merge(fieldId, nulls, Long::sum);

                    Object min = decode(column.getStatistics().minAsString(), type);
                    Object max = decode(column.getStatistics().maxAsString(), type);
                    if (min != null) {
                        lowerBounds.merge(fieldId, min,
                                (a, b) -> compare(a, b) <= 0 ? a : b);
                    }
                    if (max != null) {
                        upperBounds.merge(fieldId, max,
                                (a, b) -> compare(a, b) >= 0 ? a : b);
                    }
                }
            }
        }

        long fileSize = Files.size(file);
        return new FileMetrics(rowCount, fileSize, lowerBounds, upperBounds, valueCounts, nullValueCounts);
    }

    /** Normalize numeric bounds to Long (matching the predicate comparison side), leave strings as-is. */
    private static Object decode(String raw, String type) {
        if (raw == null) {
            return null;
        }
        return switch (type) {
            case "long", "int" -> Long.parseLong(raw);
            default -> raw;
        };
    }

    @SuppressWarnings("unchecked")
    private static int compare(Object a, Object b) {
        return ((Comparable<Object>) a).compareTo(b);
    }

    /** File-level metrics read from the footer. */
    public record FileMetrics(
            long recordCount,
            long fileSizeInBytes,
            Map<Integer, Object> lowerBounds,
            Map<Integer, Object> upperBounds,
            Map<Integer, Long> valueCounts,
            Map<Integer, Long> nullValueCounts) {
    }
}
