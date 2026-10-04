package com.iceberglearn.io;

import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.partition.PartitionKey;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Writes rows into data files, routing each row to a per-partition buffer keyed by
 * {@link PartitionKey}. When a buffer fills, it is flushed: the rows are written as a real Parquet
 * file under {@code dataDir}, and the column statistics and row-group split offsets are read back
 * from that file's footer to build the {@link DataFile} descriptor. The physical file layout stays
 * flat; only the metadata records the partition split.
 *
 * <p>The write path: {@link GenericParquetWriter} writes Parquet bytes, and
 * {@link ParquetFooterStats} reads the footer statistics (including split offsets).
 *
 * <p>The row-group and page sizes are passed through to {@link GenericParquetWriter} so a small
 * demo file can be made to land multiple row groups — which is what makes row-group-boundary
 * splitting visible downstream. The writer takes these as constructor parameters and defaults
 * to Parquet's stock 128 MB / 1 MB.
 */
public class PartitionedWriter {

    private final Schema schema;
    private final PartitionSpec spec;
    private final Path dataDir;
    private final int targetRowsPerFile;
    private final int rowGroupSize;
    private final int pageSize;

    // PartitionKey -> rows buffered for that partition. HashMap key correctness relies on
    // PartitionKey.equals/hashCode over the partition tuple.
    private final Map<PartitionKey, List<Map<Integer, Object>>> buffers = new HashMap<>();
    private final List<DataFile> completedFiles = new ArrayList<>();

    public PartitionedWriter(Schema schema, PartitionSpec spec, Path dataDir, int targetRowsPerFile) {
        this(schema, spec, dataDir, targetRowsPerFile,
                GenericParquetWriter.DEFAULT_ROW_GROUP_SIZE, GenericParquetWriter.DEFAULT_PAGE_SIZE);
    }

    public PartitionedWriter(Schema schema, PartitionSpec spec, Path dataDir, int targetRowsPerFile,
                             int rowGroupSize, int pageSize) {
        this.schema = schema;
        this.spec = spec;
        this.dataDir = dataDir;
        this.targetRowsPerFile = targetRowsPerFile;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
    }

    public void write(Map<Integer, Object> row) {
        PartitionKey key = new PartitionKey(spec);
        key.partition(row);

        buffers.computeIfAbsent(key, k -> new ArrayList<>()).add(row);

        List<Map<Integer, Object>> buffer = buffers.get(key);
        if (buffer.size() >= targetRowsPerFile) {
            flushPartition(key);
        }
    }

    private void flushPartition(PartitionKey key) {
        List<Map<Integer, Object>> buffer = buffers.remove(key);
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        try {
            Files.createDirectories(dataDir);
            String fileName = UUID.randomUUID() + ".parquet";
            Path file = dataDir.resolve(fileName);

            // Write real Parquet bytes, then read the footer for the file's true statistics
            // (bounds, counts, and the row-group split offsets that scan planning will split on).
            GenericParquetWriter.write(schema, buffer, file, rowGroupSize, pageSize);
            ParquetFooterStats.FileMetrics stats = ParquetFooterStats.read(schema, file);

            DataFile dataFile = DataFile.builder(dataDir.getFileName() + "/" + fileName)
                    .recordCount(stats.recordCount())
                    .fileSizeInBytes(stats.fileSizeInBytes())
                    .lowerBounds(stats.lowerBounds())
                    .upperBounds(stats.upperBounds())
                    .valueCounts(stats.valueCounts())
                    .nullValueCounts(stats.nullValueCounts())
                    .partition(key.values())
                    .splitOffsets(stats.splitOffsets())
                    .build();
            completedFiles.add(dataFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to flush partition " + key, e);
        }
    }

    public List<DataFile> complete() {
        // Copy keys: flushPartition mutates the map, so iterating it directly would throw.
        for (PartitionKey key : new ArrayList<>(buffers.keySet())) {
            flushPartition(key);
        }
        return List.copyOf(completedFiles);
    }
}
