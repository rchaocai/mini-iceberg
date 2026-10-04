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
 * Writes rows into real Parquet data files, routing each row to a per-partition buffer keyed by
 * {@link PartitionKey}. When a buffer fills, it is flushed: the rows are written as a real Parquet
 * file under {@code dataDir}, and the column statistics are read back from that file's footer to
 * build the {@link DataFile} descriptor. The physical file layout stays flat; only the metadata
 * records the partition split.
 *
 * <p>Each file gets a UUID-based name, mirroring Iceberg's
 * {@code FileGenerationUtil.generateFileName()} (which embeds a UUID), so concurrent writers in
 * later chapters never collide on the same path.
 *
 * <p>This mirrors real Iceberg's write path: a {@code FileAppender} writes Parquet bytes, and
 * {@code FileAppender.metrics()} reads the footer statistics. {@link GenericParquetWriter} plays
 * the appender role; {@link ParquetFooterStats} reads the metrics — so the bounds that
 * {@code MetricsFilter} reads at planning time come from the file on disk, not from a parallel
 * in-memory accumulator.
 */
public class PartitionedWriter {

    private final Schema schema;
    private final PartitionSpec spec;
    private final Path dataDir;
    private final int targetRowsPerFile;

    // PartitionKey -> rows buffered for that partition. HashMap key correctness relies on
    // PartitionKey.equals/hashCode over the partition tuple.
    private final Map<PartitionKey, List<Map<Integer, Object>>> buffers = new HashMap<>();
    private final List<DataFile> completedFiles = new ArrayList<>();

    public PartitionedWriter(Schema schema, PartitionSpec spec, Path dataDir, int targetRowsPerFile) {
        this.schema = schema;
        this.spec = spec;
        this.dataDir = dataDir;
        this.targetRowsPerFile = targetRowsPerFile;
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

            // Write real Parquet bytes, then read the footer for the file's true statistics.
            GenericParquetWriter.write(schema, buffer, file);
            ParquetFooterStats.FileMetrics stats = ParquetFooterStats.read(schema, file);

            DataFile dataFile = DataFile.builder(dataDir.getFileName() + "/" + fileName)
                    .recordCount(stats.recordCount())
                    .fileSizeInBytes(stats.fileSizeInBytes())
                    .lowerBounds(stats.lowerBounds())
                    .upperBounds(stats.upperBounds())
                    .valueCounts(stats.valueCounts())
                    .nullValueCounts(stats.nullValueCounts())
                    .partition(key.values())
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
