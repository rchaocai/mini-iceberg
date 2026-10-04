package com.iceberglearn.io;

import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.schema.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Writes rows into real Parquet data files for an unpartitioned table, the mini analog of real
 * Iceberg's {@code UnpartitionedWriter}: rows are buffered and flushed to one Parquet file per
 * {@code targetRowsPerFile} rows.
 *
 * <p>Each file gets a UUID-based name, mirroring Iceberg's
 * {@code FileGenerationUtil.generateFileName()} (which embeds a UUID), so concurrent writers in
 * later chapters never collide on the same path.
 *
 * <p>This chapter does not yet collect column bounds, so the resulting {@link DataFile} carries
 * only path / record count / file size. The data-skipping chapter extends the write path to read
 * the footer for real bounds.
 */
public class UnpartitionedWriter {

    private final Schema schema;
    private final Path dataDir;
    private final int targetRowsPerFile;
    private final List<Map<Integer, Object>> buffer = new ArrayList<>();
    private final List<DataFile> completedFiles = new ArrayList<>();

    public UnpartitionedWriter(Schema schema, Path dataDir, int targetRowsPerFile) {
        this.schema = schema;
        this.dataDir = dataDir;
        this.targetRowsPerFile = targetRowsPerFile;
    }

    public void write(Map<Integer, Object> row) {
        buffer.add(row);
        if (buffer.size() >= targetRowsPerFile) {
            flush();
        }
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<Map<Integer, Object>> rows = new ArrayList<>(buffer);
        buffer.clear();
        try {
            Files.createDirectories(dataDir);
            String fileName = UUID.randomUUID() + ".parquet";
            Path file = dataDir.resolve(fileName);
            GenericParquetWriter.write(schema, rows, file);
            DataFile dataFile = DataFile.builder(dataDir.getFileName() + "/" + fileName)
                    .recordCount(rows.size())
                    .fileSizeInBytes(Files.size(file))
                    .build();
            completedFiles.add(dataFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to flush data file", e);
        }
    }

    public List<DataFile> complete() {
        flush();
        return List.copyOf(completedFiles);
    }
}
