package com.iceberglearn.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Schema-level helpers over Parquet files, the mini analog of real Iceberg's
 * {@code ParquetSchemaUtil}: the bridge between "the file's columns" and "the table's
 * columns", built on field ids.
 *
 * <p>Every data file written by this project carries its field ids inside the Parquet
 * schema itself. This class reads them back, so a reader can translate between the ids a
 * file knows and the column names the Parquet library uses to materialize records —
 * without consulting any table metadata. A file that predates ids (or a foreign file
 * without them) simply yields an empty map, and callers treat name-matching as their
 * fallback.
 */
public final class ParquetSchemaUtil {

    private ParquetSchemaUtil() {
    }

    /**
     * Read the file's {@code field-id → column name} mapping from its Parquet schema.
     * Insertion order follows the file's column order.
     */
    public static Map<Integer, String> fieldIdToName(java.nio.file.Path file) throws IOException {
        Map<Integer, String> idToName = new LinkedHashMap<>();
        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(file.toString()), new Configuration()))) {
            for (Type field : reader.getFooter().getFileMetaData().getSchema().getFields()) {
                if (field.getId() != null) {
                    idToName.put(field.getId().intValue(), field.getName());
                }
            }
        }
        return idToName;
    }
}
