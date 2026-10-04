package com.iceberglearn.io;

import com.iceberglearn.schema.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads rows back from a Parquet file under a read schema, the mini analog of real Iceberg's
 * {@code ParquetReader}: the read side of schema evolution.
 *
 * <p>Rows are matched by <em>field id</em>, never by name. The file's own id → name mapping
 * ({@link ParquetSchemaUtil}) tells this reader which physical column holds each id's bytes;
 * the read schema tells it which ids to project and which labels to surface. The three
 * evolution operations fall out of this one rule:
 *
 * <ul>
 *   <li>rename — the id is unchanged, so the file's bytes land under the new name;</li>
 *   <li>add — an id missing from the file materializes as null;</li>
 *   <li>delete — an id missing from the read schema is simply never read.</li>
 * </ul>
 */
public final class ParquetReader {

    private ParquetReader() {
    }

    /**
     * Read every row of {@code file}, projected to {@code readSchema}: each row is a
     * {@code field-id → value} map over exactly the read schema's fields.
     */
    public static List<Map<Integer, Object>> read(Schema readSchema, java.nio.file.Path file) {
        try {
            Map<Integer, String> fileColumns = ParquetSchemaUtil.fieldIdToName(file);
            List<Map<Integer, Object>> rows = new ArrayList<>();
            try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader =
                         AvroParquetReader.<GenericRecord>builder(new Path(file.toString())).build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    rows.add(project(readSchema, fileColumns, record));
                }
            }
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + file, e);
        }
    }

    /** Translate one file record into a field-id keyed row of the read schema. */
    private static Map<Integer, Object> project(
            Schema readSchema, Map<Integer, String> fileColumns, GenericRecord record) {
        Map<Integer, Object> row = new LinkedHashMap<>();
        for (Schema.NestedField field : readSchema.fields()) {
            // Match by field id: the name below is the FILE's physical column name, which
            // may be an old label the current schema no longer uses. Missing id → null.
            String column = fileColumns.get(field.id());
            Object value = column != null ? record.get(column) : null;
            // Avro materializes string columns as Utf8; the mini row model speaks plain
            // Java types, so strings are unwrapped at this boundary.
            row.put(field.id(), value instanceof org.apache.avro.util.Utf8 utf8
                    ? utf8.toString() : value);
        }
        return row;
    }
}
