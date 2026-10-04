package com.iceberglearn.io;

import com.iceberglearn.schema.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Writes rows (field-id -> value) to a real Parquet file, the mini analog of real Iceberg's
 * {@code FileAppender}: it turns in-memory rows into on-disk columnar bytes. The Avro schema is
 * built from the table {@link Schema}, so it writes any columns rather than a hardcoded record.
 *
 * <p>It writes bytes only; per-column statistics are read back from the file footer separately
 * by {@link ParquetFooterStats} -- mirroring how real Iceberg's writer writes bytes and
 * {@code FileAppender.metrics()} reads them from the Parquet footer.
 */
public final class GenericParquetWriter {

    private GenericParquetWriter() {
    }

    /** Write {@code rows} to {@code file} as Parquet, using an Avro schema derived from {@code schema}. */
    public static void write(Schema schema, List<Map<Integer, Object>> rows, java.nio.file.Path file)
            throws IOException {
        org.apache.avro.Schema avroSchema = toAvro(schema);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                new Path(file.toString()))
                .withSchema(avroSchema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (Map<Integer, Object> row : rows) {
                GenericRecord record = new GenericData.Record(avroSchema);
                for (Schema.NestedField field : schema.fields()) {
                    record.put(field.name(), row.get(field.id()));
                }
                writer.write(record);
            }
        }
    }

    /** Translate the mini string-typed schema into an Avro record schema (columns keep their names). */
    static org.apache.avro.Schema toAvro(Schema schema) {
        List<org.apache.avro.Schema.Field> avroFields = schema.fields().stream()
                .map(f -> new org.apache.avro.Schema.Field(
                        f.name(),
                        avroType(f.type()),
                        null,
                        null))
                .toList();
        org.apache.avro.Schema record = org.apache.avro.Schema.createRecord(
                "row", null, "com.iceberglearn", false, avroFields);
        return record;
    }

    private static org.apache.avro.Schema avroType(String type) {
        return switch (type) {
            case "long" -> org.apache.avro.Schema.create(Type.LONG);
            case "int" -> org.apache.avro.Schema.create(Type.INT);
            case "string" -> org.apache.avro.Schema.create(Type.STRING);
            default -> throw new IllegalArgumentException("Unsupported column type: " + type);
        };
    }
}
