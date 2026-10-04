package com.iceberglearn.io;

import com.iceberglearn.schema.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Writes rows (field-id → value) to a real Parquet file, the mini analog of real Iceberg's
 * {@code FileAppender}: it turns in-memory rows into on-disk columnar bytes.
 *
 * <p>Like real Iceberg, the writer embeds each column's field id directly into the file's
 * Parquet schema ({@code optional int64 id = 1}). The file is then self-describing at the
 * Parquet level: no matter how the table schema later evolves — renames, drops, additions —
 * the bytes keep carrying their field ids, and readers match columns by id instead of by
 * name. The mini schema still rides along as the embedded Avro schema, which the Avro
 * write support uses to materialize records.
 *
 * <p>It writes bytes only; per-column statistics are read back from the footer separately by
 * {@link ParquetFooterStats} — mirroring how real Iceberg's writer writes bytes and
 * {@code FileAppender.metrics()} reads them from the Parquet footer.
 */
public final class GenericParquetWriter {

    private GenericParquetWriter() {
    }

    /** Write {@code rows} to {@code file} as Parquet, using schemas derived from {@code schema}. */
    public static void write(Schema schema, List<Map<Integer, Object>> rows, java.nio.file.Path file)
            throws IOException {
        org.apache.avro.Schema avroSchema = toAvro(schema);
        MessageType messageType = toParquet(schema);
        try (ParquetWriter<GenericRecord> writer = new ParquetWriter<>(
                new Path(file.toString()),
                new AvroWriteSupport<>(messageType, avroSchema, GenericData.get()),
                CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE)) {
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
        return org.apache.avro.Schema.createRecord("row", null, "com.iceberglearn", false, avroFields);
    }

    /**
     * Translate the mini schema into a Parquet message type that carries each column's
     * field id — the teaching analog of the official AvroSchemaUtil.convert, whose output
     * is what makes schema evolution survive renames: the ids, not the names, are the
     * durable identity of a column inside the file.
     */
    static MessageType toParquet(Schema schema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Schema.NestedField field : schema.fields()) {
            switch (field.type()) {
                case "long" -> builder.addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .id(field.id()).named(field.name()));
                case "int" -> builder.addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .id(field.id()).named(field.name()));
                case "string" -> builder.addField(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                                .id(field.id())
                                .named(field.name()));
                default -> throw new IllegalArgumentException("Unsupported column type: " + field.type());
            }
        }
        return builder.named("row");
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
