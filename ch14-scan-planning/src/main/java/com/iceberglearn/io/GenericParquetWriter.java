package com.iceberglearn.io;

import com.iceberglearn.schema.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
 * Writes rows (field-id → value) to a real Parquet file: it turns in-memory rows into
 * on-disk columnar bytes.
 *
 * <p>The writer embeds each column's field id directly into the file's Parquet schema
 * ({@code optional int64 id = 1}). The file is then self-describing at the Parquet level:
 * no matter how the table schema later evolves — renames, drops, additions — the bytes
 * keep carrying their field ids, and readers match columns by id instead of by name. The
 * schema still rides along as the embedded Avro schema, which the Avro write support uses
 * to materialize records.
 *
 * <p>It writes bytes only; per-column statistics and row-group split offsets are read back
 * from the footer separately by {@link ParquetFooterStats}.
 *
 * <p>Two overloads exist: the default uses Parquet's stock row-group/page sizes (one row group
 * per file in practice — the writer never reaches the 128 MB block threshold); the demo
 * overload takes explicit {@code rowGroupSize}/{@code pageSize} so a small file can be made
 * to land multiple row groups, which is what makes row-group-boundary splitting visible.
 * The writer takes these as constructor parameters.
 */
public final class GenericParquetWriter {

    /** Default row-group size used by the simple overload: Parquet's stock 128 MB block. */
    public static final int DEFAULT_ROW_GROUP_SIZE = ParquetWriter.DEFAULT_BLOCK_SIZE;

    /** Default page size used by the simple overload: Parquet's stock 1 MB page. */
    public static final int DEFAULT_PAGE_SIZE = ParquetWriter.DEFAULT_PAGE_SIZE;

    private GenericParquetWriter() {
    }

    /**
     * Write {@code rows} to {@code file} as Parquet, using Parquet's default row-group and page
     * sizes. With default sizes (128 MB row group) and small demo data, the file lands as a
     * single row group; call {@link #write(Schema, List, java.nio.file.Path, int, int)} when
     * multiple row groups are needed (e.g. to demonstrate row-group-boundary splitting).
     */
    public static void write(Schema schema, List<Map<Integer, Object>> rows, java.nio.file.Path file)
            throws IOException {
        write(schema, rows, file, DEFAULT_ROW_GROUP_SIZE, DEFAULT_PAGE_SIZE);
    }

    /**
     * Write {@code rows} to {@code file} as Parquet with an explicit row-group size and page size.
     * The footer's resulting {@code BlockMetaData.getStartingPos()} list is what later becomes
     * the file's split offsets.
     */
    @SuppressWarnings("deprecation")
    public static void write(Schema schema, List<Map<Integer, Object>> rows, java.nio.file.Path file,
                             int rowGroupSize, int pageSize) throws IOException {
        org.apache.avro.Schema avroSchema = toAvro(schema);
        MessageType messageType = toParquet(schema);
        try (ParquetWriter<GenericRecord> writer = new ParquetWriter<>(
                new Path(file.toString()),
                new AvroWriteSupport<>(messageType, avroSchema, GenericData.get()),
                CompressionCodecName.SNAPPY,
                rowGroupSize,
                pageSize)) {
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
