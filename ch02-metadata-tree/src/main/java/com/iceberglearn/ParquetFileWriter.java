package com.iceberglearn;

import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetFileWriter {

    private static final Schema USER_SCHEMA = new Schema.Parser().parse("{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"User\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"long\"},\n" +
            "    {\"name\": \"name\", \"type\": \"string\"},\n" +
            "    {\"name\": \"age\", \"type\": \"int\"},\n" +
            "    {\"name\": \"email\", \"type\": \"string\"}\n" +
            "  ]\n" +
            "}");

    public static void writeUsers(List<User> users, String path) throws IOException {
        Configuration conf = new Configuration();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(path))
                .withSchema(USER_SCHEMA)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (User user : users) {
                GenericRecord record = new GenericData.Record(USER_SCHEMA);
                record.put("id", user.id());
                record.put("name", user.name());
                record.put("age", user.age());
                record.put("email", user.email());
                writer.write(record);
            }
        }
    }
}