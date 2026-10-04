package com.iceberglearn;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;

/**
 * Parquet 文件写入工具
 * 使用 parquet-avro 库简化 Parquet 文件写入
 */
public class ParquetFileWriter {

    // Avro Schema 定义
    private static final Schema USER_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "email", "type": "string"}
              ]
            }
            """);

    /**
     * 将用户列表写入 Parquet 文件
     *
     * @param users 用户列表
     * @param path  文件路径
     */
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

    /**
     * 将用户列表写入 Parquet 文件（带不同 schema 版本，用于演示 schema 不兼容问题）
     * Schema v2: 新增 address 字段
     */
    public static void writeUsersV2(List<User> users, String path) throws IOException {
        Schema userSchemaV2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "User",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "email", "type": "string"},
                    {"name": "address", "type": "string"}
                  ]
                }
                """);

        Configuration conf = new Configuration();
        
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(path))
                .withSchema(userSchemaV2)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            
            for (User user : users) {
                GenericRecord record = new GenericData.Record(userSchemaV2);
                record.put("id", user.id());
                record.put("name", user.name());
                record.put("age", user.age());
                record.put("email", user.email());
                record.put("address", "unknown");
                writer.write(record);
            }
        }
    }
}
