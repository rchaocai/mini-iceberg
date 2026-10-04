package com.iceberglearn;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet 文件读取工具
 * 使用 parquet-avro 库简化 Parquet 文件读取
 */
public class ParquetFileReader {

    /**
     * 从 Parquet 文件读取用户列表
     *
     * @param path 文件路径
     * @return 用户列表
     */
    public static List<User> readUsers(String path) throws IOException {
        Configuration conf = new Configuration();
        List<User> users = new ArrayList<>();
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path))
                .withConf(conf)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                long id = (Long) record.get("id");
                String name = record.get("name").toString();
                int age = (Integer) record.get("age");
                String email = record.get("email").toString();
                users.add(new User(id, name, age, email));
            }
        }
        
        return users;
    }

    public static List<String> readSchemaFields(String path) throws IOException {
        Configuration conf = new Configuration();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path))
                .withConf(conf)
                .build()) {
            GenericRecord record;
            if ((record = reader.read()) == null) {
                return List.of();
            }

            return record.getSchema().getFields().stream()
                    .map(field -> field.name())
                    .toList();
        }
    }

    public static List<String[]> readWithAddress(String path) throws IOException {
        Configuration conf = new Configuration();
        List<String[]> results = new ArrayList<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path))
                .withConf(conf)
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                String id = record.get("id").toString();
                boolean hasAddress = record.getSchema().getField("address") != null;
                String address = hasAddress ? record.get("address").toString() : null;
                results.add(new String[]{id, address});
            }
        }

        return results;
    }
}
