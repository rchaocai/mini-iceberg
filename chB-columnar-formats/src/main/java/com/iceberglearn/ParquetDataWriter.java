package com.iceberglearn;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public final class ParquetDataWriter {

    public static final Schema SALE_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Sale",
              "namespace": "com.iceberglearn",
              "fields": [
                {"name": "order_id", "type": "long"},
                {"name": "region", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "amount_cents", "type": "long"},
                {"name": "customer_email", "type": "string"}
              ]
            }
            """);

    private ParquetDataWriter() {
    }

    public static void writeCsv(List<Sale> sales, java.nio.file.Path path) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("order_id,region,status,quantity,amount_cents,customer_email");
            writer.newLine();
            for (Sale sale : sales) {
                writer.write(sale.orderId() + ","
                        + sale.region() + ","
                        + sale.status() + ","
                        + sale.quantity() + ","
                        + sale.amountCents() + ","
                        + sale.customerEmail());
                writer.newLine();
            }
        }
    }

    public static void writeParquet(List<Sale> sales, java.nio.file.Path path) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(path.toString()))
                .withSchema(SALE_SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withDictionaryEncoding(true)
                .withRowGroupSize(64 * 1024)
                .withPageSize(8 * 1024)
                .build()) {
            for (Sale sale : sales) {
                GenericRecord record = new GenericData.Record(SALE_SCHEMA);
                record.put("order_id", sale.orderId());
                record.put("region", sale.region());
                record.put("status", sale.status());
                record.put("quantity", sale.quantity());
                record.put("amount_cents", sale.amountCents());
                record.put("customer_email", sale.customerEmail());
                writer.write(record);
            }
        }
    }
}
