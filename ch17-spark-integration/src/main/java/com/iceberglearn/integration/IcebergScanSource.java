package com.iceberglearn.integration;

import com.iceberglearn.expressions.AlwaysTrue;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.scan.CombinedScanTask;
import com.iceberglearn.scan.TableScan;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * The storage-side contract that an engine talks to. Wraps a {@link Table}
 * and exposes only {@code scan(filter)} and schema conversion.
 */
public class IcebergScanSource {

    private final Table table;

    public IcebergScanSource(Table table) {
        this.table = table;
    }

    /**
     * Plan scan tasks for the current snapshot, optionally filtered.
     * This is a metadata-only operation — no Parquet data is read.
     */
    public List<CombinedScanTask> scan(Expr filter) {
        TableScan scan = table.newScan();
        if (filter != null && !(filter instanceof AlwaysTrue)) {
            scan = scan.filter(filter);
        }
        return scan.planTasks();
    }

    public Schema icebergSchema() {
        return table.schema();
    }

    /**
     * The table's root directory. Used to resolve relative data file paths
     * stored in manifests into absolute paths for Parquet reading.
     */
    public String location() {
        return table.location();
    }

    /**
     * Convert the Iceberg schema to a mini-Spark schema so the engine
     * knows column names and types without reading any metadata itself.
     */
    public com.sparklearn.sql.Schema sparkSchema() {
        List<Field> fields = new ArrayList<>();
        for (Schema.NestedField nf : table.schema().fields()) {
            DataType dataType = switch (nf.type()) {
                case "long" -> DataType.LONG;
                case "int" -> DataType.INTEGER;
                case "double" -> DataType.DOUBLE;
                case "string" -> DataType.STRING;
                case "boolean" -> DataType.BOOLEAN;
                default -> DataType.OBJECT;
            };
            fields.add(new Field(nf.name(), dataType));
        }
        return new com.sparklearn.sql.Schema(fields);
    }
}
