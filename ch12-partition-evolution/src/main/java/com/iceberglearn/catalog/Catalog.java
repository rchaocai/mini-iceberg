package com.iceberglearn.catalog;

import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

/** A Catalog API for table create, drop, and load operations. */
public interface Catalog {

    /**
     * Create an unpartitioned table. Delegates to {@link #createTable(TableIdentifier, Schema,
     * PartitionSpec)} with {@link PartitionSpec#unpartitioned()}, mirroring the real Iceberg
     * catalog default.
     */
    default Table createTable(TableIdentifier identifier, Schema schema) {
        return createTable(identifier, schema, PartitionSpec.unpartitioned());
    }

    /** Create a table partitioned by the given {@link PartitionSpec}. */
    Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec);

    /** Load a table by its logical identifier. */
    Table loadTable(TableIdentifier identifier);

    /** Return whether the identifier resolves to a table. */
    boolean tableExists(TableIdentifier identifier);

    /** Drop a table and the files stored below its table location. */
    boolean dropTable(TableIdentifier identifier);
}
