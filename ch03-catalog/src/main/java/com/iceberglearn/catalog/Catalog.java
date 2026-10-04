package com.iceberglearn.catalog;

import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

/** A Catalog API for table create, drop, and load operations. */
public interface Catalog {

    /** Create an empty table. */
    Table createTable(TableIdentifier identifier, Schema schema);

    /** Load a table by its logical identifier. */
    Table loadTable(TableIdentifier identifier);

    /** Return whether the identifier resolves to a table. */
    boolean tableExists(TableIdentifier identifier);

    /** Drop a table and the files stored below its table location. */
    boolean dropTable(TableIdentifier identifier);
}
