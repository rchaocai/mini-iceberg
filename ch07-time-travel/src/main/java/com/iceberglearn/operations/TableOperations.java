package com.iceberglearn.operations;

import com.iceberglearn.metadata.TableMetadata;


/** Metadata access and commit operations for one table. */
public interface TableOperations {

    /** Return the currently loaded metadata, refreshing once when necessary. */
    TableMetadata current();

    /** Check the table location for a newer metadata version and load it. */
    TableMetadata refresh();

    /** Atomically replace base metadata with a new immutable version. */
    void commit(TableMetadata base, TableMetadata metadata);

    /** Return a full path below the table's metadata directory. */
    String metadataFileLocation(String fileName);

    /** Return the physical metadata file loaded by the latest refresh. */
    String currentMetadataLocation();
}
