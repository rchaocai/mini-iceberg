package com.iceberglearn.operations;

import com.iceberglearn.metadata.TableMetadata;

/** Read-side subset of Iceberg's TableOperations SPI. */
public interface TableOperations {

    /** Return the currently loaded metadata, refreshing once when necessary. */
    TableMetadata current();

    /** Check the table location for a newer metadata version and load it. */
    TableMetadata refresh();

    /** Return a full path below the table's metadata directory. */
    String metadataFileLocation(String fileName);

    /** Return the physical metadata file loaded by the latest refresh. */
    String currentMetadataLocation();
}
