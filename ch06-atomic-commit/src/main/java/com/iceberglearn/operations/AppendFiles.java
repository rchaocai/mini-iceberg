package com.iceberglearn.operations;

import com.iceberglearn.metadata.DataFile;


/** An update that collects data files and commits them in a new snapshot. */
public interface AppendFiles {

    /** Add one data file to this pending append. */
    AppendFiles appendFile(DataFile file);

    /** Build and atomically commit the new snapshot. */
    void commit();
}
