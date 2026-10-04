package com.iceberglearn.scan;

import java.util.List;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.table.Table;



/** API for configuring a scan of one table snapshot. */
public interface TableScan {

    /** Return the table from which this scan loads files. */
    Table table();

    /** Return a new scan bound to the snapshot with the given ID. */
    TableScan useSnapshot(long snapshotId);

    /** Return a new scan bound to the snapshot that was current at the given time. */
    TableScan asOfTime(long timestampMillis);

    /** Return the snapshot that will be used by this scan. */
    Snapshot snapshot();

    /** Plan the live data files referenced by the selected snapshot. */
    List<DataFile> planFiles();
}
