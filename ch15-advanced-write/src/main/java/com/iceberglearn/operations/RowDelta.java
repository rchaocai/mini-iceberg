package com.iceberglearn.operations;

import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.DeleteFile;

/**
 * An update that encodes row-level changes — inserts plus row deletes — in a new snapshot.
 *
 * <p>Unlike {@link OverwriteFiles}, which removes whole data files, RowDelta <em>adds</em>
 * {@link DeleteFile}s alongside new {@link DataFile}s. A delete file does not remove a data
 * file from the table; it tells readers which rows to drop when scanning a data file. The
 * data files go into a DATA manifest, the delete files into a separate DELETES manifest,
 * and both are listed in the new snapshot's manifest list.
 */
public interface RowDelta {

    /** Add a data file of new rows. */
    RowDelta addRows(DataFile inserts);

    /** Add a delete file encoding rows to remove. */
    RowDelta addDeletes(DeleteFile deletes);

    /** Build and atomically commit the new snapshot. */
    void commit();
}
