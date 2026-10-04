package com.iceberglearn.scan;

import com.iceberglearn.metadata.DataFile;

/**
 * A scan task over one contiguous byte range in a single data file.
 *
 * <p>Small files produce one task covering the whole file ({@code start=0},
 * {@code length=file size}). Files with row-group split offsets are split into
 * multiple tasks, one per row group — each chunk's {@code start} lands on a
 * row-group boundary read from the Parquet footer, never cutting a row group
 * in half. Files without split offsets fall back to fixed-size byte-threshold
 * chunks. See {@link SplitPlanner#splitFiles} for the two-path logic.
 */
public record FileScanTask(
        DataFile file,
        long start,
        long length
) implements ScanTask {

    @Override
    public long sizeBytes() {
        return length;
    }
}
