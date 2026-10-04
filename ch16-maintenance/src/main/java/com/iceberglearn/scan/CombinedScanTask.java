package com.iceberglearn.scan;

import java.util.List;

/**
 * A scan task made of several file ranges, bin-packed to roughly one split size.
 *
 * <p>Small files don't justify one task each — the per-task overhead (opening the
 * file, scheduling, network round-trips) can dwarf the actual read. Bin-packing
 * combines several small file ranges into one combined task that targets the
 * configured split size, so an engine worker gets enough work to amortize the
 * overhead.
 */
public record CombinedScanTask(
        List<FileScanTask> files
) implements ScanTask {

    public CombinedScanTask {
        files = List.copyOf(files);
    }

    @Override
    public long sizeBytes() {
        long total = 0;
        for (FileScanTask task : files) {
            total += task.sizeBytes();
        }
        return total;
    }
}
