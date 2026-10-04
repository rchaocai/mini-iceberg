package com.iceberglearn.scan;

import com.iceberglearn.metadata.DataFile;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Splits large files into byte-range tasks and bin-packs small ones into combined tasks.
 *
 * <p>{@link #splitFiles} cuts each surviving file into {@link FileScanTask} chunks, and
 * {@link #planTasks} accumulates those chunks into {@link CombinedScanTask} bins that
 * each target roughly {@code splitSize} bytes.
 *
 * <h2>Splitting: row-group boundaries when available, byte threshold otherwise</h2>
 *
 * <p>{@link #splitFiles} follows two paths:
 * <ul>
 *   <li><b>Offsets-aware path</b> — taken when {@link DataFile#splitOffsets()} is non-null,
 *       non-empty, and strictly ascending. Produces one chunk per row group: chunk {@code i}
 *       starts at {@code splitOffsets[i]} and runs to {@code splitOffsets[i+1]} (or to the
 *       file end for the last one). For Parquet, the footer's
 *       {@code BlockMetaData.getStartingPos()} gives one offset per row group; this path
 *       never cuts a row group in half, so predicate pushdown (Parquet's per-row-group
 *       min/max skipping) stays local to each chunk and the chunk is the smallest unit of
 *       IO. The {@code splitSize} parameter is <em>ignored</em> on this path: row groups are
 *       atomic, the planner does not split them further.</li>
 *   <li><b>Fixed-size fallback</b> — taken when {@code splitOffsets} is missing or not
 *       strictly ascending (e.g. a synthetic test file with no row-group metadata). Cuts
 *       the file into contiguous chunks of at most {@code splitSize} bytes, used for
 *       formats without split-offset metadata.</li>
 * </ul>
 *
 * <h2>Bin-packing: first-fit with a lookback window, with {@code openFileCost} floor</h2>
 *
 * <p>{@link #planTasks} delegates to {@link BinPacking#pack}, which mirrors real
 * Iceberg's {@code BinPacking.PackingIterator}: each split is placed by first-fit
 * into a sliding window of at most {@code lookback} bins, and when the window
 * overflows the heaviest bin is closed and emitted. {@code openFileCost} floors
 * each split's weight so a tiny file is not packed for free — opening a file has
 * real cost, and counting it prevents the planner from stuffing hundreds of tiny
 * files into one task and overwhelming the engine with open-file overhead.
 */
public final class SplitPlanner {

    /** Default target size for one combined task: 128 MB, matching HDFS block size. */
    public static final long DEFAULT_SPLIT_SIZE = 128L * 1024 * 1024;

    /**
     * Default lookback: how many open bins the packer keeps in its window before it
     * starts evicting the heaviest. Matches real Iceberg's
     * {@code TableProperties.SPLIT_LOOKBACK_DEFAULT}.
     */
    public static final int DEFAULT_SPLIT_LOOKBACK = 10;

    /** Default estimated cost of opening one file, used as a minimum task weight. */
    public static final long DEFAULT_OPEN_FILE_COST = 4L * 1024 * 1024;

    private SplitPlanner() {
    }

    /**
     * Cut each file into one or more {@link FileScanTask} chunks.
     *
     * <p>Files with usable {@link DataFile#splitOffsets()} are split at row-group boundaries
     * (one chunk per row group); files without them fall back to fixed-size byte-threshold
     * splitting. In both cases the chunks tile the file exactly: their starts and lengths
     * sum back to the file's full byte range.
     *
     * @param splitSize target chunk size in bytes, consulted only on the fixed-size fallback
     *                  path (ignored when row-group split offsets are available, since row
     *                  groups are atomic)
     */
    public static List<FileScanTask> splitFiles(List<DataFile> files, long splitSize) {
        List<FileScanTask> tasks = new ArrayList<>();
        for (DataFile file : files) {
            List<Long> offsets = file.splitOffsets();
            if (offsets != null && !offsets.isEmpty() && isStrictlyAscending(offsets)) {
                splitAtRowGroupBoundaries(file, offsets, tasks);
            } else {
                splitByFixedSize(file, splitSize, tasks);
            }
        }
        return tasks;
    }

    /**
     * Offsets-aware split — one {@link FileScanTask} per row group. Chunk {@code i}
     * starts at {@code offsets[i]} and runs to {@code offsets[i+1]}; the last chunk
     * runs to the file end. The sum of chunk lengths equals the file size.
     */
    private static void splitAtRowGroupBoundaries(
            DataFile file, List<Long> offsets, List<FileScanTask> tasks) {
        long fileSize = file.fileSizeInBytes();
        int last = offsets.size() - 1;
        for (int i = 0; i <= last; i++) {
            long start = offsets.get(i);
            long length = (i < last) ? offsets.get(i + 1) - start : fileSize - start;
            tasks.add(new FileScanTask(file, start, length));
        }
    }

    /**
     * Fixed-size fallback — cut into contiguous chunks of at most {@code splitSize} bytes.
     * Files at or below the threshold produce one whole-file task; larger files are cut
     * by a while loop that takes {@code splitSize} bytes per step (or the remaining bytes
     * on the last step).
     */
    private static void splitByFixedSize(DataFile file, long splitSize, List<FileScanTask> tasks) {
        long fileSize = file.fileSizeInBytes();
        if (fileSize <= splitSize) {
            tasks.add(new FileScanTask(file, 0, fileSize));
            return;
        }
        long offset = 0;
        while (offset < fileSize) {
            long remaining = fileSize - offset;
            long chunk = Math.min(splitSize, remaining);
            tasks.add(new FileScanTask(file, offset, chunk));
            offset += chunk;
        }
    }

    /** Strictly ascending: each offset is greater than the previous one. */
    private static boolean isStrictlyAscending(List<Long> offsets) {
        for (int i = 1; i < offsets.size(); i++) {
            if (offsets.get(i) <= offsets.get(i - 1)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Bin-pack split tasks into combined tasks of roughly {@code splitSize} bytes.
     *
     * <p>Delegates to {@link BinPacking#pack}: first-fit placement into a sliding
     * window of at most {@code lookback} bins, evicting the heaviest bin when the
     * window overflows. {@code openFileCost} floors each split's weight so a tiny
     * file is not packed for free — opening a file has real cost, and counting it
     * prevents the planner from stuffing hundreds of tiny files into one task and
     * overwhelming the engine with open-file overhead.
     */
    public static List<CombinedScanTask> planTasks(
            List<FileScanTask> splits, long splitSize, int lookback, long openFileCost) {
        Function<FileScanTask, Long> weightFunc =
                split -> Math.max(split.length(), openFileCost);
        return BinPacking.pack(splits, splitSize, lookback, weightFunc).stream()
                .map(CombinedScanTask::new)
                .toList();
    }
}
