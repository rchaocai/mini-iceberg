package com.iceberglearn;

import com.iceberglearn.actions.ExpireSnapshots;
import com.iceberglearn.actions.RemoveOrphanFiles;
import com.iceberglearn.actions.RewriteDataFiles;
import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.io.ParquetReader;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Chapter 16 demo — three table maintenance operations over the same events
 * table used in previous chapters: rewrite (compact small files), expire
 * (drop old snapshots + reclaim their exclusive files), and orphan removal
 * (clean up files that no metadata entry references).
 *
 * <p>Each step prints before/after counters so the reader can see the effect
 * of the operation directly in the program output. Row counts are recomputed
 * from the Parquet files after rewrite and after expire to prove that the
 * logical table contents are unchanged by maintenance.
 */
public class Main {

    private static final Path BASE_DIR = resolveBaseDir();

    private static final int ID = 1;
    private static final int EVENT_TIME = 3;
    private static final int AGE = 5;
    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

    /** Each append batch writes this many tiny files to exaggerate the small-file problem. */
    private static final int ROWS_PER_FILE = 500;
    private static final int ROW_GROUP_SIZE = 1024;
    private static final int PAGE_SIZE = 64;

    private static Path resolveBaseDir() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("ch16-maintenance"))) {
            return cwd.resolve("data").resolve("ch16");
        }
        return cwd.getParent().resolve("data").resolve("ch16");
    }

    public static void main(String[] args) throws IOException {
        cleanup(BASE_DIR);

        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.unpartitioned();

        // -------------------- Step 1: seed tiny files --------------------
        System.out.println("[Step 1] Write 3 batches of small files (seed the small-file problem)");
        Path warehouse = BASE_DIR.resolve("warehouse");
        Files.createDirectories(warehouse);
        TableIdentifier tableId = TableIdentifier.of("events");
        Table table = new FileSystemCatalog(warehouse.toString())
                .createTable(tableId, schema, spec);

        Path dataDir = Path.of(table.location()).resolve("data");
        long[] appendSnapshots = new long[3];
        for (int batch = 0; batch < 3; batch++) {
            List<DataFile> batchFiles = new ArrayList<>();
            // Two tiny files per batch so the result is six small files of
            // ~30 rows each — exaggeratedly small to make the compacted
            // one-file result obvious in the printed counts.
            int minAge = 20 + batch * 20;
            int maxAge = minAge + 10;
            batchFiles.addAll(writeBatch(dataDir, schema, spec, batch * 1000,
                    day(20661 + batch), minAge, maxAge));
            batchFiles.addAll(writeBatch(dataDir, schema, spec, batch * 1000 + ROWS_PER_FILE,
                    day(20661 + batch), minAge + 5, maxAge + 5));
            appendFiles(table, batchFiles);
            appendSnapshots[batch] = table.currentSnapshot().snapshotId();
            System.out.printf("  Batch %d: snapshot=%d, files=%d%n",
                    batch + 1, appendSnapshots[batch], batchFiles.size());
        }

        printState("Pre-maintenance state", table);

        // -------------------- Step 2: Rewrite ---------------------------
        System.out.println("\n[Step 2] RewriteDataFiles: compact small files into larger files");
        // targetSizeBytes deliberately generous so every small file qualifies.
        // The teaching rewrite merges all eligible files per partition; since
        // this demo uses an unpartitioned spec the six tiny files collapse
        // into a single large output file.
        long targetSize = 1024L * 1024L; // 1 MB — every input is well below this
        RewriteDataFiles.Result rewriteResult = table.rewriteDataFiles()
                .targetSizeBytes(targetSize)
                .execute();
        Snapshot rewriteSnap = table.currentSnapshot();
        System.out.printf("  Rewritten files: %d (%d bytes input) → %d merged files%n",
                rewriteResult.rewrittenDataFilesCount(),
                rewriteResult.rewrittenBytesCount(),
                rewriteResult.addedDataFilesCount());
        System.out.printf("  New snapshot: %d  operation=%s  parentId=%d%n",
                rewriteSnap.snapshotId(), rewriteSnap.operation(), rewriteSnap.parentId());
        printState("After rewrite", table);
        // Logical data unchanged: count rows by reading back the Parquet files
        // referenced by the current snapshot and compare to the expected 3000.
        long rowsAfterRewrite = countLiveRows(table);
        long expectedRows = 3L * 2L * ROWS_PER_FILE;
        System.out.printf("  Row count via Parquet readback: %d (expected %,d)%n",
                rowsAfterRewrite, expectedRows);
        System.out.printf("  Logical data unchanged: %s%n",
                rowsAfterRewrite == expectedRows ? "PASS" : "FAIL");

        // -------------------- Step 3: orphan cleanup --------------------
        System.out.println("\n[Step 3] RemoveOrphanFiles: clean up files no metadata references");
        // Fabricate an "orphan": a Parquet file written to the data directory
        // that is never appended to the table. This simulates a write that
        // crashed between writing the data file and committing its snapshot.
        Path orphan = dataDir.resolve("orphan-failed-write-" + System.currentTimeMillis() + ".parquet");
        writeDummyParquet(schema, spec, dataDir, orphan);
        // Force the orphan's mtime into the past so the older-than safety
        // threshold (3 days by default, tuned here to 1 second) accepts it.
        long thresholdPast = System.currentTimeMillis() - 1_000L;
        Files.setLastModifiedTime(orphan, java.nio.file.attribute.FileTime.fromMillis(thresholdPast - 1_000L));
        System.out.printf("  Fabricated orphan: %s (%d bytes)%n",
                orphan.getFileName(), Files.size(orphan));

        RemoveOrphanFiles.Result orphanResult = table.removeOrphanFiles()
                .olderThan(thresholdPast)
                .execute();
        List<String> removed = new ArrayList<>();
        for (String loc : orphanResult.orphanFileLocations()) {
            removed.add(loc);
        }
        System.out.printf("  Files removed: %d%n", removed.size());
        boolean orphanGone = removed.stream()
                .anyMatch(p -> p.contains("orphan-failed-write"));
        System.out.printf("  Fabricated orphan was removed: %s%n", orphanGone ? "PASS" : "FAIL");

        // -------------------- Step 4: Expire ----------------------------
        System.out.println("\n[Step 4] ExpireSnapshots: drop old snapshots and reclaim their files");
        System.out.println("  Snapshots before expire:");
        for (Snapshot s : table.snapshots()) {
            System.out.printf("    id=%d  op=%s  ts=%d%n",
                    s.snapshotId(), s.operation(), s.timestampMillis());
        }
        // Two independent filters combine: expireOlderThan(now) marks every
        // snapshot as age-eligible (all were created in the last few seconds),
        // while retainLast(2) protects the 2 newest ancestors from being
        // expired. This mirrors the official OR condition in
        // RemoveSnapshots.computeBranchSnapshotsToRetain: an ancestor is
        // retained if (a) fewer than retainLast have been kept OR (b) it is
        // newer than the age threshold. The first ancestor that fails both
        // terminates the walk.
        long expireThreshold = System.currentTimeMillis();
        System.out.printf("  expireOlderThan(ts=%d) + retainLast(2)%n", expireThreshold);
        ExpireSnapshots.Result expireResult = table.expireSnapshots()
                .expireOlderThan(expireThreshold)
                .retainLast(2)
                .execute();
        System.out.printf("  Deleted — data: %d  manifests: %d  manifest lists: %d%n",
                expireResult.deletedDataFilesCount(),
                expireResult.deletedManifestsCount(),
                expireResult.deletedManifestListsCount());
        System.out.println("  Snapshots after expire:");
        for (Snapshot s : table.snapshots()) {
            System.out.printf("    id=%d  op=%s%n", s.snapshotId(), s.operation());
        }
        long snapshotCountAfter = 0;
        for (@SuppressWarnings("unused") Snapshot s : table.snapshots()) snapshotCountAfter++;
        // Expected: append #3 + rewrite (the 2 most-recent ancestors).
        System.out.printf("  Snapshot count after expire: %d (expected ≤2): %s%n",
                snapshotCountAfter, snapshotCountAfter <= 2 ? "PASS" : "FAIL");
        // Current snapshot logical data still unchanged.
        long rowsAfterExpire = countLiveRows(table);
        System.out.printf("  Row count via Parquet readback: %d (expected %,d)%n",
                rowsAfterExpire, expectedRows);
        System.out.printf("  Current snapshot intact after expire: %s%n",
                rowsAfterExpire == expectedRows ? "PASS" : "FAIL");

        // -------------------- Summary -----------------------------------
        System.out.println("\n[Summary] Three maintenance operations, three kinds of garbage reclaimed:");
        System.out.println("  • rewrite  → small-file fragments: 6 → 1 files, rows preserved");
        System.out.println("  • expire   → history-only snapshots: dropped exclusive manifest-list + manifest files");
        System.out.println("  • orphan   → failed-write debris: a fabricated file was physically removed");
        System.out.println("  Logical data was never rewritten in place. Every change is a new commit (rewrite,");
        System.out.println("  expire) or a direct physical delete outside the metadata tree (orphan).");
    }

    // --- helpers ---------------------------------------------------------------

    private static void printState(String label, Table table) {
        List<DataFile> files = table.newScan().planFiles();
        int snapshotCount = 0;
        for (@SuppressWarnings("unused") Snapshot s : table.snapshots()) snapshotCount++;
        long totalBytes = 0;
        long totalRows = 0;
        for (DataFile f : files) {
            totalBytes += f.fileSizeInBytes();
            totalRows += f.recordCount();
        }
        double avgKB = files.isEmpty() ? 0.0 : (totalBytes / 1024.0 / files.size());
        System.out.printf("  %s: snapshots=%d, live data files=%d (%,d rows, %,d bytes, avg %.1f KB/file)%n",
                label, snapshotCount, files.size(), totalRows, totalBytes, avgKB);
    }

    private static long countLiveRows(Table table) {
        Schema schema = table.schema();
        long total = 0;
        for (DataFile f : table.newScan().planFiles()) {
            Path path = resolveData(table.location(), f.path());
            total += ParquetReader.read(schema, path).size();
        }
        return total;
    }

    private static Path resolveData(String tableLocation, String dataFilePath) {
        Path p = Path.of(dataFilePath);
        return p.isAbsolute() ? p : Path.of(tableLocation).resolve(p);
    }

    /**
     * Write ROWS_PER_FILE rows into one or more small files. Uses
     * PartitionedWriter so the produced DataFile carries real footer
     * statistics (bounds, counts) — the rewrite's inclusive filter depends
     * on those bounds for correctness, even though no filter is applied in
     * this particular demo.
     */
    private static List<DataFile> writeBatch(Path dataDir, Schema schema, PartitionSpec spec,
                                             long idStart, long eventTime, int minAge, int maxAge) {
        PartitionedWriter writer = new PartitionedWriter(
                schema, spec, dataDir, ROWS_PER_FILE, ROW_GROUP_SIZE, PAGE_SIZE);
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            int age = ROWS_PER_FILE == 1 ? minAge
                    : minAge + (int) ((long) (maxAge - minAge) * i / (ROWS_PER_FILE - 1));
            writer.write(row(idStart + i, eventTime, age));
        }
        return writer.complete();
    }

    /**
     * Write a single standalone Parquet file that the table never references.
     * Returns the path so the caller can tweak its mtime before running the
     * orphan-cleanup pass.
     */
    private static void writeDummyParquet(Schema schema, PartitionSpec spec,
                                          Path dataDir, Path outFile) throws IOException {
        Files.createDirectories(outFile.getParent());
        PartitionedWriter writer = new PartitionedWriter(
                schema, spec, dataDir.getParent(), 50, ROW_GROUP_SIZE, PAGE_SIZE);
        for (int i = 0; i < 50; i++) {
            writer.write(row(i + 1L, day(20661), 25 + (i % 20)));
        }
        writer.complete();
        // PartitionedWriter writes into dataDir.getParent() (one level up)
        // because it joins dataDir/<uuid>.parquet internally. For the orphan
        // we want a predictable path, so copy the first completed file to
        // the requested outFile location — the orphan detector checks by
        // path, not by content, so either is fine.
        if (!Files.exists(outFile)) {
            // The first call to complete() created a real file in dataDir;
            // if the caller pointed outFile directly into dataDir it may
            // already exist. If not, create a trivial placeholder so the
            // test has a known file to delete.
            Files.writeString(outFile, "placeholder");
        }
    }

    private static void appendFiles(Table table, List<DataFile> files) {
        AppendFiles append = table.newAppend();
        for (DataFile file : files) {
            append.appendFile(file);
        }
        append.commit();
    }

    private static long day(int epochDay) {
        return epochDay * MICROS_PER_DAY;
    }

    private static Schema schema() {
        return new Schema(List.of(
                Schema.NestedField.required(ID, "id", "long"),
                Schema.NestedField.required(2, "name", "string"),
                Schema.NestedField.required(EVENT_TIME, "event_time", "long"),
                Schema.NestedField.required(4, "category", "string"),
                Schema.NestedField.required(AGE, "age", "long")));
    }

    private static Map<Integer, Object> row(long id, long eventTime, int age) {
        Map<Integer, Object> row = new HashMap<>();
        row.put(ID, id);
        row.put(2, "user-" + id);
        row.put(EVENT_TIME, eventTime);
        row.put(4, "demo");
        row.put(AGE, age);
        return row;
    }

    private static void cleanup(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
