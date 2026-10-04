package com.iceberglearn.actions;

import com.iceberglearn.SnapshotIdGeneratorUtil;
import com.iceberglearn.expressions.Expr;
import com.iceberglearn.io.ParquetReader;
import com.iceberglearn.io.PartitionedWriter;
import com.iceberglearn.manifests.ManifestContent;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestFiles;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.metrics.MetricsFilter;
import com.iceberglearn.operations.SnapshotUpdate;
import com.iceberglearn.operations.TableOperations;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.schema.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Implementation of {@link RewriteDataFiles} on top of the shared
 * {@link SnapshotUpdate} commit loop.
 *
 * <p>The pipeline:
 * <ol>
 *   <li>Walk the current snapshot's DATA manifests and collect every <em>live</em>
 *       {@link DataFile} whose size is below {@link #targetSizeBytes} and whose
 *       metrics could match the optional rewrite filter (inclusive check).</li>
 *   <li>Group the selected files by their partition tuple. Rewrite never mixes
 *       rows from different output partitions into the same data file.</li>
 *   <li>Read all rows from each group via {@link ParquetReader#read(Schema, Path)}
 *       and re-write them with {@link PartitionedWriter} using the table's
 *       current spec, producing fewer, larger output files. This is the actual
 *       physical compaction.</li>
 *   <li>Build a single DATA manifest with an {@link ManifestEntry.Status#ADDED ADDED}
 *       entry for every merged output file plus a
 *       {@link ManifestEntry.Status#DELETED DELETED} entry for every input file
 *       that was replaced. Manifests untouched by the rewrite are simply carried
 *       forward from the previous manifest list.</li>
 *   <li>Write the manifest list, stamp a fresh snapshot with {@code operation=rewrite},
 *       and hand the result to the standard commit retry loop.</li>
 * </ol>
 *
 * <p>The rewrite does not change the logical row count. Old snapshot versions keep
 * pointing at the original small files, so time travel continues to work until
 * those snapshots are expired and the files are finally reclaimed.
 */
public class BaseRewriteDataFiles extends SnapshotUpdate implements RewriteDataFiles {

    static final long DEFAULT_TARGET_SIZE_BYTES = 128L * 1024L * 1024L; // 128 MB

    private long targetSizeBytes = DEFAULT_TARGET_SIZE_BYTES;
    private Expr filter;
    private int rewrittenCount;
    private int addedCount;
    private long rewrittenBytes;

    public BaseRewriteDataFiles(TableOperations operations) {
        super(operations, "rewrite");
    }

    @Override
    public RewriteDataFiles targetSizeBytes(long value) {
        this.targetSizeBytes = value;
        return this;
    }

    @Override
    public RewriteDataFiles filter(Expr expr) {
        this.filter = expr;
        return this;
    }

    @Override
    public Result execute() {
        rewrittenCount = 0;
        addedCount = 0;
        rewrittenBytes = 0L;
        commit();
        return new Result() {
            @Override public int rewrittenDataFilesCount() { return rewrittenCount; }
            @Override public int addedDataFilesCount() { return addedCount; }
            @Override public long rewrittenBytesCount() { return rewrittenBytes; }
        };
    }

    // ---------- SnapshotUpdate.apply ----------

    @Override
    protected TableMetadata apply(TableMetadata base) {
        Snapshot current = base.currentSnapshot();
        if (current == null) {
            return base;
        }
        ManifestList currentMl;
        try {
            currentMl = ManifestFiles.readManifestList(
                    base.location(), current.manifestListLocation());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read manifest list for rewrite", e);
        }
        Schema schema = base.currentSchema();
        PartitionSpec spec = base.defaultSpec();

        // Pass 1: partition the selected small files by their partition tuple. Rows
        // from two different partitions must never land in the same output file,
        // because the output file's partition tuple would be wrong for one of them.
        Map<List<Object>, List<DataFile>> filesByPartition = new HashMap<>();
        List<ManifestFile> unchangedManifests = new ArrayList<>();
        for (ManifestFile mf : currentMl.manifests()) {
            if (mf.content() != ManifestContent.DATA) {
                // DELETES manifests are carried forward untouched: rewrite only
                // touches data files, not delete files.
                unchangedManifests.add(mf);
                continue;
            }
            ManifestEntries entries;
            try {
                entries = ManifestFiles.readManifest(base.location(), mf.path());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read manifest " + mf.path(), e);
            }

            boolean manifestTouched = false;
            for (ManifestEntry entry : entries.entries()) {
                if (!entry.isLive()) continue;
                DataFile df = entry.dataFile();
                if (df.fileSizeInBytes() < targetSizeBytes
                        && MetricsFilter.canMatch(filter, df)) {
                    filesByPartition.computeIfAbsent(
                            df.partition() == null ? List.of() : df.partition(),
                            k -> new ArrayList<>()).add(df);
                    manifestTouched = true;
                }
            }
            if (!manifestTouched) {
                // No file in this manifest was picked up — reuse the whole manifest
                // descriptor instead of rewriting it. This keeps the manifest tree
                // sparse for partial rewrites.
                unchangedManifests.add(mf);
            }
        }

        if (filesByPartition.isEmpty()) {
            // Nothing to do. Return the base unchanged so the commit short-circuits
            // without creating an empty snapshot.
            return base;
        }

        // Pass 2: read + rewrite each partition group.
        long snapshotId = freshSnapshotId();
        long sequenceNumber = nextSequenceNumber(base);
        Path dataDir = Path.of(base.location()).resolve("data");
        List<ManifestEntry> rewriteEntries = new ArrayList<>();

        for (Map.Entry<List<Object>, List<DataFile>> group : filesByPartition.entrySet()) {
            List<DataFile> inputs = group.getValue();
            rewrittenCount += inputs.size();
            for (DataFile input : inputs) {
                rewrittenBytes += input.fileSizeInBytes();
            }
            // Accumulate all rows across the input files for this partition.
            List<Map<Integer, Object>> rows = new ArrayList<>();
            for (DataFile input : inputs) {
                Path inputPath = resolveDataPath(base.location(), input.path());
                rows.addAll(ParquetReader.read(schema, inputPath));
            }
            // Re-write the rows. PartitionedWriter already writes the correct
            // partition tuple for us, so the output file's metadata is correct.
            // We pick a target row count derived from the average row size so
            // output files land roughly around targetSizeBytes.
            long avgRowBytes = rows.isEmpty() ? 100L : Math.max(1L, rewrittenBytes / Math.max(1, rows.size()));
            int targetRowsPerFile = (int) Math.max(1, targetSizeBytes / avgRowBytes);
            PartitionedWriter writer = new PartitionedWriter(
                    schema, spec, dataDir, targetRowsPerFile);
            for (Map<Integer, Object> row : rows) {
                writer.write(row);
            }
            List<DataFile> outputs = writer.complete();
            addedCount += outputs.size();

            // ADDED entries for the new (merged) files.
            for (DataFile out : outputs) {
                rewriteEntries.add(new ManifestEntry(
                        ManifestEntry.Status.ADDED, snapshotId, out));
            }
            // DELETED entries for every input file.
            for (DataFile input : inputs) {
                rewriteEntries.add(new ManifestEntry(
                        ManifestEntry.Status.DELETED, snapshotId, input));
            }
        }

        System.out.printf("  [rewrite] picked %d files (%d bytes) → %d merged files%n",
                rewrittenCount, rewrittenBytes, addedCount);

        // Pass 3: assemble the new manifest list and stamp the rewrite snapshot.
        ManifestFile rewriteManifest = writeDataManifest(
                base, rewriteEntries, snapshotId, sequenceNumber);
        List<ManifestFile> allManifests = new ArrayList<>(unchangedManifests);
        allManifests.add(rewriteManifest);

        String mlLocation = "metadata/snap-" + snapshotId + "-" + UUID.randomUUID() + ".json";
        writeJson(base.location(), mlLocation, new ManifestList(allManifests));

        long parentId = current.snapshotId();
        Map<String, String> summary = new HashMap<>(current.summary());
        summary.put("operation", "rewrite");
        Snapshot newSnapshot = Snapshot.builder(snapshotId)
                .timestampMillis(System.currentTimeMillis())
                .manifestListLocation(mlLocation)
                .sequenceNumber(sequenceNumber)
                .parentId(parentId)
                .summary(summary)
                .build();
        return appendSnapshot(base, newSnapshot);
    }

    private static Path resolveDataPath(String tableLocation, String dataFilePath) {
        Path path = Path.of(dataFilePath);
        return path.isAbsolute() ? path : Path.of(tableLocation).resolve(path);
    }
}
