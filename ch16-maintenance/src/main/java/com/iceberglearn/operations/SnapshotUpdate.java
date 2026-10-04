package com.iceberglearn.operations;

import com.iceberglearn.SnapshotIdGeneratorUtil;
import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.manifests.ManifestContent;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestFiles;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.DeleteFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotLogEntry;
import com.iceberglearn.metadata.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Drives the read-compute-commit cycle for an update that publishes a new snapshot.
 *
 * <p>Subclasses implement {@link #apply(TableMetadata)} to describe how the next metadata
 * follows from a given base. This class owns two things:
 * <ul>
 *   <li>the optimistic-concurrency retry loop — read current metadata, hand it to
 *       {@code apply}, attempt the commit, and on conflict wait with exponential backoff
 *       before re-reading and re-applying against the new base;</li>
 *   <li>the shared write primitives every snapshot-producing update needs — fresh ids,
 *       the current manifest list, JSON file writing, manifest descriptor construction,
 *       and the {@link #appendSnapshot(TableMetadata, Snapshot)} tail that turns a new
 *       snapshot into the next {@link TableMetadata}.</li>
 * </ul>
 *
 * <p>Shared base for append, overwrite, delete, and row-delta.
 */
public abstract class SnapshotUpdate {
    // Defaults match typical table property values for retry behaviour.
    static final int DEFAULT_MAX_RETRIES = 4;
    static final long DEFAULT_MIN_RETRY_WAIT_MS = 100;
    static final long DEFAULT_MAX_RETRY_WAIT_MS = 60_000;
    static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final TableOperations operations;
    private final String operationName;

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private long minRetryWaitMs = DEFAULT_MIN_RETRY_WAIT_MS;
    private long maxRetryWaitMs = DEFAULT_MAX_RETRY_WAIT_MS;
    private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;

    protected SnapshotUpdate(TableOperations operations, String operationName) {
        this.operations = operations;
        this.operationName = operationName;
    }

    /** Compute the metadata that should follow {@code base} when this update is applied. */
    protected abstract TableMetadata apply(TableMetadata base);

    protected final TableOperations operations() {
        return operations;
    }

    /** Public entry point used by callers such as {@code AppendFiles.commit()}. */
    public void commit() {
        TableMetadata base = operations.current();
        TableMetadata next = apply(base);

        int attempt = 0;
        long waitMs = minRetryWaitMs;
        while (true) {
            attempt++;
            try {
                operations.commit(base, next);
                System.out.printf("  [%s] commit succeeded on attempt %d%n", operationName, attempt);
                return;
            } catch (CommitFailedException conflict) {
                if (attempt >= maxRetries) {
                    throw new CommitFailedException(
                            conflict,
                            "Cannot commit %s after %d attempts",
                            operationName, attempt);
                }
                System.out.printf("  [%s] conflict on attempt %d, retrying in %dms%n",
                        operationName, attempt, waitMs);
                sleep(waitMs);
                waitMs = (long) Math.min(waitMs * backoffMultiplier, maxRetryWaitMs);

                // Re-read the latest base and re-apply the update against it. Re-applying — not
                // replaying the previous next — is what keeps a concurrent writer's commit alive:
                // the new next is built on top of whatever landed on disk while we were waiting.
                base = operations.refresh();
                next = apply(base);
            }
        }
    }

    // ---------- shared write primitives ----------

    /** Fresh, collision-free snapshot id (UUID-derived), independent of the sequence number. */
    protected long freshSnapshotId() {
        return SnapshotIdGeneratorUtil.generateSnapshotID();
    }

    /** Sequence number for the new snapshot: one past the base's last. */
    protected long nextSequenceNumber(TableMetadata base) {
        return base.lastSequenceNumber() + 1;
    }

    /** The current snapshot's manifest list, or an empty list on the first commit. */
    protected List<ManifestFile> currentManifests(TableMetadata base) {
        Snapshot current = base.currentSnapshot();
        if (current == null) {
            return new ArrayList<>();
        }
        try {
            ManifestList manifestList = ManifestFiles.readManifestList(
                    base.location(), current.manifestListLocation());
            return new ArrayList<>(manifestList.manifests());
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read manifests for snapshot " + current.snapshotId(), e);
        }
    }

    /**
     * Write a JSON object to {@code tableLocation/relativePath} as a brand-new file.
     *
     * <p>{@code CREATE_NEW} never overwrites: a retry that re-runs {@code apply()} generates
     * a fresh UUID-bearing path, so an earlier attempt's orphaned file simply stays on disk
     * unreferenced — correctness is preserved, and a maintenance pass reclaims the orphan.
     */
    protected void writeJson(String tableLocation, String relativePath, Object value) {
        Path path = Path.of(tableLocation).resolve(relativePath);
        try {
            Files.createDirectories(path.getParent());
            try (OutputStream output = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)) {
                OBJECT_MAPPER.writeValue(output, value);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write " + path, e);
        }
    }

    /**
     * Write a DATA manifest holding {@code entries} and return its descriptor. The per-status
     * counts (added/existing/deleted files and rows) are derived from the entries so callers
     * never have to tally them by hand — append passes only ADDED, delete passes only DELETED,
     * overwrite passes a mix.
     */
    protected ManifestFile writeDataManifest(
            TableMetadata base, List<ManifestEntry> entries, long snapshotId, long sequenceNumber) {
        String location = "metadata/manifest-" + snapshotId + "-" + UUID.randomUUID() + ".json";
        writeJson(base.location(), location, new ManifestEntries(entries));
        System.out.println("  [manifest:data] write " + location
                + " entries=" + entries.size());
        return manifestDescriptor(base, location, ManifestContent.DATA, snapshotId, sequenceNumber,
                countByStatus(entries, ManifestEntry.Status.ADDED),
                countByStatus(entries, ManifestEntry.Status.EXISTING),
                countByStatus(entries, ManifestEntry.Status.DELETED),
                rowsByStatus(entries, ManifestEntry.Status.ADDED),
                rowsByStatus(entries, ManifestEntry.Status.EXISTING),
                rowsByStatus(entries, ManifestEntry.Status.DELETED));
    }

    /**
     * Write a DELETES manifest holding {@code entries} (each carrying a {@link DeleteFile}) and
     * return its descriptor. The shape mirrors {@link #writeDataManifest}; only the content type
     * and the entry/envelope type differ.
     */
    protected com.iceberglearn.manifests.ManifestFile writeDeleteManifest(
            TableMetadata base,
            List<com.iceberglearn.manifests.DeleteManifestEntry> entries,
            long snapshotId, long sequenceNumber) {
        String location = "metadata/manifest-deletes-" + snapshotId + "-" + UUID.randomUUID() + ".json";
        writeJson(base.location(), location, new com.iceberglearn.manifests.DeleteManifestEntries(entries));
        System.out.println("  [manifest:deletes] write " + location
                + " entries=" + entries.size());
        int added = 0;
        long addedRows = 0;
        for (com.iceberglearn.manifests.DeleteManifestEntry e : entries) {
            if (e.status() == ManifestEntry.Status.ADDED) {
                added++;
                addedRows += e.deleteFile().recordCount();
            }
        }
        return manifestDescriptor(base, location, ManifestContent.DELETES, snapshotId, sequenceNumber,
                added, 0, 0, addedRows, 0L, 0L);
    }

    /** Build a {@link ManifestFile} descriptor from already-written manifest bytes. */
    private com.iceberglearn.manifests.ManifestFile manifestDescriptor(
            TableMetadata base, String location, ManifestContent content,
            long snapshotId, long sequenceNumber,
            int added, int existing, int deleted, long addedRows, long existingRows, long deletedRows) {
        long length;
        try {
            length = Files.size(Path.of(base.location()).resolve(location));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to inspect manifest " + location, e);
        }
        return com.iceberglearn.manifests.ManifestFile.builder(location)
                .length(length)
                .partitionSpecId(base.defaultSpecId())
                .content(content)
                .sequenceNumber(sequenceNumber)
                .minSequenceNumber(sequenceNumber)
                .snapshotId(snapshotId)
                .addedFilesCount(added)
                .existingFilesCount(existing)
                .deletedFilesCount(deleted)
                .addedRowsCount(addedRows)
                .existingRowsCount(existingRows)
                .deletedRowsCount(deletedRows)
                .build();
    }

    private static int countByStatus(List<ManifestEntry> entries, ManifestEntry.Status status) {
        int n = 0;
        for (ManifestEntry e : entries) {
            if (e.status() == status) n++;
        }
        return n;
    }

    private static long rowsByStatus(List<ManifestEntry> entries, ManifestEntry.Status status) {
        long rows = 0;
        for (ManifestEntry e : entries) {
            if (e.status() == status) rows += e.dataFile().recordCount();
        }
        return rows;
    }

    /** Build the next {@link TableMetadata} that appends {@code snapshot} to {@code base}. */
    protected TableMetadata appendSnapshot(TableMetadata base, Snapshot snapshot) {
        List<Snapshot> snapshots = new ArrayList<>(base.snapshots());
        snapshots.add(snapshot);
        List<SnapshotLogEntry> snapshotLog = new ArrayList<>(base.snapshotLog());
        snapshotLog.add(new SnapshotLogEntry(snapshot.timestampMillis(), snapshot.snapshotId()));
        return new TableMetadata(
                base.formatVersion(),
                base.tableUuid(),
                base.location(),
                snapshot.sequenceNumber(),
                base.currentSchemaId(),
                base.schemas(),
                base.defaultSpecId(),
                base.partitionSpecs(),
                snapshot.snapshotId(),
                snapshots,
                snapshotLog);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommitFailedException(e);
        }
    }
}
