package com.iceberglearn.actions;

import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.manifests.DeleteManifestEntries;
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
import com.iceberglearn.operations.TableOperations;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Implementation of {@link ExpireSnapshots}.
 *
 * <p>Expiration is split into four clearly ordered steps:
 * <ol>
 *   <li>Decide which snapshots expire, walking the chain from the current snapshot
 *       backwards and applying the age + retain-last + explicit-ID rules.
 *       The current snapshot is always retained.</li>
 *   <li>Compute two sets of file paths: those reachable only from expired
 *       snapshots, and those still reachable from remaining snapshots. Their
 *       difference — the <em>exclusive</em> files — is what will be deleted.
 *       Files that live in both sets (shared across an expired and a retained
 *       snapshot) must never be removed.</li>
 *   <li>Commit a new {@link TableMetadata} whose snapshot list no longer contains
 *       the expired entries. Only after the commit succeeds does physical
 *       deletion begin; this way, if deletion fails partway the metadata still
 *       describes a correct table, and a re-run can retry cleanup.</li>
 *   <li>Delete the exclusive files. A custom {@link #deleteWith(Consumer)} hook
 *       lets callers collect the files for audit instead of physically removing
 *       them.</li>
 * </ol>
 *
 * <p>Files are counted in three categories in the result: data files, manifests
 * (both DATA and DELETES kinds), and manifest lists. The manifest-list category
 * can never overlap with the data-file category because the two use different
 * prefixes, so double-counting is impossible.
 */
public class BaseExpireSnapshots implements ExpireSnapshots {

    /** Default maximum snapshot age: snapshots older than this are expired. */
    static final long DEFAULT_MAX_SNAPSHOT_AGE_MS = 5L * 24L * 60L * 60L * 1000L; // 5 days
    /** Default minimum snapshots to retain regardless of age. */
    static final int DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 1;

    private final TableOperations operations;

    private long olderThanMillis;
    private int retainLast = DEFAULT_MIN_SNAPSHOTS_TO_KEEP;
    private final Set<Long> explicitExpireIds = new HashSet<>();
    private Consumer<String> deleteFunc;

    public BaseExpireSnapshots(TableOperations operations) {
        this.operations = operations;
        // Mirror the official RemoveSnapshots constructor: a default age
        // threshold so that snapshots older than 5 days are expired even when
        // the caller does not call expireOlderThan explicitly. retainLast
        // defaults to 1.
        long now = System.currentTimeMillis();
        this.olderThanMillis = now - DEFAULT_MAX_SNAPSHOT_AGE_MS;
    }

    @Override
    public ExpireSnapshots expireOlderThan(long timestampMillis) {
        this.olderThanMillis = timestampMillis;
        return this;
    }

    @Override
    public ExpireSnapshots retainLast(int numSnapshots) {
        if (numSnapshots < 1) {
            throw new IllegalArgumentException("Must retain at least one snapshot");
        }
        this.retainLast = numSnapshots;
        return this;
    }

    @Override
    public ExpireSnapshots expireSnapshotId(long snapshotId) {
        explicitExpireIds.add(snapshotId);
        return this;
    }

    @Override
    public ExpireSnapshots deleteWith(Consumer<String> func) {
        this.deleteFunc = func;
        return this;
    }

    @Override
    public Result execute() {
        Consumer<String> deleter = deleteFunc != null ? deleteFunc : this::deleteFile;
        TableMetadata base = operations.refresh();
        Long currentId = base.currentSnapshotId();

        // --- Step 1: select expired and retained snapshot ids ----------------
        // Walk ancestors from the current snapshot backwards. An ancestor is
        // retained if (a) we haven't collected retainLast yet, OR (b) it is
        // newer than the age threshold. This is the same OR condition used by
        // the official RemoveSnapshots.computeBranchSnapshotsToRetain: the
        // first ancestor that fails both conditions terminates the walk,
        // because older ancestors will also fail.
        List<Long> ancestorChain = ancestorChain(base);
        Set<Long> retainedIds = new HashSet<>();
        for (long id : ancestorChain) {
            Snapshot snap = base.snapshot(id);
            if (retainedIds.size() < retainLast
                    || (snap != null && snap.timestampMillis() >= olderThanMillis)) {
                retainedIds.add(id);
            } else {
                break; // first ancestor that fails both → stop
            }
        }
        // Explicitly-expired ids are removed regardless of age / retain-last,
        // *except* the current snapshot which can never be expired.
        Set<Long> expiredIds = new HashSet<>();
        for (Snapshot snap : base.snapshots()) {
            long id = snap.snapshotId();
            boolean expired = explicitExpireIds.contains(id)
                    || !retainedIds.contains(id);
            if (expired && (currentId == null || id != currentId)) {
                expiredIds.add(id);
            }
        }
        System.out.printf("  [expire] snapshots: total=%d retained=%d expired=%d%n",
                base.snapshots().size(),
                base.snapshots().size() - expiredIds.size(),
                expiredIds.size());
        if (expiredIds.isEmpty()) {
            return emptyResult();
        }

        // --- Step 2: reachable file sets (expired-only vs remaining) ---------
        Set<String> expiredReachable = reachableFiles(base, expiredIds);
        Set<Long> remainingIds = new HashSet<>();
        for (Snapshot snap : base.snapshots()) {
            if (!expiredIds.contains(snap.snapshotId())) {
                remainingIds.add(snap.snapshotId());
            }
        }
        Set<String> remainingReachable = reachableFiles(base, remainingIds);

        List<String> toDelete = new ArrayList<>();
        for (String f : expiredReachable) {
            if (!remainingReachable.contains(f)) {
                toDelete.add(f);
            }
        }

        // Categorize for the result counters. The path prefixes are the same
        // written by SnapshotUpdate.writeDataManifest / writeDeleteManifest /
        // the manifest-list writer inside appendSnapshot.
        long deletedData = 0, deletedManifests = 0, deletedManifestLists = 0;
        for (String f : toDelete) {
            String name = Path.of(f).getFileName().toString();
            if (name.startsWith("snap-")) {
                deletedManifestLists++;
            } else if (name.startsWith("manifest-")) {
                deletedManifests++;
            } else if (name.endsWith(".parquet")) {
                deletedData++;
            } else {
                // Unknown prefix: conservatively count it as a manifest to avoid
                // under-reporting. The categorization is informational only.
                deletedManifests++;
            }
        }
        long finalDeletedData = deletedData;
        long finalDeletedManifests = deletedManifests;
        long finalDeletedManifestLists = deletedManifestLists;

        // --- Step 3: commit new metadata without expired snapshots ----------
        List<Snapshot> keptSnapshots = new ArrayList<>();
        for (Snapshot snap : base.snapshots()) {
            if (!expiredIds.contains(snap.snapshotId())) {
                keptSnapshots.add(snap);
            }
        }
        // Remove entries in the snapshot log that reference expired ids. The
        // log is append-only in general; expiration is the one operation that
        // trims it so history stays consistent with what's retained.
        List<SnapshotLogEntry> keptLog = new ArrayList<>();
        for (SnapshotLogEntry entry : base.snapshotLog()) {
            if (!expiredIds.contains(entry.snapshotId())) {
                keptLog.add(entry);
            }
        }
        TableMetadata next = new TableMetadata(
                base.formatVersion(),
                base.tableUuid(),
                base.location(),
                base.lastSequenceNumber(),
                base.currentSchemaId(),
                base.schemas(),
                base.defaultSpecId(),
                base.partitionSpecs(),
                base.currentSnapshotId(),
                keptSnapshots,
                keptLog);
        try {
            operations.commit(base, next);
            System.out.println("  [expire] metadata commit OK, expiring " + toDelete.size() + " files");
        } catch (CommitFailedException e) {
            // Commit conflict — do not touch any files. The caller will retry.
            throw e;
        }

        // --- Step 4: physically delete exclusive files ---------------------
        // Delete paths are resolved against the table location. Files written
        // via SnapshotUpdate.writeJson store relative paths (e.g. "metadata/..."),
        // and data files written via PartitionedWriter also use relative paths
        // ("data/...").
        for (String relative : toDelete) {
            Path resolved = resolve(base.location(), relative);
            deleter.accept(resolved.toString());
        }

        return new Result() {
            @Override public long deletedDataFilesCount() { return finalDeletedData; }
            @Override public long deletedManifestsCount() { return finalDeletedManifests; }
            @Override public long deletedManifestListsCount() { return finalDeletedManifestLists; }
        };
    }

    // ---------- helpers -----------------------------------------------------

    /**
     * Return the ancestor-id chain starting at the current snapshot and walking
     * parent pointers until the first commit. Order is newest → oldest.
     */
    private static List<Long> ancestorChain(TableMetadata base) {
        List<Long> chain = new ArrayList<>();
        Long id = base.currentSnapshotId();
        while (id != null) {
            chain.add(id);
            Snapshot snap = base.snapshot(id);
            id = snap != null ? snap.parentId() : null;
        }
        return chain;
    }

    /**
     * Return every file path reachable from the given snapshot ids: manifest
     * list files, manifest files (DATA + DELETES), data files inside DATA
     * manifests, and delete files inside DELETES manifests.
     */
    private static Set<String> reachableFiles(TableMetadata base, Set<Long> snapshotIds) {
        Set<String> result = new HashSet<>();
        for (long id : snapshotIds) {
            Snapshot snap = base.snapshot(id);
            if (snap == null) continue;
            result.add(snap.manifestListLocation());
            ManifestList ml;
            try {
                ml = ManifestFiles.readManifestList(
                        base.location(), snap.manifestListLocation());
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to read manifest list " + snap.manifestListLocation(), e);
            }
            for (ManifestFile mf : ml.manifests()) {
                result.add(mf.path());
                if (mf.content() == ManifestContent.DATA) {
                    ManifestEntries entries;
                    try {
                        entries = ManifestFiles.readManifest(base.location(), mf.path());
                    } catch (IOException e) {
                        throw new UncheckedIOException(
                                "Failed to read manifest " + mf.path(), e);
                    }
                    for (ManifestEntry entry : entries.entries()) {
                        DataFile df = entry.dataFile();
                        if (df != null) result.add(df.path());
                    }
                } else if (mf.content() == ManifestContent.DELETES) {
                    DeleteManifestEntries entries;
                    try {
                        entries = ManifestFiles.readDeleteManifest(
                                base.location(), mf.path());
                    } catch (IOException e) {
                        throw new UncheckedIOException(
                                "Failed to read delete manifest " + mf.path(), e);
                    }
                    for (var entry : entries.entries()) {
                        DeleteFile df = entry.deleteFile();
                        if (df != null) result.add(df.path());
                    }
                }
            }
        }
        return result;
    }

    private static Result emptyResult() {
        return new Result() {
            @Override public long deletedDataFilesCount() { return 0; }
            @Override public long deletedManifestsCount() { return 0; }
            @Override public long deletedManifestListsCount() { return 0; }
        };
    }

    private static Path resolve(String tableLocation, String file) {
        Path p = Path.of(file);
        return p.isAbsolute() ? p : Path.of(tableLocation).resolve(p);
    }

    private void deleteFile(String path) {
        try {
            Files.deleteIfExists(Path.of(path));
        } catch (IOException e) {
            // Surface as unchecked; individual delete failures are rare and the
            // file can be cleaned up by a later maintenance run.
            throw new UncheckedIOException("Failed to delete " + path, e);
        }
    }
}
