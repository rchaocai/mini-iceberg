package com.iceberglearn.operations;

import com.iceberglearn.SnapshotIdGeneratorUtil;
import com.iceberglearn.manifests.ManifestEntries;
import com.iceberglearn.manifests.ManifestEntry;
import com.iceberglearn.manifests.ManifestFile;
import com.iceberglearn.manifests.ManifestList;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * AppendFiles implementation that prepares a snapshot before committing table metadata.
 *
 * <p>Extends {@link SnapshotUpdate} so the read-compute-commit cycle is retried with
 * optimistic concurrency: when a concurrent writer publishes first, this append re-reads
 * the new base and rebuilds the manifest and manifest-list against it. The shared write
 * primitives ({@link #currentManifests}, {@link #writeJson}, {@link #appendSnapshot})
 * live on the base class; this subclass only describes what {@link #apply} does for an
 * append — one ADDED-only DATA manifest.
 */
public class MergeAppend extends SnapshotUpdate implements AppendFiles {
    private final List<DataFile> files = new ArrayList<>();

    public MergeAppend(TableOperations operations) {
        super(operations, "append");
    }

    @Override
    public AppendFiles appendFile(DataFile file) {
        files.add(file);
        return this;
    }

    @Override
    public void commit() {
        if (files.isEmpty()) {
            return;
        }
        // Delegate to SnapshotUpdate.commit(), which owns the OCC retry loop. On every retry
        // it will call apply() again with a freshly refreshed base, so the manifest and
        // manifest-list are rebuilt against the latest known state.
        super.commit();
    }

    @Override
    protected TableMetadata apply(TableMetadata base) {
        long snapshotId = freshSnapshotId();
        long sequenceNumber = nextSequenceNumber(base);

        // One DATA manifest, ADDED entries only. The shared writeDataManifest helper
        // derives the per-status counts from the entries, so the descriptor carries
        // addedFilesCount = files.size() without the caller tallying it by hand.
        List<ManifestEntry> entries = files.stream()
                .map(file -> new ManifestEntry(ManifestEntry.Status.ADDED, snapshotId, file))
                .toList();
        ManifestFile manifest = writeDataManifest(base, entries, snapshotId, sequenceNumber);
        System.out.println("  [append] write manifest: " + manifest.path());

        List<ManifestFile> manifests = currentManifests(base);
        manifests.add(manifest);
        String manifestListLocation =
                "metadata/snap-" + snapshotId + "-" + UUID.randomUUID() + ".manifest-list.json";
        writeJson(base.location(), manifestListLocation, new ManifestList(manifests));
        System.out.println("  [append] write manifest list: " + manifestListLocation);

        Snapshot snapshot = Snapshot.builder(snapshotId)
                .parentId(base.currentSnapshotId())
                .timestampMillis(System.currentTimeMillis())
                .manifestListLocation(manifestListLocation)
                .sequenceNumber(sequenceNumber)
                .operation("append")
                .summary(Map.of(
                        "added-data-files", Integer.toString(files.size()),
                        "added-records", Long.toString(
                                files.stream().mapToLong(DataFile::recordCount).sum())))
                .build();
        System.out.println("  [append] build snapshot: " + snapshotId);

        return appendSnapshot(base, snapshot);
    }
}
