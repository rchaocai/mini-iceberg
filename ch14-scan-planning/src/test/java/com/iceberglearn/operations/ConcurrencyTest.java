package com.iceberglearn.operations;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the optimistic-concurrency machinery added in chapter 7:
 * concurrent appends, the two CAS defense lines, and the retry loop.
 */
class ConcurrencyTest {

    @TempDir
    Path warehouse;

    @Test
    void concurrentAppendsBothLandInTheSnapshotHistory() throws Exception {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        TableIdentifier identifier = TableIdentifier.of("events");
        Table table = catalog.createTable(identifier, Schema.userSchema());
        table.newAppend().appendFile(file("data/a.parquet")).commit();

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread writerA = new Thread(() -> {
            try {
                start.await();
                catalog.loadTable(identifier).newAppend()
                        .appendFile(file("data/b.parquet"))
                        .commit();
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        }, "Writer-A");

        Thread writerB = new Thread(() -> {
            try {
                start.await();
                catalog.loadTable(identifier).newAppend()
                        .appendFile(file("data/c.parquet"))
                        .commit();
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        }, "Writer-B");

        writerA.start();
        writerB.start();
        start.countDown();
        done.await();

        if (error.get() != null) {
            throw new AssertionError("Writer failed", error.get());
        }

        Table reloaded = catalog.loadTable(identifier);
        assertEquals(3, reloaded.metadata().snapshots().size(),
                "baseline + two concurrent appends should produce three snapshots");

        List<String> paths = reloaded.newScan().planFiles().stream()
                .map(DataFile::path)
                .sorted()
                .toList();
        assertEquals(List.of("data/a.parquet", "data/b.parquet", "data/c.parquet"), paths,
                "both concurrent writers' files must survive the race");
    }

    @Test
    void aStaleBaseFailsCasAfterRefresh() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        TableIdentifier identifier = TableIdentifier.of("events");
        Table first = catalog.createTable(identifier, Schema.userSchema());
        first.newAppend().appendFile(file("data/a.parquet")).commit();

        TableMetadata staleBase = first.metadata();

        // Another writer publishes a new version while `first` is not looking.
        catalog.loadTable(identifier).newAppend()
                .appendFile(file("data/b.parquet")).commit();

        // Refresh so the cached current is replaced by a freshly deserialized object.
        first.operations().refresh();

        CommitFailedException ex = assertThrows(
                CommitFailedException.class,
                () -> first.operations().commit(staleBase, withAddedSnapshot(staleBase)));
        assertTrue(ex.getMessage().contains("base != current"),
                "CAS should reject a stale base once the instance has refreshed: " + ex.getMessage());
    }

    @Test
    void anUnrefreshedStaleBaseIsCaughtAtTheRenameLayer() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        TableIdentifier identifier = TableIdentifier.of("events");
        Table unaware = catalog.createTable(identifier, Schema.userSchema());
        unaware.newAppend().appendFile(file("data/a.parquet")).commit();

        TableMetadata staleBase = unaware.metadata();

        // Another writer publishes a new version. `unaware` does NOT refresh, so its
        // cached current still equals staleBase by reference — CAS passes.
        catalog.loadTable(identifier).newAppend()
                .appendFile(file("data/b.parquet")).commit();

        CommitFailedException ex = assertThrows(
                CommitFailedException.class,
                () -> unaware.operations().commit(staleBase, withAddedSnapshot(staleBase)));
        assertTrue(ex.getMessage().contains("already exists"),
                "rename layer should reject the version collision: " + ex.getMessage());
    }

    @Test
    void retryRebuildsNextAgainstTheLatestBaseAndSucceeds() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        TableIdentifier identifier = TableIdentifier.of("events");
        Table table = catalog.createTable(identifier, Schema.userSchema());
        table.newAppend().appendFile(file("data/a.parquet")).commit();

        // Wrap the same table location with operations that fail the first rename with
        // a CommitFailedException, then let subsequent attempts through.
        FailOnceWithConflictTableOperations ops =
                new FailOnceWithConflictTableOperations(Path.of(table.location()));
        Table retrying = new Table(identifier, ops);

        // The first attempt conflicts at rename; SnapshotUpdate should catch it, refresh,
        // re-apply against the (unchanged) base, and succeed on the second attempt.
        retrying.newAppend().appendFile(file("data/b.parquet")).commit();

        Table reloaded = catalog.loadTable(identifier);
        assertEquals(2, reloaded.metadata().snapshots().size(),
                "the retry should have published exactly one new snapshot");
        List<String> paths = reloaded.newScan().planFiles().stream()
                .map(DataFile::path)
                .sorted()
                .toList();
        assertEquals(List.of("data/a.parquet", "data/b.parquet"), paths,
                "the retried append should still contain both files");
    }

    private static DataFile file(String path) {
        return DataFile.builder(path)
                .recordCount(10)
                .lowerBounds(Map.of(1, 1L))
                .upperBounds(Map.of(1, 10L))
                .build();
    }

    /** Build a synthetic next metadata so commit() can be called directly, bypassing the retry loop. */
    private static TableMetadata withAddedSnapshot(TableMetadata base) {
        long snapshotId = base.lastSequenceNumber() + 1;
        Snapshot snapshot = Snapshot.builder(snapshotId)
                .parentId(base.currentSnapshotId())
                .timestampMillis(System.currentTimeMillis())
                .manifestListLocation("metadata/snap-" + snapshotId + "-dummy.manifest-list.json")
                .sequenceNumber(snapshotId)
                .operation("append")
                .summary(Map.of("added-data-files", "1"))
                .build();
        List<Snapshot> snapshots = new ArrayList<>(base.snapshots());
        snapshots.add(snapshot);
        return new TableMetadata(
                base.formatVersion(),
                base.tableUuid(),
                base.location(),
                snapshot.sequenceNumber(),
                base.currentSchemaId(),
                base.schemas(),
                snapshot.snapshotId(),
                snapshots,
                base.snapshotLog());
    }
}
