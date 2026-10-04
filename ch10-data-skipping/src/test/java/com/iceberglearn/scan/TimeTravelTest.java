package com.iceberglearn.scan;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimeTravelTest {

    @TempDir
    Path warehouse;

    @Test
    void asOfTimeUsesTheLastHistoryEntryAtOrBeforeTheTimestamp() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());

        append(table, "data/a.parquet");
        long firstSnapshotId = table.currentSnapshot().snapshotId();
        long firstTimestamp = table.currentSnapshot().timestampMillis();
        waitUntilAfter(firstTimestamp);
        append(table, "data/b.parquet");
        long secondSnapshotId = table.currentSnapshot().snapshotId();
        long secondTimestamp = table.currentSnapshot().timestampMillis();

        assertEquals(firstSnapshotId, table.newScan().asOfTime(firstTimestamp).snapshot().snapshotId());
        assertEquals(secondSnapshotId, table.newScan().asOfTime(secondTimestamp).snapshot().snapshotId());
        assertEquals(firstSnapshotId, table.newScan().asOfTime((firstTimestamp + secondTimestamp) / 2)
                .snapshot().snapshotId());
    }

    @Test
    void timeTravelReadsTheManifestTreeOfTheSelectedSnapshot() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());

        append(table, "data/a.parquet");
        long firstTimestamp = table.currentSnapshot().timestampMillis();
        waitUntilAfter(firstTimestamp);
        append(table, "data/b.parquet");

        assertEquals(List.of("data/a.parquet"),
                table.newScan().asOfTime(firstTimestamp).planFiles().stream()
                        .map(DataFile::path).toList());
        assertEquals(List.of("data/a.parquet", "data/b.parquet"),
                table.newScan().planFiles().stream().map(DataFile::path).toList());
    }

    @Test
    void aBoundScanCannotBeChangedAndUnknownTimesAreRejected() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());
        append(table, "data/a.parquet");
        long firstSnapshotId = table.currentSnapshot().snapshotId();
        long timestamp = table.currentSnapshot().timestampMillis();

        TableScan bound = table.newScan().useSnapshot(firstSnapshotId);
        assertThrows(IllegalArgumentException.class, () -> bound.asOfTime(timestamp));
        assertThrows(IllegalArgumentException.class,
                () -> table.newScan().asOfTime(timestamp - 1));
    }

    @Test
    void asOfTimeFollowsARollbackInsteadOfSnapshotCreationTime() {
        FileSystemCatalog catalog = new FileSystemCatalog(warehouse.toString());
        Table table = catalog.createTable(TableIdentifier.of("events"), Schema.userSchema());

        append(table, "data/a.parquet");
        long firstSnapshotId = table.currentSnapshot().snapshotId();
        waitUntilAfter(table.currentSnapshot().timestampMillis());
        append(table, "data/b.parquet");
        long secondSnapshotId = table.currentSnapshot().snapshotId();
        long secondTimestamp = table.currentSnapshot().timestampMillis();
        waitUntilAfter(secondTimestamp);

        table.rollbackTo(firstSnapshotId);
        long rollbackTimestamp = table.history().get(table.history().size() - 1).timestampMillis();

        assertEquals(firstSnapshotId, table.currentSnapshot().snapshotId());
        assertEquals(2, table.metadata().snapshots().size());
        assertEquals(secondSnapshotId, table.newScan().asOfTime(secondTimestamp).snapshot().snapshotId());
        assertEquals(firstSnapshotId, table.newScan().asOfTime(rollbackTimestamp).snapshot().snapshotId());
        assertEquals(List.of("data/a.parquet"),
                table.newScan().asOfTime(rollbackTimestamp).planFiles().stream()
                        .map(DataFile::path).toList());
    }

    private static void append(Table table, String path) {
        table.newAppend().appendFile(DataFile.builder(path)
                .recordCount(10)
                .lowerBounds(Map.of(1, 1L))
                .upperBounds(Map.of(1, 10L))
                .build()).commit();
    }

    private static void waitUntilAfter(long timestampMillis) {
        while (System.currentTimeMillis() <= timestampMillis) {
            Thread.onSpinWait();
        }
    }
}
