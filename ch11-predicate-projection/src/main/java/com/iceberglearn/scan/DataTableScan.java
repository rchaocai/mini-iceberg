package com.iceberglearn.scan;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.manifests.*;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotUtil;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** TableScan implementation that plans data files from one snapshot. */
public class DataTableScan implements TableScan {
    private final Table table;
    private final Long snapshotId;
    private final Expr filter;

    public DataTableScan(Table table, Long snapshotId) {
        this(table, snapshotId, null);
    }

    public DataTableScan(Table table, Long snapshotId, Expr filter) {
        this.table = table;
        this.snapshotId = snapshotId;
        this.filter = filter;
    }

    @Override
    public Table table() {
        return table;
    }

    @Override
    public TableScan useSnapshot(long scanSnapshotId) {
        if (snapshotId != null) {
            throw new IllegalArgumentException(
                    "Cannot override snapshot, already set snapshot id=" + snapshotId);
        }
        if (table.snapshot(scanSnapshotId) == null) {
            throw new IllegalArgumentException("Cannot find snapshot with ID " + scanSnapshotId);
        }
        return new DataTableScan(table, scanSnapshotId, filter);
    }

    @Override
    public TableScan asOfTime(long timestampMillis) {
        if (snapshotId != null) {
            throw new IllegalArgumentException(
                    "Cannot override snapshot, already set snapshot id=" + snapshotId);
        }
        return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table, timestampMillis));
    }

    @Override
    public TableScan filter(Expr predicate) {
        return new DataTableScan(table, snapshotId, predicate);
    }

    @Override
    public Expr filter() {
        return filter;
    }

    @Override
    public Snapshot snapshot() {
        return snapshotId != null ? table.snapshot(snapshotId) : table.currentSnapshot();
    }

    @Override
    public List<DataFile> planFiles() {
        Snapshot scanSnapshot = snapshot();
        if (scanSnapshot == null) {
            return List.of();
        }

        List<DataFile> dataFiles = collectLiveFiles(scanSnapshot);
        if (filter == null) {
            return dataFiles;
        }

        // The filter is written against source columns. Projection from source columns to
        // partition fields happens inside ManifestGroup.filterRows, exactly where the real
        // Iceberg ManifestGroup runs Projections.inclusive(spec).project(dataFilter).
        PartitionSpec spec = table.metadata().defaultSpec();
        return new ManifestGroup(dataFiles, spec).filterRows(filter).planFiles();
    }

    private List<DataFile> collectLiveFiles(Snapshot scanSnapshot) {
        try {
            ManifestList manifestList = ManifestFiles.readManifestList(
                    table.location(), scanSnapshot.manifestListLocation());
            List<DataFile> dataFiles = new ArrayList<>();
            for (ManifestFile manifest : manifestList.manifests()) {
                ManifestEntries entries = ManifestFiles.readManifest(table.location(), manifest.path());
                for (ManifestEntry entry : entries.entries()) {
                    if (entry.isLive()) {
                        dataFiles.add(entry.dataFile());
                    }
                }
            }
            return List.copyOf(dataFiles);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to plan files for snapshot " + scanSnapshot.snapshotId(), e);
        }
    }
}
