package com.iceberglearn.scan;

import com.iceberglearn.manifests.*;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotUtil;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** TableScan implementation that plans data files from one snapshot. */
public class DataTableScan implements TableScan {
    private final Table table;
    private final Long snapshotId;

    public DataTableScan(Table table, Long snapshotId) {
        this.table = table;
        this.snapshotId = snapshotId;
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
        return new DataTableScan(table, scanSnapshotId);
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
    public Snapshot snapshot() {
        return snapshotId != null ? table.snapshot(snapshotId) : table.currentSnapshot();
    }

    @Override
    public List<DataFile> planFiles() {
        Snapshot scanSnapshot = snapshot();
        if (scanSnapshot == null) {
            return List.of();
        }

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
