package com.iceberglearn.table;

import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.SnapshotLogEntry;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.operations.AppendFiles;
import com.iceberglearn.operations.MergeAppend;
import com.iceberglearn.operations.TableOperations;
import com.iceberglearn.scan.DataTableScan;
import com.iceberglearn.scan.TableScan;
import com.iceberglearn.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/** A logical table backed by TableOperations. */
public class Table {
    private final TableIdentifier identifier;
    private final TableOperations operations;

    public Table(TableIdentifier identifier, TableOperations operations) {
        this.identifier = identifier;
        this.operations = operations;
    }

    public String name() {
        return identifier.toString();
    }

    public TableIdentifier identifier() {
        return identifier;
    }

    public TableMetadata metadata() {
        return operations.current();
    }

    public TableMetadata refresh() {
        return operations.refresh();
    }

    public Schema schema() {
        return metadata().currentSchema();
    }

    public Snapshot currentSnapshot() {
        return metadata().currentSnapshot();
    }

    public Snapshot snapshot(long snapshotId) {
        return metadata().snapshot(snapshotId);
    }

    public Iterable<Snapshot> snapshots() {
        return metadata().snapshots();
    }

    public List<SnapshotLogEntry> history() {
        return metadata().snapshotLog();
    }

    public TableScan newScan() {
        return new DataTableScan(this, null);
    }

    public AppendFiles newAppend() {
        return new MergeAppend(operations);
    }

    /** Atomically make an existing snapshot the current table state. */
    public void rollbackTo(long snapshotId) {
        TableMetadata base = metadata();
        if (base.snapshot(snapshotId) == null) {
            throw new IllegalArgumentException("Cannot find snapshot with ID " + snapshotId);
        }
        if (base.currentSnapshotId() != null && base.currentSnapshotId() == snapshotId) {
            return;
        }

        List<SnapshotLogEntry> snapshotLog = new ArrayList<>(base.snapshotLog());
        snapshotLog.add(new SnapshotLogEntry(System.currentTimeMillis(), snapshotId));
        TableMetadata next = new TableMetadata(
                base.formatVersion(),
                base.tableUuid(),
                base.location(),
                base.lastSequenceNumber(),
                base.currentSchemaId(),
                base.schemas(),
                base.defaultSpecId(),
                base.partitionSpecs(),
                snapshotId,
                base.snapshots(),
                snapshotLog);
        operations.commit(base, next);
    }

    public String location() {
        return metadata().location();
    }

    public String metadataFileLocation() {
        return operations.currentMetadataLocation();
    }

    public TableOperations operations() {
        return operations;
    }
}
