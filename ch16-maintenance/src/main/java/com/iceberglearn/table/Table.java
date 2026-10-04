package com.iceberglearn.table;

import com.iceberglearn.actions.BaseExpireSnapshots;
import com.iceberglearn.actions.BaseRemoveOrphanFiles;
import com.iceberglearn.actions.BaseRewriteDataFiles;
import com.iceberglearn.actions.ExpireSnapshots;
import com.iceberglearn.actions.RemoveOrphanFiles;
import com.iceberglearn.actions.RewriteDataFiles;
import com.iceberglearn.catalog.*;
import com.iceberglearn.metadata.*;
import com.iceberglearn.operations.*;
import com.iceberglearn.partition.PartitionSpec;
import com.iceberglearn.scan.DataTableScan;
import com.iceberglearn.scan.TableScan;
import com.iceberglearn.schema.*;

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

    /** The spec new writes use — the table's current default. */
    public PartitionSpec spec() {
        return metadata().defaultSpec();
    }

    /**
     * Entry point for partition spec evolution: stage add/remove/rename, then commit as a
     * pure metadata update (no new snapshot, no data file rewritten).
     */
    public UpdatePartitionSpec updatePartitionSpec() {
        return new BaseUpdatePartitionSpec(operations);
    }

    /**
     * Entry point for schema evolution: stage add/rename/delete, then commit as a pure
     * metadata update (no new snapshot, no data file rewritten).
     */
    public UpdateSchema updateSchema() {
        return new BaseUpdateSchema(operations);
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

    /**
     * Entry point for overwriting data files: select existing files whose rows all match
     * a row filter (strict match) and mark them deleted, optionally add new files, then
     * commit as one snapshot with {@code operation=overwrite} (or {@code delete}/{@code append}
     * when only one side is present).
     */
    public OverwriteFiles newOverwrite() {
        return new BaseOverwriteFiles(operations);
    }

    /**
     * Entry point for deleting data files: mark files deleted by reference or by strict
     * row-filter match, then commit as one snapshot with {@code operation=delete}. Physical
     * files are left untouched for a maintenance pass to reclaim.
     */
    public DeleteFiles newDelete() {
        return new BaseDeleteFiles(operations);
    }

    /**
     * Entry point for row-level changes: add new {@link DataFile}s of inserted rows and
     * {@link DeleteFile}s encoding rows to remove, then commit as one snapshot. Data files
     * go into a DATA manifest, delete files into a separate DELETES manifest; both are
     * listed in the new snapshot's manifest list.
     */
    public RowDelta newRowDelta() {
        return new BaseRowDelta(operations);
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

    /**
     * Entry point for the rewrite maintenance operation: compact small live data
     * files into fewer, larger files. Rows and their partition values are
     * preserved; only the physical file layout changes. The commit produces a
     * fresh snapshot marked {@code operation=rewrite}.
     */
    public RewriteDataFiles rewriteDataFiles() {
        return new BaseRewriteDataFiles(operations);
    }

    /**
     * Entry point for the expire maintenance operation: drop snapshots older
     * than a given age (subject to {@link ExpireSnapshots#retainLast(int)} and
     * explicit id overrides) and physically delete manifest list, manifest,
     * and data files that are no longer referenced by any retained snapshot.
     */
    public ExpireSnapshots expireSnapshots() {
        return new BaseExpireSnapshots(operations);
    }

    /**
     * Entry point for the orphan-cleanup maintenance operation: scan the
     * table's on-disk location for regular files that are not reachable from
     * any snapshot in the current metadata, and remove the ones older than a
     * safety threshold. Orphan removal is the only maintenance pass that
     * does not produce a commit — orphan files live outside the metadata.
     */
    public RemoveOrphanFiles removeOrphanFiles() {
        return new BaseRemoveOrphanFiles(operations);
    }
}
