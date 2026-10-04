package com.iceberglearn;

import com.iceberglearn.catalog.FileSystemCatalog;
import com.iceberglearn.catalog.TableIdentifier;
import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.io.UnpartitionedWriter;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metadata.Snapshot;
import com.iceberglearn.metadata.TableMetadata;
import com.iceberglearn.schema.Schema;
import com.iceberglearn.table.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Chapter 8: optimistic concurrency — two writers race to commit, the loser retries. */
public class Main {
    private static final String WAREHOUSE = "../data/ch08/warehouse";

    public static void main(String[] args) throws Exception {
        Path warehouse = Path.of(WAREHOUSE).toAbsolutePath().normalize();
        cleanup(warehouse);

        FileSystemCatalog catalog = new FileSystemCatalog(WAREHOUSE);
        TableIdentifier identifier = TableIdentifier.of("analytics", "users");
        Table table = catalog.createTable(identifier, Schema.userSchema());
        Path dataDir = Path.of(table.location()).resolve("data");

        System.out.println("[步骤1] 写入初始快照（基线 v1）");
        append(table, writeDataFile(Schema.userSchema(), dataDir, 0));
        System.out.println("  当前 snapshotId = " + table.currentSnapshot().snapshotId());
        System.out.println("  当前 lastSequenceNumber = " + table.metadata().lastSequenceNumber());

        System.out.println("\n[步骤2] 启动两个并发 writer，同时基于 v1 提交");
        System.out.println("  Writer-A: 添加一个新数据文件");
        System.out.println("  Writer-B: 添加另一个新数据文件");

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);

        Thread writerA = new Thread(() -> {
            try {
                startLatch.await();
                Table local = catalog.loadTable(identifier);
                local.newAppend()
                        .appendFile(writeDataFile(Schema.userSchema(), dataDir, 1))
                        .commit();
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        }, "Writer-A");

        Thread writerB = new Thread(() -> {
            try {
                startLatch.await();
                Table local = catalog.loadTable(identifier);
                local.newAppend()
                        .appendFile(writeDataFile(Schema.userSchema(), dataDir, 2))
                        .commit();
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        }, "Writer-B");

        writerA.start();
        writerB.start();
        System.out.println("  ⚡ 释放信号，两个 writer 同时开始");
        startLatch.countDown();
        doneLatch.await();
        writerA.join();
        writerB.join();

        System.out.println("\n[步骤3] 验证最终状态");
        Table after = catalog.loadTable(identifier);
        System.out.println("  快照数量: " + after.metadata().snapshots().size());
        System.out.println("  当前 snapshotId = " + after.currentSnapshot().snapshotId());
        System.out.println("  当前 lastSequenceNumber = " + after.metadata().lastSequenceNumber());
        System.out.println("  快照历史:");
        for (Snapshot snapshot : after.metadata().snapshots()) {
            Long parent = snapshot.parentId();
            System.out.printf("    - snapshotId=%d, parentId=%s, operation=%s%n",
                    snapshot.snapshotId(), parent, snapshot.operation());
        }
        List<String> paths = after.newScan().planFiles().stream().map(DataFile::path).toList();
        System.out.println("  当前扫描读取的文件: " + paths);

        System.out.println("\n[步骤4] 演示直接用过期 base 调 commit 触发 CAS 失败（不重试）");
        Table staleTable = catalog.loadTable(identifier);
        TableMetadata staleBase = staleTable.metadata();
        System.out.println("  读到的 staleBase snapshotId = " + staleBase.currentSnapshotId());

        // 让别的 writer 抢先提交一版，使 staleBase 真正过期。
        append(catalog.loadTable(identifier), writeDataFile(Schema.userSchema(), dataDir, 3));
        System.out.println("  另一个 writer 已提交，当前 snapshotId = "
                + catalog.loadTable(identifier).currentSnapshot().snapshotId());

        // 显式刷新 staleTable 的 operations，让它"知道"磁盘上已经有新版本。
        // 这会让 currentMetadata 变成新对象，CAS 检查 base != current 立即失败。
        staleTable.operations().refresh();
        try {
            staleTable.operations().commit(staleBase, withAddedSnapshot(staleBase));
            System.out.println("  ✗ 不应该到达这里");
        } catch (CommitFailedException e) {
            System.out.println("  ✓ CAS 检查失败: " + e.getMessage());
        }

        System.out.println("\n[步骤5] 演示不刷新直接用过期 base 提交（rename 层拦截）");
        Table unaware = catalog.loadTable(identifier);
        TableMetadata unawareBase = unaware.metadata();
        System.out.println("  读到的 unawareBase snapshotId = " + unawareBase.currentSnapshotId());

        // 再让别的 writer 抢先提交一版。这次 unaware 的 operations 实例没刷新，
        // 所以 CAS 检查会通过（base == cached current），但 rename 时发现文件已存在。
        append(catalog.loadTable(identifier), writeDataFile(Schema.userSchema(), dataDir, 4));
        System.out.println("  另一个 writer 已提交，当前 snapshotId = "
                + catalog.loadTable(identifier).currentSnapshot().snapshotId());

        try {
            unaware.operations().commit(unawareBase, withAddedSnapshot(unawareBase));
            System.out.println("  ✗ 不应该到达这里");
        } catch (CommitFailedException e) {
            System.out.println("  ✓ rename 层拦截: " + e.getMessage());
        }

        System.out.println("\n结果：乐观并发不靠锁，靠两层防线 + 重试。冲突是正常事件，不是错误。");
    }

    private static void append(Table table, DataFile file) {
        table.newAppend().appendFile(file).commit();
    }

    private static DataFile writeDataFile(Schema schema, Path dataDir, int fileIndex) {
        UnpartitionedWriter writer = new UnpartitionedWriter(schema, dataDir, 100);
        for (int row = 0; row < 100; row++) {
            long id = fileIndex * 100L + row + 1;
            Map<Integer, Object> r = new LinkedHashMap<>();
            r.put(1, id);
            r.put(2, "user_" + id);
            r.put(3, 20 + (int) (id % 40));
            r.put(4, "user" + id + "@example.com");
            writer.write(r);
        }
        return writer.complete().get(0);
    }

    // Build a synthetic next metadata so we can call commit() directly without going through
    // SnapshotUpdate's retry loop. This lets us demonstrate the CAS failure in isolation.
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
        List<Snapshot> snapshots = new java.util.ArrayList<>(base.snapshots());
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

    private static void cleanup(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
