package com.iceberglearn.manifests;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metrics.DataFileStats;
import com.iceberglearn.metrics.MetricsFilter;
import com.iceberglearn.partition.PartitionField;
import com.iceberglearn.partition.PartitionSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Two-stage file planner: partition pruning then metrics pruning.
 *
 * <p>Both stages are <em>metadata-only</em> — neither opens a data file. Partition pruning uses
 * a {@link PartitionSpec} + a partition predicate to discard whole
 * partitions; metrics pruning uses {@link MetricsFilter} + a data predicate to discard
 * individual files whose {@code [lower, upper]} ranges cannot satisfy the predicate.
 *
 * <p>The two filters are independent: callers may set either, both, or neither. When a filter
 * is {@code null} (or the spec is unpartitioned) that stage is skipped — which is why a
 * metrics-only scan still prints {@code N → N → K} with the first arrow a no-op.
 */
public class ManifestGroup {

    private final List<DataFile> files;
    private final PartitionSpec partitionSpec;

    private Expr partitionFilter = null;
    private Expr metricsFilter = null;

    public ManifestGroup(List<DataFile> files, PartitionSpec partitionSpec) {
        this.files = files;
        this.partitionSpec = partitionSpec;
    }

    /** Set the partition predicate (used by stage 1). Returns {@code this} for chaining. */
    public ManifestGroup filterPartitions(Expr partitionFilter) {
        this.partitionFilter = partitionFilter;
        return this;
    }

    /** Set the data predicate (used by stage 2). Returns {@code this} for chaining. */
    public ManifestGroup filterData(Expr metricsFilter) {
        this.metricsFilter = metricsFilter;
        return this;
    }

    /**
     * Run the two stages and return the surviving files.
     *
     * <p>Prints one summary line so the reader can see the funnel:
     * {@code 候选 N → 分区裁剪后 M → 指标裁剪后 K}.
     */
    public List<DataFile> planFiles() {
        List<DataFile> candidates = new ArrayList<>(files);

        // Stage 1: partition pruning — evaluate the partition predicate against each file's
        // partition tuple. The predicate operates on partition field IDs (1000+).
        int beforePartition = candidates.size();
        if (partitionFilter != null && partitionSpec != null && partitionSpec.isPartitioned()) {
            candidates = candidates.stream()
                    .filter(this::matchesPartitionFilter)
                    .collect(Collectors.toList());
        }
        int afterPartition = candidates.size();

        // Stage 2: metrics pruning — evaluate the data predicate against each file's
        // min/max statistics.
        int beforeMetrics = candidates.size();
        if (metricsFilter != null) {
            candidates = candidates.stream()
                    .filter(file -> MetricsFilter.canMatch(metricsFilter, file))
                    .collect(Collectors.toList());
        }
        int afterMetrics = candidates.size();

        System.out.println("    统计: 候选文件 " + beforePartition
                + " → 分区裁剪后 " + afterPartition
                + " → 指标裁剪后 " + afterMetrics);
        return candidates;
    }

    /**
     * Evaluate the partition predicate against one file's partition tuple.
     *
     * <p>The partition tuple holds exact values (one per partition field), so we build a
     * {@link DataFileStats} where {@code lower = upper = partition value} for each field.
     * This lets us reuse the same {@link Expr#evaluate} machinery: a predicate like
     * {@code Equal(1000, 20661)} checks whether the partition value equals 20661.
     */
    private boolean matchesPartitionFilter(DataFile file) {
        if (file.partition() == null || file.partition().isEmpty()) {
            return true;
        }

        List<Object> partition = file.partition();
        List<PartitionField> fields = partitionSpec.fields();
        Map<Integer, Object> bounds = new HashMap<>();
        for (int i = 0; i < fields.size() && i < partition.size(); i++) {
            bounds.put(fields.get(i).fieldId(), partition.get(i));
        }

        DataFileStats stats = new DataFileStats(bounds, bounds, Map.of(), Map.of());
        return partitionFilter.evaluate(stats);
    }
}
