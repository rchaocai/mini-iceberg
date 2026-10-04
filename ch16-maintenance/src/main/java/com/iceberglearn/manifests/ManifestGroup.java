package com.iceberglearn.manifests;

import com.iceberglearn.expressions.*;
import com.iceberglearn.metadata.DataFile;
import com.iceberglearn.metrics.MetricsFilter;
import com.iceberglearn.partition.PartitionSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 * <p>After spec evolution a table carries several specs, and each manifest declares (via its
 * {@code partitionSpecId}) the spec its partition tuples were written with. Files are
 * therefore held <em>grouped by spec id</em>, and the row filter is projected separately for
 * each group: the same {@code event_time} predicate becomes a day predicate for the day-spec
 * group and an hour predicate for the hour-spec group. Metrics pruning is spec-independent
 * and runs over all groups alike.
 *
 * <p>The two filters are independent: callers may set either, both, or neither. When a filter
 * is {@code null} (or a group's spec is unpartitioned) that stage is skipped for the group —
 * which is why a metrics-only scan still prints {@code N → N → K} with the first arrow a no-op.
 */
public class ManifestGroup {

    // Insertion order = manifest order, so a no-filter scan returns files in a stable order.
    private final Map<Integer, List<DataFile>> filesBySpecId;
    private final Map<Integer, PartitionSpec> specsById;

    private Expr partitionFilter = null;
    private Expr rowFilter = null;
    private Expr metricsFilter = null;

    /**
     * Primary constructor for a table that may carry several specs. Fails fast on a group
     * whose spec id is unknown: evaluating a day-projected predicate against hour tuples
     * would silently mis-prune, so the mismatch must never get that far.
     */
    public ManifestGroup(Map<Integer, List<DataFile>> filesBySpecId,
                         Map<Integer, PartitionSpec> specsById) {
        for (Integer specId : filesBySpecId.keySet()) {
            if (!specsById.containsKey(specId)) {
                throw new IllegalStateException(
                        "Manifest references unknown partition spec id " + specId
                                + "; known spec ids: " + specsById.keySet());
            }
        }
        this.filesBySpecId = filesBySpecId;
        this.specsById = specsById;
    }

    /** Single-spec convenience kept for call sites that hold exactly one spec. */
    public ManifestGroup(List<DataFile> files, PartitionSpec partitionSpec) {
        this(new LinkedHashMap<>(Map.of(partitionSpec.specId(), List.copyOf(files))),
                Map.of(partitionSpec.specId(), partitionSpec));
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
     * Set one user-facing row predicate for both pruning stages.
     *
     * <p>The predicate is only staged here; the partition stage projects it per spec group
     * inside {@link #planFiles()}, because each group needs the projection phrased in its own
     * spec's field ids. The metrics stage keeps the original predicate, with NOT pushed to
     * leaves so inclusive evaluation stays conservative.
     */
    public ManifestGroup filterRows(Expr rowFilter) {
        this.rowFilter = rowFilter;
        this.metricsFilter = RewriteNot.rewrite(rowFilter);
        return this;
    }

    /**
     * Run the two stages per spec group and return the surviving files.
     *
     * <p>Prints one summary line per group (only when several specs are present) plus the
     * aggregate funnel, so the reader can see both granularities:
     * {@code 候选 N → 分区裁剪后 M → 指标裁剪后 K}.
     */
    public List<DataFile> planFiles() {
        // One projected predicate per spec id — the teaching stand-in for the official
        // evalCache: each entry is phrased in that spec's field ids.
        Map<Integer, Expr> projectedBySpecId = new LinkedHashMap<>();
        boolean severalSpecs = filesBySpecId.size() > 1;

        List<DataFile> surviving = new ArrayList<>();
        int totalCandidates = 0;
        int totalAfterPartition = 0;
        int totalAfterMetrics = 0;

        for (Map.Entry<Integer, List<DataFile>> group : filesBySpecId.entrySet()) {
            PartitionSpec spec = specsById.get(group.getKey());
            List<DataFile> candidates = new ArrayList<>(group.getValue());

            // Stage 1: partition pruning — evaluate this group's projected predicate against
            // each file's partition tuple, using THIS group's spec to read the tuple.
            int beforePartition = candidates.size();
            Expr groupPartitionFilter = groupPartitionFilter(spec, projectedBySpecId);
            if (groupPartitionFilter != null && spec.isPartitioned()) {
                candidates = candidates.stream()
                        .filter(file -> matchesPartitionFilter(groupPartitionFilter, spec, file))
                        .collect(Collectors.toList());
            }
            int afterPartition = candidates.size();

            // Stage 2: metrics pruning — evaluate the data predicate against each file's
            // min/max statistics. Independent of specs.
            int beforeMetrics = candidates.size();
            if (metricsFilter != null) {
                candidates = candidates.stream()
                        .filter(file -> MetricsFilter.canMatch(metricsFilter, file))
                        .collect(Collectors.toList());
            }
            int afterMetrics = candidates.size();

            if (severalSpecs) {
                System.out.println("    [spec " + spec.specId() + "] 候选文件 " + beforePartition
                        + " → 分区裁剪后 " + afterPartition
                        + " → 指标裁剪后 " + afterMetrics);
            }
            surviving.addAll(candidates);
            totalCandidates += beforePartition;
            totalAfterPartition += afterPartition;
            totalAfterMetrics += afterMetrics;
        }

        System.out.println("    统计: 候选文件 " + totalCandidates
                + " → 分区裁剪后 " + totalAfterPartition
                + " → 指标裁剪后 " + totalAfterMetrics);
        return surviving;
    }

    /**
     * This group's partition predicate: the explicit partition filter (if any) AND the
     * inclusive projection of the row filter through <em>this</em> group's spec — matching
     * the official Expressions.and(partitionFilter, Projections.inclusive(spec).project(...)).
     */
    private Expr groupPartitionFilter(PartitionSpec spec, Map<Integer, Expr> cache) {
        Expr projected = cache.computeIfAbsent(spec.specId(), id ->
                rowFilter != null && spec.isPartitioned()
                        ? Projections.inclusive(spec).project(rowFilter)
                        : AlwaysTrue.INSTANCE);
        return partitionFilter != null ? Exprs.and(partitionFilter, projected) : projected;
    }

    /**
     * Evaluate the partition predicate against one file's partition tuple.
     *
     * <p>The partition tuple holds exact values, so it is evaluated by {@link PartitionEvaluator}
     * rather than the weaker metrics evaluator. This matters for predicates such as {@code !=}.
     */
    private boolean matchesPartitionFilter(Expr filter, PartitionSpec spec, DataFile file) {
        if (file.partition() == null || file.partition().isEmpty()) {
            return true;
        }

        return PartitionEvaluator.evaluate(filter, spec, file.partition());
    }
}
