package com.iceberglearn.metrics;

import java.util.Map;

/**
 * A read-only view of one data file's column metrics, queried by field id.
 *
 * <p>This is the input to {@link com.iceberglearn.expressions.Expr#evaluate}. Wrapping the four raw maps that {@link com.iceberglearn.metadata.DataFile}
 * carries keeps the {@code Expr} implementations independent of {@code DataFile}'s other fields
 * (path, format, partition, ...) — predicates care only about min/max/null counts.
 *
 * <p>Defensive by design: getters return {@code null}/{@code 0} rather than throwing when a map
 * is absent or a column is missing. The "missing metric" case is then handled by each
 * predicate's conservative branch ({@code return true} — keep the file).
 */
public record DataFileStats(
        Map<Integer, Object> lowerBounds,
        Map<Integer, Object> upperBounds,
        Map<Integer, Long> valueCounts,
        Map<Integer, Long> nullValueCounts
) {

    /** Lower bound of {@code fieldId}, or {@code null} if not collected. */
    public Object getLowerBound(int fieldId) {
        return lowerBounds != null ? lowerBounds.get(fieldId) : null;
    }

    /** Upper bound of {@code fieldId}, or {@code null} if not collected. */
    public Object getUpperBound(int fieldId) {
        return upperBounds != null ? upperBounds.get(fieldId) : null;
    }

    /** Non-null value count for {@code fieldId}; {@code 0} when unknown. */
    public long getValueCount(int fieldId) {
        return valueCounts != null ? valueCounts.getOrDefault(fieldId, 0L) : 0L;
    }

    /** Null value count for {@code fieldId}; {@code 0} when unknown. */
    public long getNullCount(int fieldId) {
        return nullValueCounts != null ? nullValueCounts.getOrDefault(fieldId, 0L) : 0L;
    }

    /** Whether the file is known to contain any nulls for {@code fieldId}. */
    public boolean hasNull(int fieldId) {
        return getNullCount(fieldId) > 0;
    }
}
