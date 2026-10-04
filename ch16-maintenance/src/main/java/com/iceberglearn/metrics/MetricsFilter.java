package com.iceberglearn.metrics;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.metadata.DataFile;

/**
 * Adapter that turns "does this {@link DataFile} match this {@link Expr}" into a pure
 * {@code Expr} × {@link DataFileStats} evaluation.
 *
 * <p>The two-step split lets {@link Expr} implementations depend only on {@link DataFileStats},
 * not on {@link DataFile}'s full surface — predicates stay testable with a hand-built stats
 * object, no {@code DataFile.Builder} ceremony required. The {@link #dataFileToStats} adapter
 * is also where future metric sources (manifest partition summaries, Parquet footer stats)
 * would plug in.
 */
public final class MetricsFilter {

    private MetricsFilter() {
    }

    /**
     * Whether {@code file} may contain any row matching {@code filter}.
     *
     * @return {@code true} if the file may match (keep), {@code false} if it cannot match (skip)
     */
    public static boolean canMatch(Expr filter, DataFile file) {
        if (file == null || filter == null) {
            return true;
        }
        return filter.evaluate(dataFileToStats(file));
    }

    /** Pull the four metric maps out of a {@link DataFile} into a {@link DataFileStats}. */
    public static DataFileStats dataFileToStats(DataFile file) {
        return new DataFileStats(
                file.lowerBounds(),
                file.upperBounds(),
                file.valueCounts(),
                file.nullValueCounts()
        );
    }
}
