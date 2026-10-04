package com.iceberglearn.metrics;

import com.iceberglearn.expressions.*;
import com.iceberglearn.metadata.DataFile;

/**
 * The strict counterpart to {@link MetricsFilter}: answers whether <em>every</em> row in a
 * {@link DataFile} matches a predicate, using only the file's column metrics.
 *
 * <p>The duality with {@link MetricsFilter} is the point of this class:
 * <ul>
 *   <li>{@link MetricsFilter#canMatch} asks "might any row match?" → used by the scan to
 *       <b>keep</b> files (inclusive: when in doubt, read). {@code age > 40} looks at the
 *       <i>upper</i> bound — if even the max is {@code <= 40}, nothing can exceed 40.</li>
 *   <li>{@link #allRowsMatch} asks "must every row match?" → used by overwrite/delete to
 *       <b>remove</b> files (strict: when in doubt, keep). {@code age > 40} looks at the
 *       <i>lower</i> bound — if even the min is {@code > 40}, every row exceeds 40.</li>
 * </ul>
 *
 * <p>"When in doubt" flips too: inclusive returns {@code true} on missing bounds (keep the
 * file); strict returns {@code false} (do not delete a file whose contents are unknown).
 * Deleting the wrong file loses rows; the conservative side is therefore the opposite of
 * inclusive's.
 */
public final class StrictMetricsFilter {

    private StrictMetricsFilter() {
    }

    /**
     * Whether every row in {@code file} matches {@code filter}.
     *
     * @return {@code true} if all rows match (safe to delete); {@code false} if not, or if
     *         the file's metrics cannot prove it (keep the file)
     */
    public static boolean allRowsMatch(Expr filter, DataFile file) {
        if (file == null || filter == null) {
            return false;
        }
        return strictEval(filter, MetricsFilter.dataFileToStats(file));
    }

    private static boolean strictEval(Expr expr, DataFileStats stats) {
        if (expr instanceof AlwaysTrue) {
            // Every row matches the true literal → safe to delete.
            return true;
        }
        if (expr instanceof AlwaysFalse) {
            // No row matches the false literal → cannot delete.
            return false;
        }
        if (expr instanceof GreaterThan g) {
            Object lower = stats.getLowerBound(g.fieldId());
            return lower != null
                    && Expr.toComparable(lower).compareTo(Expr.toComparable(g.value())) > 0;
        }
        if (expr instanceof GreaterThanOrEqual ge) {
            Object lower = stats.getLowerBound(ge.fieldId());
            return lower != null
                    && Expr.toComparable(lower).compareTo(Expr.toComparable(ge.value())) >= 0;
        }
        if (expr instanceof LessThan lt) {
            Object upper = stats.getUpperBound(lt.fieldId());
            return upper != null
                    && Expr.toComparable(upper).compareTo(Expr.toComparable(lt.value())) < 0;
        }
        if (expr instanceof LessThanOrEqual le) {
            Object upper = stats.getUpperBound(le.fieldId());
            return upper != null
                    && Expr.toComparable(upper).compareTo(Expr.toComparable(le.value())) <= 0;
        }
        if (expr instanceof Equal eq) {
            Object lower = stats.getLowerBound(eq.fieldId());
            Object upper = stats.getUpperBound(eq.fieldId());
            return lower != null && upper != null
                    && Expr.toComparable(lower).compareTo(Expr.toComparable(eq.value())) == 0
                    && Expr.toComparable(upper).compareTo(Expr.toComparable(eq.value())) == 0;
        }
        if (expr instanceof NotEqual ne) {
            // Every row != value iff the value falls outside the file's [lower, upper] range.
            Object lower = stats.getLowerBound(ne.fieldId());
            Object upper = stats.getUpperBound(ne.fieldId());
            if (lower == null || upper == null) {
                return false;
            }
            Comparable<Object> v = Expr.toComparable(ne.value());
            return Expr.toComparable(lower).compareTo(v) > 0
                    || Expr.toComparable(upper).compareTo(v) < 0;
        }
        if (expr instanceof And and) {
            return strictEval(and.left(), stats) && strictEval(and.right(), stats);
        }
        if (expr instanceof Or or) {
            return strictEval(or.left(), stats) || strictEval(or.right(), stats);
        }
        if (expr instanceof Not) {
            // "All rows match NOT X" is "no row matches X", which min/max bounds cannot prove
            // for an arbitrary X — be conservative and do not delete.
            return false;
        }
        // Unknown expression node: do not delete what we cannot reason about.
        return false;
    }
}
