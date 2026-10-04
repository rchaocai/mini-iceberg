package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Predicate {@code field > value}: a file <i>might</i> contain a matching row iff the column's
 * <b>upper bound</b> is strictly greater than {@code value}.
 *
 * <p>Why the upper bound, not the lower? {@code evaluate} answers "might this file contain any
 * row &gt; value" — so we look at the largest value the file could possibly hold. If even the
 * max is {@code <= value}, no row can exceed {@code value}; the file is safe to skip. Looking
 * at the lower bound instead would answer "do <em>all</em> rows exceed value", which is a
 * different (and wrong for pruning) question — it would skip a file like {@code [41, 55]} for
 * {@code age > 50} even though it contains 51..55.
 */
public record GreaterThan(int fieldId, Object value) implements Expr {

    @SuppressWarnings("unchecked")
    @Override
    public boolean evaluate(DataFileStats stats) {
        Object upper = stats.getUpperBound(fieldId);
        if (upper == null) {
            // Conservative: no upper bound on file -> cannot disprove a match -> keep.
            return true;
        }
        Comparable<Object> upperC = Expr.toComparable(upper);
        Comparable<Object> valueC = Expr.toComparable(value);
        return upperC.compareTo(valueC) > 0;
    }

    @Override
    public String toString() {
        return "field(" + fieldId + ") > " + value;
    }
}
