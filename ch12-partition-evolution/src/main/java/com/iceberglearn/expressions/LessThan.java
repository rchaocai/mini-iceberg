package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Predicate {@code field < value}: a file <i>might</i> contain a matching row iff the column's
 * <b>lower bound</b> is strictly less than {@code value}.
 *
 * <p>Mirror of {@link GreaterThan}: {@code <} looks at the lower bound. If even the smallest value
 * in the file is {@code >= value}, no row can be less than {@code value}; skip. Looking at the
 * upper bound instead would answer "do <em>all</em> rows &lt; value", which would skip files
 * that contain some matching rows.
 */
public record LessThan(int fieldId, Object value) implements Expr {

    @SuppressWarnings("unchecked")
    @Override
    public boolean evaluate(DataFileStats stats) {
        Object lower = stats.getLowerBound(fieldId);
        if (lower == null) {
            return true;
        }
        Comparable<Object> lowerC = Expr.toComparable(lower);
        Comparable<Object> valueC = Expr.toComparable(value);
        return lowerC.compareTo(valueC) < 0;
    }

    @Override
    public String toString() {
        return "field(" + fieldId + ") < " + value;
    }
}
