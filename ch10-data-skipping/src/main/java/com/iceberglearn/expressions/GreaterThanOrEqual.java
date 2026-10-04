package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Predicate {@code field >= value}: a file <i>might</i> contain a matching row iff the column's
 * <b>upper bound</b> is greater than or equal to {@code value}.
 *
 * <p>Mirror of {@link GreaterThan} with the boundary {@code >=} instead of {@code >}. Same bound
 * (upper), same reasoning: the largest value in the file must reach {@code value} or there is
 * no hope of a match.
 */
public record GreaterThanOrEqual(int fieldId, Object value) implements Expr {

    @SuppressWarnings("unchecked")
    @Override
    public boolean evaluate(DataFileStats stats) {
        Object upper = stats.getUpperBound(fieldId);
        if (upper == null) {
            return true;
        }
        Comparable<Object> upperC = Expr.toComparable(upper);
        Comparable<Object> valueC = Expr.toComparable(value);
        return upperC.compareTo(valueC) >= 0;
    }

    @Override
    public String toString() {
        return "field(" + fieldId + ") >= " + value;
    }
}
