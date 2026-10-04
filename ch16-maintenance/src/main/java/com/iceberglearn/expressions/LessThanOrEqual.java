package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Predicate {@code field <= value}: a file <i>might</i> contain a matching row iff the column's
 * <b>lower bound</b> is less than or equal to {@code value}.
 *
 * <p>Mirror of {@link LessThan} with {@code <=} instead of {@code <}. Same bound (lower): if the
 * smallest value already exceeds {@code value}, nothing can satisfy {@code <= value}.
 */
public record LessThanOrEqual(int fieldId, Object value) implements Expr {

    @SuppressWarnings("unchecked")
    @Override
    public boolean evaluate(DataFileStats stats) {
        Object lower = stats.getLowerBound(fieldId);
        if (lower == null) {
            return true;
        }
        Comparable<Object> lowerC = Expr.toComparable(lower);
        Comparable<Object> valueC = Expr.toComparable(value);
        return lowerC.compareTo(valueC) <= 0;
    }

    @Override
    public String toString() {
        return "field(" + fieldId + ") <= " + value;
    }
}
