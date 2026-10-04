package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Predicate {@code field = value}: a file <i>might</i> contain a matching row iff {@code value}
 * falls inside the file's {@code [lower, upper]} range for the column.
 *
 * <p>This time <em>both</em> bounds matter:
 * <ul>
 *   <li>If {@code value < lower}: even the smallest row exceeds {@code value} → cannot match.</li>
 *   <li>If {@code value > upper}: even the largest row is below {@code value} → cannot match.</li>
 * </ul>
 * Only when {@code lower <= value <= upper} is there any chance the file contains {@code value}.
 */
public record Equal(int fieldId, Object value) implements Expr {

    @SuppressWarnings("unchecked")
    @Override
    public boolean evaluate(DataFileStats stats) {
        Object lower = stats.getLowerBound(fieldId);
        Object upper = stats.getUpperBound(fieldId);
        if (lower == null || upper == null) {
            return true;
        }
        Comparable<Object> lowerC = Expr.toComparable(lower);
        Comparable<Object> upperC = Expr.toComparable(upper);
        Comparable<Object> valueC = Expr.toComparable(value);
        return lowerC.compareTo(valueC) <= 0 && upperC.compareTo(valueC) >= 0;
    }

    @Override
    public String toString() {
        return "field(" + fieldId + ") = " + value;
    }
}
