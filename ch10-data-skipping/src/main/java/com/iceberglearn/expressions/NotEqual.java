package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Predicate {@code field != value}.
 *
 * <p>Pruning would require proof that every non-null value equals the literal. Equal lower and
 * upper bounds are not treated as sufficient proof because stored bounds may be conservative or
 * truncated summaries rather than exact values. The safe answer is therefore always "might
 * match".
 */
public record NotEqual(int fieldId, Object value) implements Expr {

    @Override
    public boolean evaluate(DataFileStats stats) {
        return true;
    }

    @Override
    public String toString() {
        return "field(" + fieldId + ") != " + value;
    }
}
