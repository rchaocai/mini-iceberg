package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Logical OR of two predicates, with inclusive (might-match) semantics.
 *
 * <p>A file may match {@code A OR B} iff it may match {@code A} <em>or</em> it may match
 * {@code B}. Only when <em>both</em> sides cannot match can we skip the file. Java's {@code ||}
 * short-circuits: if the left side says "maybe", we don't bother evaluating the right side.
 */
public record Or(Expr left, Expr right) implements Expr {
    @Override
    public boolean evaluate(DataFileStats stats) {
        return left.evaluate(stats) || right.evaluate(stats);
    }

    @Override
    public String toString() {
        return "(" + left + " OR " + right + ")";
    }
}
