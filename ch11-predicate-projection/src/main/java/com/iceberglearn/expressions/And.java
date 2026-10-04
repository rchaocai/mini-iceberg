package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * Logical AND of two predicates, with inclusive (might-match) semantics.
 *
 * <p>A file may match {@code A AND B} iff it may match {@code A} <em>and</em> it may match
 * {@code B}. If either side is {@code false} (cannot match), the whole AND cannot match; we
 * can skip the file without evaluating the other side. Java's {@code &&} gives us that
 * short-circuit for free.
 */
public record And(Expr left, Expr right) implements Expr {
    @Override
    public boolean evaluate(DataFileStats stats) {
        return left.evaluate(stats) && right.evaluate(stats);
    }

    @Override
    public String toString() {
        return "(" + left + " AND " + right + ")";
    }
}
