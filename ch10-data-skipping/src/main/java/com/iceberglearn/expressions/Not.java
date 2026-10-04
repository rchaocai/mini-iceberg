package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/** Logical NOT. Min/max alone cannot safely evaluate an arbitrary negated might-match result. */
public record Not(Expr child) implements Expr {
    @Override
    public boolean evaluate(DataFileStats stats) {
        // Negating "might match" is not the same as "cannot match". Keep the file unless the
        // expression has already been rewritten to a concrete comparison predicate.
        return true;
    }

    @Override
    public String toString() {
        return "NOT(" + child + ")";
    }
}
