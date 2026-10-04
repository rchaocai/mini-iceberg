package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/** Logical NOT. Min/max alone cannot safely evaluate an arbitrary negated might-match result. */
public record Not(Expr child) implements Expr {
    @Override
    public boolean evaluate(DataFileStats stats) {
        // RewriteNot turns row filters into concrete comparisons before metrics evaluation.
        // A remaining NOT is therefore unknown and must conservatively keep the file.
        return true;
    }

    @Override
    public String toString() {
        return "NOT(" + child + ")";
    }
}
