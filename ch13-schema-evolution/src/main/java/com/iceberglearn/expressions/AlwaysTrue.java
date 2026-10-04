package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * The constant {@code true} predicate: matches every file.
 *
 * <p>Used as the identity element of {@code or} (see {@link Exprs#or}) and as the default filter
 * when no predicate is supplied. Implemented as a singleton so {@code ==} comparison works in
 * the {@link Exprs} constant-folding shortcuts.
 */
public record AlwaysTrue() implements Expr {
    public static final AlwaysTrue INSTANCE = new AlwaysTrue();

    @Override
    public boolean evaluate(DataFileStats stats) {
        return true;
    }

    @Override
    public String toString() {
        return "true";
    }
}
