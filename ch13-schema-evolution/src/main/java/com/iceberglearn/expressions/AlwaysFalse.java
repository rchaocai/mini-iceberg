package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * The constant {@code false} predicate: matches no file.
 *
 * <p>Used as the identity element of {@code and} (see {@link Exprs#and}) and as the result of
 * folding an unsatisfiable filter. Singleton so {@code ==} comparison works in {@link Exprs}.
 */
public record AlwaysFalse() implements Expr {
    public static final AlwaysFalse INSTANCE = new AlwaysFalse();

    @Override
    public boolean evaluate(DataFileStats stats) {
        return false;
    }

    @Override
    public String toString() {
        return "false";
    }
}
