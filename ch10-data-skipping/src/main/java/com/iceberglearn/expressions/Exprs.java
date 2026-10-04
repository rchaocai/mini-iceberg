package com.iceberglearn.expressions;

/**
 * Factory methods for building {@link Expr} trees, with light constant-folding.
 *
 * <p>Why a factory, when each {@code Expr} implementation is just a record? Two reasons:
 * <ol>
 *   <li><b>Readability</b>: {@code Exprs.gt(5, 50)} reads more naturally than
 *       {@code new GreaterThan(5, 50)}.</li>
 *   <li><b>Constant folding</b>: {@code and(x, AlwaysFalse)} is logically {@code AlwaysFalse};
 *       {@code or(x, AlwaysTrue)} is logically {@code AlwaysTrue}. Recognizing these at build
 *       time keeps the predicate tree small, so query planners that pile on redundant
 *       {@code AND(filter, true)} don't pay for it at evaluation time.</li>
 * </ol>
 *
 * <p>The folding rules mirror Apache Iceberg's {@code Expressions} utility class.
 */
public final class Exprs {

    private Exprs() {
    }

    /** {@code A AND B}, with constant folding. */
    public static Expr and(Expr left, Expr right) {
        if (left == AlwaysFalse.INSTANCE || right == AlwaysFalse.INSTANCE) {
            return AlwaysFalse.INSTANCE;
        }
        if (left == AlwaysTrue.INSTANCE) {
            return right;
        }
        if (right == AlwaysTrue.INSTANCE) {
            return left;
        }
        return new And(left, right);
    }

    /** {@code A OR B}, with constant folding. */
    public static Expr or(Expr left, Expr right) {
        if (left == AlwaysTrue.INSTANCE || right == AlwaysTrue.INSTANCE) {
            return AlwaysTrue.INSTANCE;
        }
        if (left == AlwaysFalse.INSTANCE) {
            return right;
        }
        if (right == AlwaysFalse.INSTANCE) {
            return left;
        }
        return new Or(left, right);
    }

    /** {@code NOT A}, with double-negation elimination. */
    public static Expr not(Expr child) {
        if (child == AlwaysTrue.INSTANCE) {
            return AlwaysFalse.INSTANCE;
        }
        if (child == AlwaysFalse.INSTANCE) {
            return AlwaysTrue.INSTANCE;
        }
        if (child instanceof Not n) {
            return n.child();
        }
        return new Not(child);
    }

    public static Expr greaterThan(int fieldId, Object value) {
        return new GreaterThan(fieldId, value);
    }

    public static Expr greaterThanOrEqual(int fieldId, Object value) {
        return new GreaterThanOrEqual(fieldId, value);
    }

    public static Expr lessThan(int fieldId, Object value) {
        return new LessThan(fieldId, value);
    }

    public static Expr lessThanOrEqual(int fieldId, Object value) {
        return new LessThanOrEqual(fieldId, value);
    }

    public static Expr equal(int fieldId, Object value) {
        return new Equal(fieldId, value);
    }

    public static Expr notEqual(int fieldId, Object value) {
        return new NotEqual(fieldId, value);
    }

    public static Expr alwaysTrue() {
        return AlwaysTrue.INSTANCE;
    }

    public static Expr alwaysFalse() {
        return AlwaysFalse.INSTANCE;
    }
}
