package com.iceberglearn.expressions;

import com.iceberglearn.transforms.*;

/** Transform-specific helpers for inclusive predicate projection. */
public final class ProjectionUtil {

    private ProjectionUtil() {
    }

    public static Expr projectIdentity(int fieldId, Expr predicate) {
        if (predicate instanceof Equal p) {
            return Exprs.equal(fieldId, p.value());
        } else if (predicate instanceof NotEqual p) {
            return Exprs.notEqual(fieldId, p.value());
        } else if (predicate instanceof LessThan p) {
            return Exprs.lessThan(fieldId, p.value());
        } else if (predicate instanceof LessThanOrEqual p) {
            return Exprs.lessThanOrEqual(fieldId, p.value());
        } else if (predicate instanceof GreaterThan p) {
            return Exprs.greaterThan(fieldId, p.value());
        } else if (predicate instanceof GreaterThanOrEqual p) {
            return Exprs.greaterThanOrEqual(fieldId, p.value());
        }
        return null;
    }

    public static Expr projectDiscrete(Transform transform, int fieldId, Expr predicate) {
        if (predicate instanceof Equal p) {
            return Exprs.equal(fieldId, transform.apply(p.value()));
        } else if (predicate instanceof LessThan p) {
            Object previous = decrement(p.value());
            return previous == null ? null
                    : Exprs.lessThanOrEqual(fieldId, transform.apply(previous));
        } else if (predicate instanceof LessThanOrEqual p) {
            return Exprs.lessThanOrEqual(fieldId, transform.apply(p.value()));
        } else if (predicate instanceof GreaterThan p) {
            Object next = increment(p.value());
            return next == null ? null
                    : Exprs.greaterThanOrEqual(fieldId, transform.apply(next));
        } else if (predicate instanceof GreaterThanOrEqual p) {
            return Exprs.greaterThanOrEqual(fieldId, transform.apply(p.value()));
        }
        // NOT_EQ cannot be projected inclusively through a many-to-one transform.
        return null;
    }

    public static Expr projectBucket(Bucket bucket, int fieldId, Expr predicate) {
        if (predicate instanceof Equal p) {
            return Exprs.equal(fieldId, bucket.apply(p.value()));
        }
        // A range says nothing useful about a hash bucket; NOT_EQ is also unsafe.
        return null;
    }

    public static Expr projectTruncate(Truncate truncate, int fieldId, Expr predicate) {
        Object value = literal(predicate);
        if (value instanceof Integer || value instanceof Long) {
            return projectDiscrete(truncate, fieldId, predicate);
        }

        if (predicate instanceof Equal p) {
            return Exprs.equal(fieldId, truncate.apply(p.value()));
        } else if (predicate instanceof LessThan p) {
            return Exprs.lessThanOrEqual(fieldId, truncate.apply(p.value()));
        } else if (predicate instanceof LessThanOrEqual p) {
            return Exprs.lessThanOrEqual(fieldId, truncate.apply(p.value()));
        } else if (predicate instanceof GreaterThan p) {
            return Exprs.greaterThanOrEqual(fieldId, truncate.apply(p.value()));
        } else if (predicate instanceof GreaterThanOrEqual p) {
            return Exprs.greaterThanOrEqual(fieldId, truncate.apply(p.value()));
        }
        return null;
    }

    private static Object literal(Expr predicate) {
        if (predicate instanceof Equal p) {
            return p.value();
        } else if (predicate instanceof NotEqual p) {
            return p.value();
        } else if (predicate instanceof LessThan p) {
            return p.value();
        } else if (predicate instanceof LessThanOrEqual p) {
            return p.value();
        } else if (predicate instanceof GreaterThan p) {
            return p.value();
        } else if (predicate instanceof GreaterThanOrEqual p) {
            return p.value();
        }
        return null;
    }

    private static Object increment(Object value) {
        if (value instanceof Integer i) {
            return i == Integer.MAX_VALUE ? null : i + 1;
        } else if (value instanceof Long l) {
            return l == Long.MAX_VALUE ? null : l + 1L;
        }
        return null;
    }

    private static Object decrement(Object value) {
        if (value instanceof Integer i) {
            return i == Integer.MIN_VALUE ? null : i - 1;
        } else if (value instanceof Long l) {
            return l == Long.MIN_VALUE ? null : l - 1L;
        }
        return null;
    }
}
