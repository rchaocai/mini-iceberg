package com.iceberglearn.transforms;

import com.iceberglearn.expressions.Expr;
import com.iceberglearn.expressions.ProjectionUtil;

/**
 * truncate[W](value): for numbers, align to the nearest multiple of W at or below the value;
 * for strings, keep the first W characters.
 */
public record Truncate(int width) implements Transform {

    public static Truncate get(int width) {
        if (width <= 0) {
            throw new IllegalArgumentException(
                    "Invalid truncate width: " + width + " (must be > 0)");
        }
        return new Truncate(width);
    }

    @Override
    public Object apply(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Integer v) {
            return v - Math.floorMod(v, width);
        }
        if (value instanceof Long v) {
            return v - Math.floorMod(v, (long) width);
        }
        if (value instanceof String s) {
            return s.length() <= width ? s : s.substring(0, width);
        }
        throw new UnsupportedOperationException(
                "Cannot truncate " + value.getClass().getName());
    }

    @Override
    public Expr project(int partitionFieldId, Expr sourcePredicate) {
        return ProjectionUtil.projectTruncate(this, partitionFieldId, sourcePredicate);
    }

    @Override
    public boolean isIdentity() {
        return false;
    }

    @Override
    public String toString() {
        return "truncate[" + width + "]";
    }
}
