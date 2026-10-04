package com.iceberglearn.transforms;

/**
 * truncate[W](value): for numbers, align to the nearest multiple of W toward zero;
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
            // Align toward zero, not toward negative infinity: truncate[10](-23) = -20, not -30.
            return v >= 0 ? v - (v % width) : v + ((-v) % width);
        }
        if (value instanceof Long v) {
            return v >= 0 ? v - (v % width) : v + ((-v) % width);
        }
        if (value instanceof String s) {
            return s.length() <= width ? s : s.substring(0, width);
        }
        throw new UnsupportedOperationException(
                "Cannot truncate " + value.getClass().getName());
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
