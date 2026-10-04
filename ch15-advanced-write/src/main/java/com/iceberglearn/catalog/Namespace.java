package com.iceberglearn.catalog;

import java.util.Arrays;
import java.util.Objects;

/** A namespace in a {@link Catalog}. */
public final class Namespace {
    private static final Namespace EMPTY_NAMESPACE = new Namespace(new String[0]);

    public static Namespace empty() {
        return EMPTY_NAMESPACE;
    }

    public static Namespace of(String... levels) {
        Objects.requireNonNull(levels, "Cannot create Namespace from null array");
        if (levels.length == 0) {
            return empty();
        }

        for (String level : levels) {
            Objects.requireNonNull(level, "Cannot create a namespace with a null level");
            if (level.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("Cannot create a namespace with the null-byte character");
            }
        }

        return new Namespace(levels);
    }

    private final String[] levels;

    private Namespace(String[] levels) {
        this.levels = levels.clone();
    }

    public String[] levels() {
        return levels.clone();
    }

    public String level(int position) {
        return levels[position];
    }

    public boolean isEmpty() {
        return levels.length == 0;
    }

    public int length() {
        return levels.length;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        Namespace namespace = (Namespace) other;
        return Arrays.equals(levels, namespace.levels);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(levels);
    }

    @Override
    public String toString() {
        return String.join(".", levels);
    }
}
