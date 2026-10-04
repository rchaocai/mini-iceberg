package com.iceberglearn.catalog;

import java.util.Arrays;
import java.util.Objects;



/** Identifies a table in a catalog. */
public final class TableIdentifier {

    public static TableIdentifier of(String... names) {
        Objects.requireNonNull(names, "Cannot create table identifier from null array");
        if (names.length == 0) {
            throw new IllegalArgumentException("Cannot create table identifier without a table name");
        }
        return new TableIdentifier(
                Namespace.of(Arrays.copyOf(names, names.length - 1)),
                names[names.length - 1]);
    }

    public static TableIdentifier of(Namespace namespace, String name) {
        return new TableIdentifier(namespace, name);
    }

    public static TableIdentifier parse(String identifier) {
        Objects.requireNonNull(identifier, "Cannot parse table identifier: null");
        return of(identifier.split("\\.", -1));
    }

    private final Namespace namespace;
    private final String name;

    private TableIdentifier(Namespace namespace, String name) {
        this.namespace = Objects.requireNonNull(namespace, "Invalid Namespace: null");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid table name: null or empty");
        }
        this.name = name;
    }

    public Namespace namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    public boolean hasNamespace() {
        return !namespace.isEmpty();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TableIdentifier that = (TableIdentifier) other;
        return namespace.equals(that.namespace) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }

    @Override
    public String toString() {
        return hasNamespace() ? namespace + "." + name : name;
    }
}
