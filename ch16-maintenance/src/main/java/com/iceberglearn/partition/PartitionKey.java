package com.iceberglearn.partition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A computed partition tuple: one value per field of a {@link PartitionSpec}, in field order.
 *
 * <p>Two roles: on the write side it routes rows into per-partition buffers (it is a HashMap
 * key), and on the read side it is the value compared against each DataFile's partition tuple
 * to decide whether a file may contain matching rows. Both uses need equals/hashCode.
 */
public class PartitionKey {

    private final PartitionSpec spec;
    private final List<Object> values;

    public PartitionKey(PartitionSpec spec) {
        this.spec = spec;
        this.values = new ArrayList<>(spec.fields().size());
        // Pre-size and null-fill so set(i, ...) never raises IndexOutOfBounds.
        for (int i = 0; i < spec.fields().size(); i++) {
            values.add(null);
        }
    }

    /** Recompute this key from a row (field id -> value). The row is read, never mutated. */
    public void partition(Map<Integer, Object> row) {
        for (int i = 0; i < spec.fields().size(); i++) {
            PartitionField field = spec.fields().get(i);
            Object sourceValue = row.get(field.sourceId());
            Object transformed = PartitionUtil.transformValue(field.transform(), sourceValue);
            values.set(i, transformed);
        }
    }

    public List<Object> values() {
        return List.copyOf(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionKey that)) {
            return false;
        }
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return PartitionUtil.partitionToString(values, spec);
    }
}
