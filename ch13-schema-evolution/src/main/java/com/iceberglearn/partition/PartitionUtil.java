package com.iceberglearn.partition;

import com.iceberglearn.transforms.*;

import java.util.List;

/** Dispatch helpers that keep transform application and partition formatting in one place. */
public final class PartitionUtil {

    private PartitionUtil() {
    }

    /** Apply a transform by dispatching on its concrete type (sealed => exhaustive). */
    public static Object transformValue(Transform transform, Object value) {
        if (transform instanceof Identity i) {
            return i.apply(value);
        }
        if (transform instanceof Bucket b) {
            return b.apply(value);
        }
        if (transform instanceof Truncate t) {
            return t.apply(value);
        }
        if (transform instanceof Days d) {
            return d.apply(value);
        }
        if (transform instanceof Hours h) {
            return h.apply(value);
        }
        throw new IllegalStateException("Unhandled transform: " + transform);
    }

    /** Format a partition tuple as {field_name=value, ...}, pairing values with the spec's field names. */
    public static String partitionToString(List<Object> values, PartitionSpec spec) {
        List<PartitionField> fields = spec.fields();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            String name = i < fields.size() ? fields.get(i).name() : ("field" + i);
            sb.append(name).append('=').append(values.get(i));
        }
        sb.append('}');
        return sb.toString();
    }
}
