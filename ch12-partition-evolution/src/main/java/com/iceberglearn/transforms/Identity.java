package com.iceberglearn.transforms;

import com.iceberglearn.expressions.*;

/** The identity transform: the partition value is the source value, unchanged. */
public record Identity() implements Transform {

    // Stateless, so one instance is enough — like the real Iceberg Identity.get().
    private static final Identity INSTANCE = new Identity();

    public static Identity get() {
        return INSTANCE;
    }

    @Override
    public Object apply(Object value) {
        return value;
    }

    @Override
    public Expr project(int partitionFieldId, Expr sourcePredicate) {
        return ProjectionUtil.projectIdentity(partitionFieldId, sourcePredicate);
    }

    @Override
    public boolean isIdentity() {
        return true;
    }

    @Override
    public String toString() {
        return "identity";
    }
}
