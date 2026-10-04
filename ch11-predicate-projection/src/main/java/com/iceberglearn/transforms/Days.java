package com.iceberglearn.transforms;

import com.iceberglearn.expressions.*;

/** day(timestamp): the number of days from 1970-01-01 to the date of the timestamp. */
public record Days() implements Transform {

    private static final Days INSTANCE = new Days();

    public static Days get() {
        return INSTANCE;
    }

    // Timestamps in this project are microseconds since the epoch, matching Iceberg's TimestampType.
    private static final long MICROS_PER_DAY = 24L * 60L * 60L * 1_000_000L;

    @Override
    public Object apply(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long micros) {
            return (int) Math.floorDiv(micros, MICROS_PER_DAY);
        }
        if (value instanceof Integer days) {
            // Already a day count (e.g. from a date column) — pass through.
            return days;
        }
        throw new UnsupportedOperationException(
                "Cannot apply day transform to " + value.getClass().getName());
    }

    @Override
    public Expr project(int partitionFieldId, Expr sourcePredicate) {
        return ProjectionUtil.projectDiscrete(this, partitionFieldId, sourcePredicate);
    }

    @Override
    public boolean isIdentity() {
        return false;
    }

    @Override
    public String toString() {
        return "day";
    }
}
