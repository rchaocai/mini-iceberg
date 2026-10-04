package com.iceberglearn.transforms;

/** hour(timestamp): the number of hours from 1970-01-01 00:00 to the timestamp's hour. */
public record Hours() implements Transform {

    private static final Hours INSTANCE = new Hours();

    public static Hours get() {
        return INSTANCE;
    }

    private static final long MICROS_PER_HOUR = 60L * 60L * 1_000_000L;

    @Override
    public Object apply(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long micros) {
            return (int) (micros / MICROS_PER_HOUR);
        }
        throw new UnsupportedOperationException(
                "Cannot apply hour transform to " + value.getClass().getName());
    }

    @Override
    public boolean isIdentity() {
        return false;
    }

    @Override
    public String toString() {
        return "hour";
    }
}
