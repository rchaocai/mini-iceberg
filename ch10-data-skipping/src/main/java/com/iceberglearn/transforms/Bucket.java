package com.iceberglearn.transforms;

/** bucket[N](value): hash the value into N buckets, returning a bucket index in [0, N). */
public record Bucket(int numBuckets) implements Transform {

    public static Bucket get(int numBuckets) {
        if (numBuckets <= 0) {
            throw new IllegalArgumentException(
                    "Invalid number of buckets: " + numBuckets + " (must be > 0)");
        }
        return new Bucket(numBuckets);
    }

    @Override
    public Object apply(Object value) {
        if (value == null) {
            return null;
        }

        int hash;
        if (value instanceof Integer i) {
            hash = Integer.hashCode(i);
        } else if (value instanceof Long l) {
            hash = Long.hashCode(l);
        } else if (value instanceof String s) {
            hash = s.hashCode();
        } else {
            hash = value.hashCode();
        }

        // & MAX_VALUE zeroes the sign bit: Math.abs(Integer.MIN_VALUE) would still be negative.
        return (hash & Integer.MAX_VALUE) % numBuckets;
    }

    @Override
    public boolean isIdentity() {
        return false;
    }

    @Override
    public String toString() {
        return "bucket[" + numBuckets + "]";
    }
}
