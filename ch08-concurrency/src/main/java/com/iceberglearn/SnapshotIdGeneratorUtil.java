package com.iceberglearn;

import java.util.UUID;

public final class SnapshotIdGeneratorUtil {

    private SnapshotIdGeneratorUtil() {}

    public static long generateSnapshotID() {
        UUID uuid = UUID.randomUUID();
        long mostSignificantBits = uuid.getMostSignificantBits();
        long leastSignificantBits = uuid.getLeastSignificantBits();
        return (mostSignificantBits ^ leastSignificantBits) & Long.MAX_VALUE;
    }
}
