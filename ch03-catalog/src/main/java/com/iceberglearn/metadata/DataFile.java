package com.iceberglearn.metadata;

import java.util.Map;

/** A metadata description of one physical data file listed in a manifest. */
public record DataFile(
        String path,
        String format,
        long recordCount,
        long fileSizeInBytes,
        Map<Integer, Long> columnSizes,
        Map<Integer, Long> valueCounts,
        Map<Integer, Long> nullValueCounts,
        Map<Integer, Long> lowerBounds,
        Map<Integer, Long> upperBounds
) {
    public DataFile {
        columnSizes = copy(columnSizes);
        valueCounts = copy(valueCounts);
        nullValueCounts = copy(nullValueCounts);
        lowerBounds = copy(lowerBounds);
        upperBounds = copy(upperBounds);
    }

    private static Map<Integer, Long> copy(Map<Integer, Long> values) {
        return values == null ? null : Map.copyOf(values);
    }

    public static Builder builder(String path) {
        return new Builder(path);
    }

    public static class Builder {
        private final String path;
        private String format = "parquet";
        private long recordCount = 0;
        private long fileSizeInBytes = 0;
        private Map<Integer, Long> columnSizes;
        private Map<Integer, Long> valueCounts;
        private Map<Integer, Long> nullValueCounts;
        private Map<Integer, Long> lowerBounds;
        private Map<Integer, Long> upperBounds;

        private Builder(String path) {
            this.path = path;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public Builder fileSizeInBytes(long fileSizeInBytes) {
            this.fileSizeInBytes = fileSizeInBytes;
            return this;
        }

        public Builder lowerBounds(Map<Integer, Long> lowerBounds) {
            this.lowerBounds = lowerBounds;
            return this;
        }

        public Builder upperBounds(Map<Integer, Long> upperBounds) {
            this.upperBounds = upperBounds;
            return this;
        }

        public DataFile build() {
            return new DataFile(
                    path, format, recordCount, fileSizeInBytes,
                    columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds);
        }
    }
}
