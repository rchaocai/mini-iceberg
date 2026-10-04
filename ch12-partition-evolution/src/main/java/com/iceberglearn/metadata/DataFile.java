package com.iceberglearn.metadata;

import java.util.List;
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
        Map<Integer, Object> lowerBounds,
        Map<Integer, Object> upperBounds,
        List<Object> partition
) {
    public DataFile {
        columnSizes = copy(columnSizes);
        valueCounts = copy(valueCounts);
        nullValueCounts = copy(nullValueCounts);
        lowerBounds = lowerBounds == null ? null : Map.copyOf(lowerBounds);
        upperBounds = upperBounds == null ? null : Map.copyOf(upperBounds);
        partition = partition == null ? List.of() : List.copyOf(partition);
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
        private Map<Integer, Object> lowerBounds;
        private Map<Integer, Object> upperBounds;
        private List<Object> partition = List.of();

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

        public Builder lowerBounds(Map<Integer, Object> lowerBounds) {
            this.lowerBounds = lowerBounds;
            return this;
        }

        public Builder upperBounds(Map<Integer, Object> upperBounds) {
            this.upperBounds = upperBounds;
            return this;
        }

        public Builder valueCounts(Map<Integer, Long> valueCounts) {
            this.valueCounts = valueCounts;
            return this;
        }

        public Builder nullValueCounts(Map<Integer, Long> nullValueCounts) {
            this.nullValueCounts = nullValueCounts;
            return this;
        }

        public Builder partition(List<Object> partition) {
            this.partition = partition;
            return this;
        }

        public DataFile build() {
            return new DataFile(
                    path, format, recordCount, fileSizeInBytes,
                    columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds,
                    partition);
        }
    }
}
