package com.iceberglearn.manifests;


/**
 * A manifest descriptor stored in a manifest-list file.
 *
 * <p>The data-file entries are stored in the physical file at {@link #path()}, not embedded here.
 */
public record ManifestFile(
        String path,
        long length,
        int partitionSpecId,
        ManifestContent content,
        long sequenceNumber,
        long minSequenceNumber,
        Long snapshotId,
        Integer addedFilesCount,
        Integer existingFilesCount,
        Integer deletedFilesCount,
        Long addedRowsCount,
        Long existingRowsCount,
        Long deletedRowsCount
) {
    public static Builder builder(String path) {
        return new Builder(path);
    }

    /**
     * Builder for ManifestFile.
     */
    public static class Builder {
        private final String path;
        private long length = 0;
        private int partitionSpecId = 0;
        private ManifestContent content = ManifestContent.DATA;
        private long sequenceNumber = 0;
        private long minSequenceNumber = 0;
        private Long snapshotId;
        private Integer addedFilesCount = 0;
        private Integer existingFilesCount = 0;
        private Integer deletedFilesCount = 0;
        private Long addedRowsCount = 0L;
        private Long existingRowsCount = 0L;
        private Long deletedRowsCount = 0L;

        private Builder(String path) {
            this.path = path;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder partitionSpecId(int partitionSpecId) {
            this.partitionSpecId = partitionSpecId;
            return this;
        }

        public Builder content(ManifestContent content) {
            this.content = content;
            return this;
        }

        public Builder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        public Builder minSequenceNumber(long minSequenceNumber) {
            this.minSequenceNumber = minSequenceNumber;
            return this;
        }

        public Builder snapshotId(Long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder addedFilesCount(Integer addedFilesCount) {
            this.addedFilesCount = addedFilesCount;
            return this;
        }

        public Builder addedRowsCount(Long addedRowsCount) {
            this.addedRowsCount = addedRowsCount;
            return this;
        }

        public ManifestFile build() {
            return new ManifestFile(
                    path, length, partitionSpecId, content, sequenceNumber, minSequenceNumber, snapshotId,
                    addedFilesCount, existingFilesCount, deletedFilesCount,
                    addedRowsCount, existingRowsCount, deletedRowsCount);
        }
    }
}
