package com.iceberglearn.metadata;

import java.util.List;

/**
 * A metadata description of one physical delete file listed in a <em>delete</em> manifest.
 *
 * <p>A delete file does not hold user rows — it encodes which rows to remove from a data
 * file. A {@link FileContent#POSITION_DELETES} file lists row ordinals inside one
 * referenced {@code referencedDataFile}; an {@link FileContent#EQUALITY_DELETES} file
 * lists value tuples over {@code equalityFieldIds} whose matching rows should be removed.
 *
 * <p>Mirrors the shape of {@link DataFile} (path, record count, partition) so that a delete
 * manifest is read with the same partition routing as a data manifest; the extra fields
 * capture the delete-specific metadata.
 */
public record DeleteFile(
        String path,
        FileContent content,
        long recordCount,
        String referencedDataFile,
        List<Object> partition,
        List<Integer> equalityFieldIds
) {
    public DeleteFile {
        partition = partition == null ? List.of() : List.copyOf(partition);
        equalityFieldIds = equalityFieldIds == null ? null : List.copyOf(equalityFieldIds);
    }

    public static Builder builder(String path, FileContent content) {
        return new Builder(path, content);
    }

    public static class Builder {
        private final String path;
        private final FileContent content;
        private long recordCount = 0;
        private String referencedDataFile;
        private List<Object> partition = List.of();
        private List<Integer> equalityFieldIds;

        private Builder(String path, FileContent content) {
            this.path = path;
            this.content = content;
        }

        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public Builder referencedDataFile(String referencedDataFile) {
            this.referencedDataFile = referencedDataFile;
            return this;
        }

        public Builder partition(List<Object> partition) {
            this.partition = partition;
            return this;
        }

        public Builder equalityFieldIds(List<Integer> equalityFieldIds) {
            this.equalityFieldIds = equalityFieldIds;
            return this;
        }

        public DeleteFile build() {
            return new DeleteFile(path, content, recordCount, referencedDataFile,
                    partition, equalityFieldIds);
        }
    }
}
