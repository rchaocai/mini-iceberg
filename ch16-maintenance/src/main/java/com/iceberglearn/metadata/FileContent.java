package com.iceberglearn.metadata;

/**
 * Content type stored in a file, one of {@link #DATA}, {@link #POSITION_DELETES}, or
 * {@link #EQUALITY_DELETES}.
 *
 * <p>A {@link DataFile} always carries {@link #DATA}. A {@link DeleteFile} carries either
 * {@link #POSITION_DELETES} (delete rows by position within a referenced data file) or
 * {@link #EQUALITY_DELETES} (delete rows whose values match a set of equality fields).
 */
public enum FileContent {
    DATA(0),
    POSITION_DELETES(1),
    EQUALITY_DELETES(2);

    private final int id;

    FileContent(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }
}
