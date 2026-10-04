package com.iceberglearn.operations;

/**
 * Common shape of every staged metadata update: describe changes, then either peek at the
 * result or publish it.
 *
 * <p>{@link #apply()} builds the would-be result without touching the table, so callers can
 * validate or inspect it; {@link #commit()} publishes the result through table operations.
 */
public interface PendingUpdate<T> {

    /** Produce the new value without modifying the table. */
    T apply();

    /** Apply the staged changes to the table. */
    void commit();
}
