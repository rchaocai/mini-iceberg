package com.iceberglearn.operations;

import com.iceberglearn.exceptions.CommitFailedException;
import com.iceberglearn.metadata.TableMetadata;

/**
 * Drives the read-compute-commit cycle for an update that publishes a new snapshot.
 *
 * <p>Subclasses implement {@link #apply(TableMetadata)} to describe how the next metadata
 * follows from a given base. This class owns the optimistic-concurrency retry loop: it reads
 * the current metadata, hands it to {@code apply}, attempts the commit, and on a conflict
 * waits with exponential backoff before re-reading and re-applying against the new base.
 */
abstract class SnapshotUpdate {
    // Defaults match typical production table property values, so the
    // observable retry behaviour matches what a production deployment would do.
    static final int DEFAULT_MAX_RETRIES = 4;
    static final long DEFAULT_MIN_RETRY_WAIT_MS = 100;
    static final long DEFAULT_MAX_RETRY_WAIT_MS = 60_000;
    static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;

    private final TableOperations operations;
    private final String operationName;

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private long minRetryWaitMs = DEFAULT_MIN_RETRY_WAIT_MS;
    private long maxRetryWaitMs = DEFAULT_MAX_RETRY_WAIT_MS;
    private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;

    protected SnapshotUpdate(TableOperations operations, String operationName) {
        this.operations = operations;
        this.operationName = operationName;
    }

    /** Compute the metadata that should follow {@code base} when this update is applied. */
    protected abstract TableMetadata apply(TableMetadata base);

    /** Public entry point used by callers such as {@code AppendFiles.commit()}. */
    public void commit() {
        TableMetadata base = operations.current();
        TableMetadata next = apply(base);

        int attempt = 0;
        long waitMs = minRetryWaitMs;
        while (true) {
            attempt++;
            try {
                operations.commit(base, next);
                System.out.printf("  [%s] commit succeeded on attempt %d%n", operationName, attempt);
                return;
            } catch (CommitFailedException conflict) {
                if (attempt >= maxRetries) {
                    throw new CommitFailedException(
                            conflict,
                            "Cannot commit %s after %d attempts",
                            operationName, attempt);
                }
                System.out.printf("  [%s] conflict on attempt %d, retrying in %dms%n",
                        operationName, attempt, waitMs);
                sleep(waitMs);
                waitMs = (long) Math.min(waitMs * backoffMultiplier, maxRetryWaitMs);

                // Re-read the latest base and re-apply the update against it. Re-applying — not
                // replaying the previous next — is what keeps a concurrent writer's commit alive:
                // the new next is built on top of whatever landed on disk while we were waiting.
                base = operations.refresh();
                next = apply(base);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommitFailedException(e);
        }
    }
}
