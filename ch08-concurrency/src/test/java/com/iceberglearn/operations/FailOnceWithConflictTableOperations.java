package com.iceberglearn.operations;

import com.iceberglearn.exceptions.CommitFailedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Throws CommitFailedException on the first renameToFinal call, then delegates to super.
 * Used to exercise SnapshotUpdate's retry loop: the first commit attempt looks like a
 * conflict at the rename layer, so the update must refresh, re-apply, and try again.
 */
class FailOnceWithConflictTableOperations extends FileSystemTableOperations {
    private boolean shouldFail = true;

    FailOnceWithConflictTableOperations(Path tableLocation) {
        super(tableLocation);
    }

    @Override
    protected void renameToFinal(Path source, Path target, int nextVersion) {
        if (shouldFail) {
            shouldFail = false;
            // Match the real conflict path: clean up the temporary file before signalling failure.
            try {
                Files.deleteIfExists(source);
            } catch (IOException ignored) {
                // best-effort cleanup; the temp file is harmless if it stays
            }
            throw new CommitFailedException(
                    "Version %d already exists: %s", nextVersion, target);
        }
        super.renameToFinal(source, target, nextVersion);
    }
}
