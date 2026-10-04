package com.iceberglearn.operations;

import java.nio.file.Path;
import com.iceberglearn.exceptions.SimulatedCrashException;



/** Stops the first commit after the temporary metadata file is complete. */
class FailOnceBeforeRenameTableOperations extends FileSystemTableOperations {
    private boolean shouldFail = true;

    FailOnceBeforeRenameTableOperations(Path tableLocation) {
        super(tableLocation);
    }

    @Override
    protected void renameToFinal(Path source, Path target) {
        if (shouldFail) {
            shouldFail = false;
            System.out.println("  [commit] stop before atomic rename: " + source.getFileName()
                    + " -> " + target.getFileName());
            throw new SimulatedCrashException(
                    "Process stopped before renaming " + source.getFileName());
        }
        super.renameToFinal(source, target);
    }
}
