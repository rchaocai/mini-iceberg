package com.iceberglearn.exceptions;

/** Raised when a commit cannot be applied because the base metadata is no longer current. */
public class CommitFailedException extends RuntimeException {

    public CommitFailedException(String message) {
        super(message);
    }

    public CommitFailedException(String message, Object... args) {
        super(String.format(message, args));
    }

    public CommitFailedException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }

    public CommitFailedException(Throwable cause) {
        super(cause);
    }
}
