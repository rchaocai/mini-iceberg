package com.iceberglearn.exceptions;

/** Raised when a commit is stopped immediately before atomic rename. */
public class SimulatedCrashException extends RuntimeException {
    public SimulatedCrashException(String message) {
        super(message);
    }
}
