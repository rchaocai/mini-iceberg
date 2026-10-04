package com.iceberglearn.exceptions;

/** Thrown when a table identifier cannot be resolved. */
public class NoSuchTableException extends RuntimeException {
    public NoSuchTableException(String message) {
        super(message);
    }
}
