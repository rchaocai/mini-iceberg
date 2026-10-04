package com.iceberglearn.exceptions;

/** Thrown when a create operation targets an existing table. */
public class AlreadyExistsException extends RuntimeException {
    public AlreadyExistsException(String message) {
        super(message);
    }
}
