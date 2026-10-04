package com.iceberglearn.exceptions;


/** Thrown when the metadata files below a table location are inconsistent. */
public class ValidationException extends RuntimeException {
    public ValidationException(String message) {
        super(message);
    }
}
