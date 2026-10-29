package com.example.registry.exceptions;

/** Refused before a transaction starts; retry with the same idempotency key. */
public class SynchronousRegistrationOverloadedException extends RuntimeException {
    public SynchronousRegistrationOverloadedException() {
        super("Registration processing capacity is full; retry with the same Idempotency-Key");
    }
}
