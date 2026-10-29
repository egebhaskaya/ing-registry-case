package com.example.registry.exceptions;

/**
 * The broker did not confirm within the timeout. Design section 4: this is NOT
 * a definite failure.
 * The message may be in the queue. The client is told to retry with the same
 * Idempotency-Key, and the
 * consumer side (fingerprint + unique constraints) guarantees a retry cannot
 * create a second user.
 */
public class IntakeOutcomeUnknownException extends RuntimeException {

    public IntakeOutcomeUnknownException() {
        super("Publication outcome unknown; retry with the same Idempotency-Key");
    }
}
