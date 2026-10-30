package com.example.registry.exceptions;

/** A timeout cannot cancel a queued command. Retrying must preserve its idempotency key. */
public class IntakeOutcomeUnknownException extends RuntimeException {
    private final String reason;

    public IntakeOutcomeUnknownException() {
        this("confirm_timeout");
    }

    public IntakeOutcomeUnknownException(String reason) {
        super("Registration outcome unknown; retry with the same Idempotency-Key");
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
