package com.example.registry.exceptions;

/**
 * A definite failure to hand the command to the broker: it NACKed the message,
 * returned it as
 * unroutable, or could not be reached at all. In every case the command is
 * known NOT to be queued,
 * so the client may retry.
 */
public class IntakeRejectedException extends RuntimeException {

    private final String reason;

    /**
     * @param reason a short machine-readable code: broker_rejected, unroutable or
     *               broker_unavailable
     */
    public IntakeRejectedException(String reason) {
        super("Registration command not accepted by the broker: " + reason);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
