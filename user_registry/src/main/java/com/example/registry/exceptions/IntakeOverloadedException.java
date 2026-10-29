package com.example.registry.exceptions;

/**
 * Thrown when all admission permits are busy and none became free within the
 * configured wait
 * (design section 4: reject promptly with a controlled 503 instead of queueing
 * requests in memory).
 *
 * <p>
 * Extends {@link RuntimeException} so callers are not forced to declare it;
 * Spring maps it to an
 * HTTP response in {@link ApiExceptionHandler}.
 */
public class IntakeOverloadedException extends RuntimeException {

    public IntakeOverloadedException() {
        super("Intake capacity exhausted; retry shortly");
    }
}
