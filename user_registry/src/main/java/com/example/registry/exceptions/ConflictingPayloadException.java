package com.example.registry.exceptions;

import java.util.UUID;

/**
 * The same registration id arrived again with different data (design section
 * 5). The original
 * successful record is preserved; this delivery is a permanent failure that
 * belongs in the DLQ.
 */
public class ConflictingPayloadException extends RuntimeException {

    private final UUID registrationId;

    public ConflictingPayloadException(UUID registrationId) {
        super("Registration " + registrationId + " was already processed with a different payload");
        this.registrationId = registrationId;
    }

    public UUID registrationId() {
        return registrationId;
    }
}
