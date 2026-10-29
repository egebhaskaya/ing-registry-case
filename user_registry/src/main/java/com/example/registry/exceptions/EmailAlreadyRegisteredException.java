package com.example.registry.exceptions;

import java.util.UUID;

/**
 * A different registration id tried to register an email that already exists
 * (design section 5):
 * a permanent business conflict. No user and no success event are created; the
 * delivery goes to the DLQ.
 */
public class EmailAlreadyRegisteredException extends RuntimeException {

    private final UUID registrationId;

    public EmailAlreadyRegisteredException(UUID registrationId) {
        super("Registration " + registrationId + " uses an email that is already registered");
        this.registrationId = registrationId;
    }

    public UUID registrationId() {
        return registrationId;
    }
}
