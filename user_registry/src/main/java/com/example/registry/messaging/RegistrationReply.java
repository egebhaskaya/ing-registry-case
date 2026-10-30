package com.example.registry.messaging;

import com.example.registry.services.RegistrationResult;

/** A committed result or a permanent business conflict, returned only to callers waiting for a reply. */
public record RegistrationReply(RegistrationResult result, String conflict) {
    public static RegistrationReply completed(RegistrationResult result) {
        return new RegistrationReply(result, null);
    }

    public static RegistrationReply conflict(String reason) {
        return new RegistrationReply(null, reason);
    }
}
