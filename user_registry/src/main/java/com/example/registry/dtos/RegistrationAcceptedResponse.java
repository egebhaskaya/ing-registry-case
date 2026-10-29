package com.example.registry.dtos;

import java.time.Instant;
import java.util.UUID;

/**
 * The 202 receipt (design section 4). It says "the command was accepted",
 * nothing more: the user is
 * not created yet, Compliance has not run. Returning an explicit body keeps
 * that contract visible
 * instead of implied by an empty response.
 *
 * @param registrationId the Idempotency-Key the client sent; the client can
 *                       resend with the same key safely
 * @param status         always "ACCEPTED"
 * @param acceptedAt     when the broker confirmed the command
 */
public record RegistrationAcceptedResponse(UUID registrationId, String status, Instant acceptedAt) {

    public static RegistrationAcceptedResponse accepted(UUID registrationId, Instant acceptedAt) {
        return new RegistrationAcceptedResponse(registrationId, "ACCEPTED", acceptedAt);
    }
}
