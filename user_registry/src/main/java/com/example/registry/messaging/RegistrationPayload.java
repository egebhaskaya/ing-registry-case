package com.example.registry.messaging;

/**
 * The normalized registration data carried inside a
 * {@code RegisterUserCommand}.
 *
 * <p>
 * Kept separate from {@code RegistrationRequest} on purpose: the request is
 * what the client sent
 * (raw, validated), the payload is what the system agreed to store (trimmed,
 * canonical case).
 * Only the payload is fingerprinted and persisted.
 */
public record RegistrationPayload(String fullName, String email, String countryCode) {
}
