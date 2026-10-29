package com.example.registry.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * The message the API publishes to RabbitMQ and the consumer executes (design
 * section 4, "command").
 *
 * @param registrationId     the client Idempotency-Key; the consumer uses it to
 *                           detect redelivery
 * @param payloadFingerprint SHA-256 of the normalized payload; distinguishes
 *                           "same request again"
 *                           from "same key, different data"
 * @param payload            the normalized registration data
 * @param submittedAt        when the API accepted it; carried through to the
 *                           outbox event so completion
 *                           latency can be measured end to end (design section
 *                           7)
 */
public record RegisterUserCommand(
                UUID registrationId,
                String payloadFingerprint,
                RegistrationPayload payload,
                Instant submittedAt) {
}
