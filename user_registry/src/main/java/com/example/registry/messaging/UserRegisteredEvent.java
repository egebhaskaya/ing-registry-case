package com.example.registry.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * The payload of the {@code UserRegistered} outbox event: the contract
 * Compliance consumes.
 *
 * <p>
 * {@code eventId} is the same value as the outbox row id. Debezium also puts it
 * in a Kafka header,
 * but carrying it inside the payload lets Compliance deduplicate on a field it
 * parses anyway, with no
 * dependency on header mapping. The event carries the original submission time
 * so Compliance can
 * measure end-to-end completion from the moment the API accepted the request
 * (design sections 4 and 7).
 */
public record UserRegisteredEvent(
                UUID eventId,
                UUID userId,
                UUID registrationId,
                String email,
                String fullName,
                String countryCode,
                Instant submittedAt,
                Instant registeredAt) {
}
