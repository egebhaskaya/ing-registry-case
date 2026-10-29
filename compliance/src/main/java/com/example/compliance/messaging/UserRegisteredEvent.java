package com.example.compliance.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * The {@code UserRegistered} event as Compliance reads it from Kafka: the JSON
 * that Registry wrote into
 * its outbox payload and Debezium published unchanged. The field names must
 * match Registry's record
 * exactly; this copy exists so the two services share a contract (the JSON)
 * rather than code.
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
