package com.example.compliance.models;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the {@code processed_events} ledger: "this event id has been
 * handled".
 */
public record ProcessedEvent(
                UUID eventId,
                UUID userId,
                UUID registrationId,
                Instant submittedAt,
                Instant processedAt) {
}
