package com.example.registry.models;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the {@code outbox} table, in the shape Debezium's Outbox Event
 * Router expects.
 *
 * @param id            stable event id; Compliance deduplicates on it
 * @param aggregateType "user"; Debezium derives the Kafka topic from it
 * @param aggregateId   the user id; becomes the Kafka record key (design
 *                      section 5)
 * @param type          "UserRegistered"
 * @param payload       the event body as a JSON string (stored as JSONB)
 */
public record OutboxEvent(
                UUID id,
                String aggregateType,
                String aggregateId,
                String type,
                String payload,
                Instant createdAt) {
}
