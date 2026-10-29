package com.example.registry.models;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the {@code users} table. A plain record: no ORM annotations, the
 * repository maps columns
 * to fields explicitly (design section 3: plain JDBC, no JPA).
 *
 * @param id                 generated per registration with UUID.randomUUID();
 *                           no shared sequence to contend on
 * @param registrationId     the Idempotency-Key, unique
 * @param payloadFingerprint SHA-256 of the normalized payload the user was
 *                           created from
 * @param email              normalized, unique
 */
public record User(
                UUID id,
                UUID registrationId,
                String payloadFingerprint,
                String email,
                String fullName,
                String countryCode,
                Instant createdAt) {
}
