package com.example.compliance.services;

/**
 * Result of handling one event without error.
 *
 * <ul>
 * <li>{@link #PROCESSED}: first time this event id was seen; ledger row and
 * result committed.</li>
 * <li>{@link #DUPLICATE_IGNORED}: the event id was already in the ledger (Kafka
 * redelivered the
 * record, for example after a consumer restart); nothing was written.</li>
 * </ul>
 */
public enum ComplianceOutcome {
    PROCESSED,
    DUPLICATE_IGNORED
}
