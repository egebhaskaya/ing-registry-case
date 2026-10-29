package com.example.registry.services;

/**
 * Result of processing one command without error.
 *
 * <ul>
 * <li>{@link #CREATED}: a new user and its outbox event were committed.</li>
 * <li>{@link #DUPLICATE_IGNORED}: the same registration (same id, same
 * fingerprint) was already
 * committed earlier, for example because RabbitMQ redelivered the command.
 * Nothing was written.</li>
 * </ul>
 * Conflicts are not outcomes; they are exceptions, because the message must be
 * rejected.
 */
public enum RegistrationOutcome {
    CREATED,
    DUPLICATE_IGNORED
}
