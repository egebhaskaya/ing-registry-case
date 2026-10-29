package com.example.registry.messaging;

/**
 * What the broker told us about one publish (design section 4: a 202 needs a
 * positive confirm AND routing).
 *
 * <ul>
 * <li>{@link #CONFIRMED}: the broker acknowledged the message and did not
 * return it; it is in the queue.</li>
 * <li>{@link #REJECTED}: the broker NACKed it (for example the queue is full
 * with reject-publish).</li>
 * <li>{@link #UNROUTABLE}: the broker accepted it but no queue was bound for
 * the routing key, so it was returned.</li>
 * <li>{@link #UNKNOWN}: no confirmation arrived in time. The message may or may
 * not be queued; the
 * caller must not claim it definitely failed.</li>
 * </ul>
 */
public enum PublishOutcome {
    CONFIRMED,
    REJECTED,
    UNROUTABLE,
    UNKNOWN
}
