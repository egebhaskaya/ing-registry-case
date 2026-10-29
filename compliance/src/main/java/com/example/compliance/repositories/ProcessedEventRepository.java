package com.example.compliance.repositories;

import com.example.compliance.models.ProcessedEvent;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** SQL for the {@code processed_events} ledger. */
@Repository
public class ProcessedEventRepository {

    private final JdbcTemplate jdbc;

    public ProcessedEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the ledger row unless the event id is already there.
     *
     * <p>
     * {@code ON CONFLICT DO NOTHING} is PostgreSQL's way to say "ignore a duplicate
     * key instead of
     * failing": the statement returns 0 affected rows and the transaction stays
     * healthy. That is simpler
     * than Registry's approach because Compliance only needs to know "seen
     * before?", not why.
     *
     * @return true if this call inserted the row (first time), false if it already
     *         existed
     */
    public boolean insertIfAbsent(ProcessedEvent event) {
        int inserted = jdbc.update(
                "INSERT INTO processed_events (event_id, user_id, registration_id, submitted_at, processed_at) "
                        + "VALUES (?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING",
                event.eventId(), event.userId(), event.registrationId(),
                OffsetDateTime.ofInstant(event.submittedAt(), ZoneOffset.UTC),
                OffsetDateTime.ofInstant(event.processedAt(), ZoneOffset.UTC));
        return inserted == 1;
    }

    public long count() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM processed_events", Long.class);
        return count == null ? 0 : count;
    }
}
