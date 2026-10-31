package com.example.registry.repositories;

import com.example.registry.models.OutboxEvent;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * SQL for the {@code outbox} table. The application only ever inserts here;
 * Debezium reads the
 * committed rows from the PostgreSQL write-ahead log and publishes them to
 * Kafka (design section 5).
 * The read methods exist for tests and verification, not for a polling
 * publisher.
 */
@Repository
public class OutboxRepository {

    private static final String INSERT_SQL =
            "INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload, created_at) "
                    + "VALUES (?, ?, ?, ?, ?::jsonb, ?)";

    private static final RowMapper<OutboxEvent> ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getObject("id", UUID.class),
            rs.getString("aggregatetype"),
            rs.getString("aggregateid"),
            rs.getString("type"),
            rs.getString("payload"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant());

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * {@code ?::jsonb} casts the JSON string parameter to the JSONB column type on
     * the database side.
     */
    public void insert(OutboxEvent event) {
        jdbc.update(INSERT_SQL, parameters(event));
    }

    public void insertBatch(List<OutboxEvent> events) {
        if (!events.isEmpty()) {
            jdbc.batchUpdate(INSERT_SQL, events.stream().map(OutboxRepository::parameters).toList());
        }
    }

    private static Object[] parameters(OutboxEvent event) {
        return new Object[] {
                event.id(), event.aggregateType(), event.aggregateId(), event.type(), event.payload(),
                OffsetDateTime.ofInstant(event.createdAt(), ZoneOffset.UTC) };
    }

    public List<OutboxEvent> findAll() {
        return jdbc.query(
                "SELECT id, aggregatetype, aggregateid, type, payload::text AS payload, created_at "
                        + "FROM outbox ORDER BY created_at",
                ROW_MAPPER);
    }

    public long count() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM outbox", Long.class);
        return count == null ? 0 : count;
    }
}
