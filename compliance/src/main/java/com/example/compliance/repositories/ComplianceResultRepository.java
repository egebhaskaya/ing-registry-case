package com.example.compliance.repositories;

import com.example.compliance.models.ComplianceResult;
import com.example.compliance.services.ComplianceDecision;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/** SQL for the {@code compliance_results} table. */
@Repository
public class ComplianceResultRepository {

    private static final RowMapper<ComplianceResult> ROW_MAPPER = (rs, rowNum) -> new ComplianceResult(
            rs.getObject("event_id", UUID.class),
            rs.getObject("user_id", UUID.class),
            rs.getObject("registration_id", UUID.class),
            ComplianceDecision.valueOf(rs.getString("decision")), // the column stores the enum's name
            rs.getString("reason"),
            rs.getObject("checked_at", OffsetDateTime.class).toInstant());

    private final JdbcTemplate jdbc;

    public ComplianceResultRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(ComplianceResult result) {
        jdbc.update(
                "INSERT INTO compliance_results (event_id, user_id, registration_id, decision, reason, checked_at) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                result.eventId(), result.userId(), result.registrationId(), result.decision().name(),
                result.reason(), OffsetDateTime.ofInstant(result.checkedAt(), ZoneOffset.UTC));
    }

    public Optional<ComplianceResult> findByEventId(UUID eventId) {
        return jdbc.query(
                "SELECT event_id, user_id, registration_id, decision, reason, checked_at "
                        + "FROM compliance_results WHERE event_id = ?",
                ROW_MAPPER, eventId).stream().findFirst();
    }

    public long count() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM compliance_results", Long.class);
        return count == null ? 0 : count;
    }
}
