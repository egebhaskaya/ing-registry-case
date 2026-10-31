package com.example.registry.repositories;

import com.example.registry.models.User;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * SQL for the {@code users} table, and nothing else (design section 10:
 * repositories contain SQL,
 * not orchestration).
 *
 * <p>
 * {@link JdbcTemplate} runs a statement, binds the {@code ?} parameters in
 * order, maps rows, and
 * translates the driver's SQLException into Spring's exception hierarchy, which
 * is why a unique
 * violation surfaces as {@code DuplicateKeyException} in the service.
 * {@code @Repository} marks the
 * class as the persistence layer and registers it as a bean.
 */
@Repository
public class UserRepository {

    private static final String INSERT_SQL =
            "INSERT INTO users (id, registration_id, payload_fingerprint, email, full_name, country_code, created_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?)";

    /** How one result-set row becomes a User. Written once, used by every query. */
    private static final RowMapper<User> ROW_MAPPER = (rs, rowNum) -> new User(
            rs.getObject("id", UUID.class),
            rs.getObject("registration_id", UUID.class),
            rs.getString("payload_fingerprint"),
            rs.getString("email"),
            rs.getString("full_name"),
            rs.getString("country_code"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant());

    private final JdbcTemplate jdbc;

    public UserRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<User> findByRegistrationId(UUID registrationId) {
        return jdbc.query(
                "SELECT id, registration_id, payload_fingerprint, email, full_name, country_code, created_at "
                        + "FROM users WHERE registration_id = ?",
                ROW_MAPPER, registrationId).stream().findFirst();
    }

    public List<User> findByRegistrationIds(Collection<UUID> registrationIds) {
        if (registrationIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(registrationIds.size(), "?"));
        return jdbc.query(
                "SELECT id, registration_id, payload_fingerprint, email, full_name, country_code, created_at "
                        + "FROM users WHERE registration_id IN (" + placeholders + ")",
                ROW_MAPPER, registrationIds.toArray());
    }

    /**
     * Plain INSERT: if registration_id or email already exists, PostgreSQL rejects
     * it and Spring throws.
     */
    public void insert(User user) {
        jdbc.update(INSERT_SQL, parameters(user));
    }

    public void insertBatch(List<User> users) {
        if (!users.isEmpty()) {
            jdbc.batchUpdate(INSERT_SQL, users.stream().map(UserRepository::parameters).toList());
        }
    }

    private static Object[] parameters(User user) {
        return new Object[] {
                user.id(), user.registrationId(), user.payloadFingerprint(), user.email(),
                user.fullName(), user.countryCode(), OffsetDateTime.ofInstant(user.createdAt(), ZoneOffset.UTC) };
    }

    public boolean existsByEmail(String email) {
        Boolean exists = jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM users WHERE email = ?)", Boolean.class,
                email);
        return Boolean.TRUE.equals(exists);
    }

    public long count() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM users", Long.class);
        return count == null ? 0 : count;
    }
}
