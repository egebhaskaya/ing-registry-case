package com.example.registry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Smoke test: the application starts against real PostgreSQL/RabbitMQ and
 * Flyway has applied the schema.
 */
class RegistryApplicationTests extends AbstractIntegrationTest {

    @Test
    void flywayAppliedTheSchema() {
        // Flyway records every applied migration here; version 1 must be present and
        // successful.
        List<String> applied = jdbcTemplate.queryForList(
                "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank", String.class);
        assertThat(applied).containsExactly("1");

        // The tables exist: these queries would fail with "relation does not exist"
        // otherwise.
        jdbcTemplate.queryForObject("SELECT count(*) FROM users", Long.class);
        jdbcTemplate.queryForObject("SELECT count(*) FROM outbox", Long.class);
    }
}
