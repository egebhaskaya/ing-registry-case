package com.example.compliance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * Smoke tests: the Compliance application starts against real PostgreSQL and
 * Kafka, reports healthy,
 * and Flyway has applied the schema.
 */
@AutoConfigureRestTestClient
class ComplianceApplicationTests extends AbstractIntegrationTest {

    @Autowired
    RestTestClient client;

    @Test
    void healthIsUp() {
        client.get().uri("/actuator/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("UP");
    }

    @Test
    void flywayAppliedTheSchema() {
        List<String> applied = jdbcTemplate.queryForList(
                "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank", String.class);
        assertThat(applied).containsExactly("1");

        jdbcTemplate.queryForObject("SELECT count(*) FROM processed_events", Long.class);
        jdbcTemplate.queryForObject("SELECT count(*) FROM compliance_results", Long.class);
    }
}
