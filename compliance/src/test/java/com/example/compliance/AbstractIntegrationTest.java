package com.example.compliance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base class for tests that run the whole Compliance application against real
 * containers.
 * Subclasses with identical annotations share one application context (and its
 * database), so each
 * test cleans the tables it touches.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
public abstract class AbstractIntegrationTest {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    /** Results reference the ledger, so both are emptied together. */
    protected void truncateTables() {
        jdbcTemplate.execute("TRUNCATE TABLE compliance_results, processed_events");
    }
}
