package com.example.registry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base class for tests that run the whole application against real containers.
 *
 * <p>
 * {@code RANDOM_PORT} starts the embedded web server on a free port, so HTTP
 * tests go through
 * the real server (and therefore the real request-handling threads, which the
 * virtual-thread test
 * verifies). Spring caches one application context per distinct test
 * configuration: subclasses with the
 * same annotations share one running app and one pair of containers, while a
 * subclass that adds a
 * {@code @MockitoSpyBean} or a nested {@code @TestConfiguration} gets its own.
 * Tests that share a
 * context also share its database, which is why each test cleans up the tables
 * it touches.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
public abstract class AbstractIntegrationTest {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    /** Empties both Registry tables so a test starts from a known state. */
    protected void truncateTables() {
        jdbcTemplate.execute("TRUNCATE TABLE users, outbox");
    }
}
