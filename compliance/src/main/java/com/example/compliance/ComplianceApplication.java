package com.example.compliance;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the Compliance service.
 *
 * <p>
 * Compliance has no HTTP API of its own (design section 10: a controller is for
 * an actual
 * endpoint, not for routing Kafka consumption through HTTP). Its web server
 * exists only to serve
 * the Actuator health and metrics endpoints. The business entry point is the
 * Kafka listener that
 * consumes {@code UserRegistered} events published by Debezium from the
 * Registry outbox.
 */
@SpringBootApplication
public class ComplianceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ComplianceApplication.class, args);
    }
}
