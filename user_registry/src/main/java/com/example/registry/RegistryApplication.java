package com.example.registry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the Registry service.
 *
 * <p>
 * {@code @SpringBootApplication} does three things: marks this class as
 * configuration, turns on
 * auto-configuration (Spring wires a database pool, a RabbitMQ connection, a
 * web server... based on
 * what is on the classpath and in application.properties), and scans this package and
 * everything below it
 * for components (controllers, services, repositories, listeners).
 *
 * <p>
 * Layout follows docs/design.md §10, package-by-layer:
 * controllers → services → messaging (HTTP intake) and listeners → services →
 * repositories (processing).
 */
@SpringBootApplication
public class RegistryApplication {

    public static void main(String[] args) {
        SpringApplication.run(RegistryApplication.class, args);
    }
}
