package com.example.registry;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Starts a real PostgreSQL and a real RabbitMQ in Docker for the integration
 * tests.
 *
 * <p>
 * {@code @ServiceConnection} tells Spring Boot "connect the application to this
 * container",
 * so the test does not need to know the random port Docker picked. The design
 * (§9) requires the
 * database and broker semantics to be tested for real, not with an in-memory
 * substitute.
 *
 * <p>
 * Image tags are pinned to the same versions as infra/compose.yml.
 */
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(DockerImageName.parse("postgres:17.11"));
    }

    @Bean
    @ServiceConnection
    RabbitMQContainer rabbitContainer() {
        return new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.3.2-management"));
    }
}
