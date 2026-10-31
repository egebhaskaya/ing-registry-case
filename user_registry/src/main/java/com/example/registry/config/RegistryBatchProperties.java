package com.example.registry.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Size is a maximum; maxWait also releases a partially filled batch. */
@ConfigurationProperties("registry.batch")
public record RegistryBatchProperties(int size, Duration maxWait, String queue, String routingKey) {
    public RegistryBatchProperties {
        if (size < 1 || maxWait == null || maxWait.toMillis() < 1) {
            throw new IllegalArgumentException("registry.batch.size and registry.batch.max-wait must be positive");
        }
        if (queue == null || queue.isBlank() || routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("registry.batch.queue and registry.batch.routing-key are required");
        }
    }
}
