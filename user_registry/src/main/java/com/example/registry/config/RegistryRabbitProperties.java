package com.example.registry.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Rabbit topology and publisher-confirm timeout, bound from registry.rabbit.* in application.properties. */
@ConfigurationProperties("registry.rabbit")
public record RegistryRabbitProperties(
                String exchange,
                String routingKey,
                String queue,
                String deadLetterExchange,
                String deadLetterQueue,
                int queueMaxLength,
                Duration confirmTimeout) {
}
