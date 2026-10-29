package com.example.registry.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The {@code registry.rabbit.*} block of application.yml, bound to a record.
 *
 * <p>
 * Spring maps kebab-case keys to the record components ({@code routing-key}
 * becomes {@code routingKey},
 * {@code 5s} becomes a {@link Duration}). Keeping the names in one typed object
 * means the publisher, the
 * topology declaration and the listener cannot drift apart on a queue name.
 *
 * @param exchange           direct exchange the API publishes to
 * @param routingKey         routing key that binds the exchange to the command
 *                           queue
 * @param queue              durable queue the consumers read
 * @param deadLetterExchange fanout exchange rejected messages are dead-lettered
 *                           to
 * @param deadLetterQueue    durable queue bound to the dead-letter exchange
 *                           (design section 5: the DLQ)
 * @param queueMaxLength     bound on queued messages; overflow rejects new
 *                           publishes (design section 5)
 * @param confirmTimeout     finite wait for the broker publisher confirmation
 *                           (design section 4)
 */
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
