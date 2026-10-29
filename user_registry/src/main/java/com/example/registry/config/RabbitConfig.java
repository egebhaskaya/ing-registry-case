package com.example.registry.config;

import java.util.List;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.amqp.autoconfigure.RabbitListenerRetrySettingsCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the RabbitMQ topology (design section 5), the JSON message format, and what happens to a
 * command that cannot be processed.
 *
 * <p>Every {@code @Bean} of type Queue, Exchange or Binding is declared on the broker automatically by
 * Spring's RabbitAdmin when the first connection opens, so the topology lives in code, not in a manual
 * broker setup.
 *
 * <pre>
 *   API --publish--> [registration] --registration.register--> (registration.commands) --> consumers
 *                                                                      | retries exhausted / permanent
 *                                                                      v
 *                                                      [registration.dlx] --> (registration.commands.dlq)
 * </pre>
 * Queue overflow is not dead-lettered: with reject-publish the broker NACKs the publisher instead.
 */
@Configuration
@EnableConfigurationProperties({RegistryRabbitProperties.class, IntakeProperties.class})
public class RabbitConfig {

    /** Direct exchange: a message goes to the queue(s) bound with exactly its routing key. */
    @Bean
    DirectExchange registrationExchange(RegistryRabbitProperties props) {
        return ExchangeBuilder.directExchange(props.exchange()).durable(true).build();
    }

    /** Fanout dead-letter exchange: whatever the original routing key, a rejected message reaches the DLQ. */
    @Bean
    FanoutExchange registrationDeadLetterExchange(RegistryRabbitProperties props) {
        return ExchangeBuilder.fanoutExchange(props.deadLetterExchange()).durable(true).build();
    }

    /**
     * The command queue. Durable means it survives a broker restart. Rejected (not requeued) messages go
     * to the dead-letter exchange. maxLength plus reject-publish means that when the queue is full the
     * broker NACKs new publishes (the API then answers 503) instead of silently dropping work it already
     * accepted (design section 5).
     */
    @Bean
    Queue registrationQueue(RegistryRabbitProperties props) {
        return QueueBuilder.durable(props.queue())
                .deadLetterExchange(props.deadLetterExchange())
                .maxLength(props.queueMaxLength())
                .overflow(QueueBuilder.Overflow.rejectPublish)
                .build();
    }

    @Bean
    Queue registrationDeadLetterQueue(RegistryRabbitProperties props) {
        return QueueBuilder.durable(props.deadLetterQueue()).build();
    }

    @Bean
    Binding registrationBinding(@Qualifier("registrationQueue") Queue queue,
                                DirectExchange registrationExchange,
                                RegistryRabbitProperties props) {
        return BindingBuilder.bind(queue).to(registrationExchange).with(props.routingKey());
    }

    @Bean
    Binding deadLetterBinding(@Qualifier("registrationDeadLetterQueue") Queue dlq,
                              FanoutExchange registrationDeadLetterExchange) {
        return BindingBuilder.bind(dlq).to(registrationDeadLetterExchange);
    }

    /**
     * Messages are JSON. Spring Boot applies this converter to both the RabbitTemplate (sending) and the
     * listener containers (receiving), so a {@code RegisterUserCommand} record travels as readable JSON
     * that can be inspected in the management UI.
     */
    @Bean
    MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    /**
     * What happens when retries are exhausted (or skipped): the message is republished to the dead-letter
     * exchange with {@code x-exception-message}, {@code x-exception-stacktrace} and {@code x-original-*}
     * headers, and the original delivery is then acknowledged. That gives the DLQ a recorded failure
     * reason (design section 5), which the broker's own dead-lettering cannot provide. The stack trace
     * header is opt-in in Spring AMQP 4 ({@code includeStackTrace(true)}); it is truncated to fit the
     * broker's frame size.
     */
    @Bean
    MessageRecoverer deadLetterRecoverer(RabbitTemplate rabbitTemplate, RegistryRabbitProperties props,
                                         RegistryMetrics metrics) {
        RepublishMessageRecoverer republish =
                new RepublishMessageRecoverer(rabbitTemplate, props.deadLetterExchange(), props.routingKey())
                        .includeStackTrace(true);
        // MessageRecoverer is a functional interface: republish, then count it for the dashboard.
        return (message, cause) -> {
            republish.recover(message, cause);
            metrics.deadLettered();
        };
    }

    /**
     * Which failures are worth retrying. A permanent conflict is signalled by throwing
     * {@link AmqpRejectAndDontRequeueException}, and a message that cannot even be converted into a
     * command will not convert better the fourth time; both go straight to the recoverer. Everything else
     * (connection timeouts, lock waits...) gets the backoff schedule from application.yml. Spring's retry
     * policy matches the exception and its causes, so the wrapper the listener container adds around our
     * exception does not hide it.
     */
    @Bean
    RabbitListenerRetrySettingsCustomizer skipRetryForPermanentFailures() {
        return settings -> settings.setExceptionExcludes(
                List.of(AmqpRejectAndDontRequeueException.class, MessageConversionException.class));
    }
}
