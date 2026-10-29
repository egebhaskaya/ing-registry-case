package com.example.registry.messaging;

import com.example.registry.config.RegistryRabbitProperties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes a {@link RegisterUserCommand} and waits for the broker verdict.
 *
 * <p>
 * This is the heart of the honest 202: the HTTP thread blocks here until
 * RabbitMQ has confirmed the
 * message (or the timeout elapses). On a virtual thread that wait is cheap
 * (design section 6), so
 * blocking is the simplest correct design. No callbacks, no detached futures.
 */
@Component
public class RegistrationCommandPublisher {

    private static final Logger log = LoggerFactory.getLogger(RegistrationCommandPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final RegistryRabbitProperties props;

    public RegistrationCommandPublisher(RabbitTemplate rabbitTemplate, RegistryRabbitProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    /**
     * Sends the command and returns what the broker reported. A connection failure
     * propagates as an
     * {@code AmqpException}; the intake service maps that to a 503.
     */
    public PublishOutcome publish(RegisterUserCommand command) {
        // CorrelationData ties this exact message to its ack/nack and to any "returned"
        // notification.
        CorrelationData correlation = new CorrelationData(command.registrationId().toString());
        rabbitTemplate.convertAndSend(props.exchange(), props.routingKey(), command, correlation);
        try {
            CorrelationData.Confirm confirm = correlation.getFuture()
                    .get(props.confirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.isAck()) {
                // Two kinds of nack arrive here. A real broker nack (for example queue overflow
                // with
                // reject-publish) carries no reason: the message is definitely not queued. A
                // nack that
                // Spring AMQP synthesizes because the channel died before the broker answered
                // carries a
                // reason text, and then the message may already be queued, so it must be
                // reported as
                // unknown (design section 4), not as a definite failure.
                if (confirm.getReason() != null) {
                    log.warn("publish outcome unknown registrationId={} channel reason={}",
                            command.registrationId(), confirm.getReason());
                    return PublishOutcome.UNKNOWN;
                }
                log.warn("publish rejected by broker registrationId={}", command.registrationId());
                return PublishOutcome.REJECTED;
            }
            if (correlation.getReturned() != null) {
                // ack plus returned: the exchange exists but nothing was bound for our routing
                // key
                log.warn("publish unroutable registrationId={} replyText={}",
                        command.registrationId(), correlation.getReturned().getReplyText());
                return PublishOutcome.UNROUTABLE;
            }
            return PublishOutcome.CONFIRMED;
        } catch (TimeoutException | ExecutionException e) {
            log.warn("publish outcome unknown registrationId={} cause={}", command.registrationId(), e.toString());
            return PublishOutcome.UNKNOWN;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return PublishOutcome.UNKNOWN;
        }
    }
}
