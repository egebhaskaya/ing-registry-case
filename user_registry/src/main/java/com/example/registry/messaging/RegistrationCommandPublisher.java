package com.example.registry.messaging;

import com.example.registry.config.RegistryRabbitProperties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/** Publishes commands for the asynchronous endpoint and checks broker confirmation and routing. */
@Component
public class RegistrationCommandPublisher {
    private final RabbitTemplate rabbitTemplate;
    private final RegistryRabbitProperties props;

    public RegistrationCommandPublisher(RabbitTemplate rabbitTemplate, RegistryRabbitProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    public PublishOutcome publish(RegisterUserCommand command) {
        CorrelationData correlation = new CorrelationData();
        rabbitTemplate.convertAndSend(props.exchange(), props.routingKey(), command, correlation);
        return outcome(correlation);
    }

    private PublishOutcome outcome(CorrelationData correlation) {
        try {
            CorrelationData.Confirm confirm = correlation.getFuture()
                    .get(props.confirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.isAck()) {
                // A channel-loss nack can follow a successful send; a broker nack without a reason is definite.
                return confirm.getReason() == null ? PublishOutcome.REJECTED : PublishOutcome.UNKNOWN;
            }
            return correlation.getReturned() == null ? PublishOutcome.CONFIRMED : PublishOutcome.UNROUTABLE;
        } catch (TimeoutException | ExecutionException e) {
            return PublishOutcome.UNKNOWN;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return PublishOutcome.UNKNOWN;
        }
    }
}
