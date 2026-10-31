package com.example.registry.messaging;

import com.example.registry.config.RegistryRabbitProperties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
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
        return publish(command, props.routingKey());
    }

    public PublishOutcome publish(RegisterUserCommand command, String routingKey) {
        CorrelationData correlation = new CorrelationData();
        rabbitTemplate.convertAndSend(props.exchange(), routingKey, command, correlation);
        return outcome(correlation);
    }

    /** Failed batch items use the existing single-message retry and DLQ path, retaining their headers. */
    public PublishOutcome retryIndividually(Message message) {
        CorrelationData correlation = new CorrelationData();
        // Received delivery mode is a separate property; restore persistence on the outgoing copy.
        Message retry = MessageBuilder.fromMessage(message)
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT).build();
        rabbitTemplate.send(props.exchange(), props.routingKey(), retry, correlation);
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
