package com.example.registry.messaging;

import com.example.registry.config.RegistryRabbitProperties;
import com.example.registry.exceptions.IntakeOutcomeUnknownException;
import com.example.registry.exceptions.IntakeRejectedException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/** Both modes publish to the same queue; only the synchronous mode requests a worker reply. */
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

    public RegistrationReply publishAndWait(RegisterUserCommand command) {
        CorrelationData correlation = new CorrelationData();
        Object reply;
        try {
            reply = rabbitTemplate.convertSendAndReceive(props.exchange(), props.routingKey(), command, correlation);
        } catch (AmqpMessageReturnedException e) {
            throw new IntakeRejectedException("unroutable");
        } catch (AmqpConnectException e) {
            throw e;
        } catch (AmqpException e) {
            // Losing the reply channel cannot establish that the command failed to reach the queue.
            throw new IntakeOutcomeUnknownException();
        }
        if (reply instanceof RegistrationReply result) {
            // A worker result is stronger evidence than a publisher confirm: the transaction has finished.
            return result;
        }
        if (reply != null) {
            throw new IllegalStateException("Unexpected registration reply type");
        }
        switch (outcome(correlation)) {
            case REJECTED -> throw new IntakeRejectedException("broker_rejected");
            case UNROUTABLE -> throw new IntakeRejectedException("unroutable");
            case UNKNOWN -> throw new IntakeOutcomeUnknownException();
            case CONFIRMED -> throw new IntakeOutcomeUnknownException("reply_timeout");
        }
        throw new IllegalStateException("Unreachable publisher outcome");
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
