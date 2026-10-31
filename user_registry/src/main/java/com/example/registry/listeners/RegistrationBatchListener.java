package com.example.registry.listeners;

import com.example.registry.messaging.PublishOutcome;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationCommandPublisher;
import com.example.registry.services.RegistrationProcessor;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

/** Collects independent deliveries into one database transaction; Rabbit acknowledges after commit. */
@Component
public class RegistrationBatchListener {
    private static final Logger log = LoggerFactory.getLogger(RegistrationBatchListener.class);
    private final RegistrationProcessor processor;
    private final RegistrationCommandPublisher publisher;
    private final MessageConverter converter;

    public RegistrationBatchListener(RegistrationProcessor processor, RegistrationCommandPublisher publisher,
            MessageConverter converter) {
        this.processor = processor;
        this.publisher = publisher;
        this.converter = converter;
    }

    @RabbitListener(id = "registration-batched", queues = "${registry.batch.queue}",
            containerFactory = "registrationBatchContainerFactory")
    public void onRegisterUsers(List<Message> messages) {
        List<RegisterUserCommand> commands = new ArrayList<>();
        List<Message> originals = new ArrayList<>();
        List<Message> retryIndividually = new ArrayList<>();
        for (Message message : messages) {
            try {
                if (converter.fromMessage(message) instanceof RegisterUserCommand command) {
                    commands.add(command);
                    originals.add(message);
                } else {
                    retryIndividually.add(message);
                }
            } catch (RuntimeException e) {
                // A malformed delivery must not prevent its valid neighbors from committing.
                retryIndividually.add(message);
            }
        }

        if (!commands.isEmpty()) {
            try {
                for (var failed : processor.processBatch(commands)) {
                    retryIndividually.add(originals.get(failed.index()));
                }
            } catch (RuntimeException e) {
                log.warn("Registration batch failed; using individual retries for {} deliveries", commands.size(), e);
                retryIndividually.addAll(originals);
            }
        }

        for (Message message : retryIndividually) {
            try {
                if (publisher.retryIndividually(message) != PublishOutcome.CONFIRMED) {
                    throw new ImmediateRequeueAmqpException("Individual retry was not confirmed by RabbitMQ");
                }
            } catch (org.springframework.amqp.AmqpException e) {
                // Previously forwarded items can be delivered again; registration idempotency protects them.
                throw new ImmediateRequeueAmqpException("Could not safely forward the batch item for retry", e);
            }
        }
    }
}
