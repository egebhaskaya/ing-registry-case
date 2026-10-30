package com.example.registry.listeners;

import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.services.RegistrationProcessor;
import com.example.registry.services.RegistrationMode;
import com.example.registry.services.RegistrationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/** Processes asynchronous registrations; the delivery acknowledgement follows the DB commit. */
@Component
public class RegistrationCommandListener {
    private static final Logger log = LoggerFactory.getLogger(RegistrationCommandListener.class);
    private final RegistrationProcessor processor;

    public RegistrationCommandListener(RegistrationProcessor processor) {
        this.processor = processor;
    }

    @RabbitListener(queues = "${registry.rabbit.queue}")
    public void onRegisterUser(RegisterUserCommand command) {
        try {
            RegistrationResult result = processor.process(command, RegistrationMode.ASYNC);
            log.info("registration processed registrationId={} outcome={}", command.registrationId(), result.outcome());
        } catch (ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            // Asynchronous callers receive permanent failures through the existing DLQ.
            throw new AmqpRejectAndDontRequeueException(e.getMessage(), e);
        }
    }
}
