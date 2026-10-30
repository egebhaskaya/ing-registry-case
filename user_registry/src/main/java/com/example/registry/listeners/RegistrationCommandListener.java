package com.example.registry.listeners;

import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationReply;
import com.example.registry.services.RegistrationProcessor;
import com.example.registry.services.RegistrationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** Both HTTP routes use this worker. A reply and delivery acknowledgement follow the DB commit. */
@Component
public class RegistrationCommandListener {
    private static final Logger log = LoggerFactory.getLogger(RegistrationCommandListener.class);
    private final RegistrationProcessor processor;

    public RegistrationCommandListener(RegistrationProcessor processor) {
        this.processor = processor;
    }

    @RabbitListener(queues = "${registry.rabbit.queue}")
    public RegistrationReply onRegisterUser(RegisterUserCommand command,
            @Header(name = AmqpHeaders.REPLY_TO, required = false) String replyTo) {
        try {
            RegistrationResult result = processor.process(command);
            log.info("registration processed registrationId={} outcome={}", command.registrationId(), result.outcome());
            return replyTo == null ? null : RegistrationReply.completed(result);
        } catch (ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            if (replyTo != null) {
                return RegistrationReply.conflict(e instanceof ConflictingPayloadException
                        ? "idempotency_conflict" : "email_already_registered");
            }
            // Asynchronous callers receive permanent failures through the existing DLQ.
            throw new AmqpRejectAndDontRequeueException(e.getMessage(), e);
        }
    }
}
