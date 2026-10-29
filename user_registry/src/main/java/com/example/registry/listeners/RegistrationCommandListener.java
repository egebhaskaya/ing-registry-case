package com.example.registry.listeners;

import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.services.RegistrationOutcome;
import com.example.registry.services.RegistrationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * The bounded registration consumer (design section 5, "Registry consumer").
 *
 * <p>{@code @RabbitListener} makes Spring run {@link #onRegisterUser} for every message on the queue,
 * on the number of consumers configured by {@code spring.rabbitmq.listener.simple.concurrency}
 * (4 by default). Each consumer works on a virtual thread, but there are still only 4 of them:
 * concurrency is the bound, not the thread type (design section 6).
 *
 * <p>The processing path is deliberately synchronous: this method calls the transactional service and
 * waits. When it returns normally, Spring acknowledges the delivery, which is after the commit, so a
 * crash in between only causes a harmless redelivery. No {@code @Async}, no futures, no extra executor.
 *
 * <p>When it throws, the retry settings in application.yml decide what happens: a transient failure
 * (for example a database connection timeout) is retried in memory on this same consumer, up to three
 * times with growing delays; a permanent one ({@link AmqpRejectAndDontRequeueException}) is not retried
 * at all. In both cases the end of the road is the recoverer in {@code RabbitConfig}: the command is
 * republished to the dead-letter exchange with the failure reason in its headers and the original
 * delivery is then acknowledged. The broker's own dead-lettering (the queue's DLX argument) is only
 * the fallback for the case where the recoverer itself cannot publish.
 *
 * <p>Measurement (design section 7): the completion timer runs from the moment the API accepted the
 * request ({@code submittedAt}, carried in the command) to the commit, so it includes queue waiting.
 * It is recorded only for a newly created registration, never for duplicates or failures.
 */
@Component
public class RegistrationCommandListener {

    private static final Logger log = LoggerFactory.getLogger(RegistrationCommandListener.class);

    private final RegistrationProcessor processor;

    public RegistrationCommandListener(RegistrationProcessor processor) {
        this.processor = processor;
    }

    @RabbitListener(queues = "${registry.rabbit.queue}")
    public void onRegisterUser(RegisterUserCommand command) {
        RegistrationOutcome outcome;
        try {
            outcome = processor.processConsumer(command).outcome();
        } catch (ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            // Permanent business conflict: retrying can never succeed, so send it straight to the DLQ.
            log.warn("registration rejected permanently registrationId={} reason={}",
                    command.registrationId(), e.getMessage());
            throw new AmqpRejectAndDontRequeueException(e.getMessage(), e);
        }

        log.info("registration processed registrationId={} outcome={}", command.registrationId(), outcome);
    }
}
