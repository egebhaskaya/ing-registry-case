package com.example.registry.listeners;

import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.services.RegistrationOutcome;
import com.example.registry.services.RegistrationService;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.dao.DuplicateKeyException;
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

    private final RegistrationService registrationService;
    private final RegistryMetrics metrics;

    public RegistrationCommandListener(RegistrationService registrationService, RegistryMetrics metrics) {
        this.registrationService = registrationService;
        this.metrics = metrics;
    }

    @RabbitListener(queues = "${registry.rabbit.queue}")
    public void onRegisterUser(RegisterUserCommand command) {
        RegistrationOutcome outcome;
        try {
            outcome = registerOrResolve(command);
        } catch (ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            // Permanent business conflict: retrying can never succeed, so send it straight to the DLQ.
            metrics.registration("conflict");
            log.warn("registration rejected permanently registrationId={} reason={}",
                    command.registrationId(), e.getMessage());
            throw new AmqpRejectAndDontRequeueException(e.getMessage(), e);
        } catch (RuntimeException e) {
            // Anything else is treated as transient: counted, then handed to the retry machinery.
            metrics.registrationFailed();
            throw e;
        }

        metrics.registration(outcome.name().toLowerCase(Locale.ROOT));
        if (outcome == RegistrationOutcome.CREATED) {
            metrics.completion(Duration.between(command.submittedAt(), Instant.now()));
        }
        log.info("registration processed registrationId={} outcome={}", command.registrationId(), outcome);
    }

    private RegistrationOutcome registerOrResolve(RegisterUserCommand command) {
        try {
            return registrationService.register(command);
        } catch (DuplicateKeyException e) {
            // A unique constraint fired, which means two deliveries raced or the email already exists.
            // That transaction is already rolled back; classify the situation in a fresh one.
            return registrationService.resolveDuplicate(command);
        }
    }
}
