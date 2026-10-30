package com.example.registry.services;

import com.example.registry.config.IntakeProperties;
import com.example.registry.config.RegistryMetrics;
import com.example.registry.dtos.RegistrationAcceptedResponse;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.exceptions.IntakeOutcomeUnknownException;
import com.example.registry.exceptions.IntakeOverloadedException;
import com.example.registry.exceptions.IntakeRejectedException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationCommandPublisher;
import com.example.registry.messaging.RegistrationPayload;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.amqp.AmqpException;
import org.springframework.stereotype.Service;

/** Shared normalization and admission; async publishes to RabbitMQ, sync processes directly. */
@Service
public class RegistrationRequestService {
    private final RegistrationNormalizer normalizer;
    private final RegistrationCommandPublisher publisher;
    private final RegistrationProcessor processor;
    private final IntakeProperties props;
    private final RegistryMetrics metrics;
    private final Semaphore admission;

    public RegistrationRequestService(RegistrationNormalizer normalizer, RegistrationCommandPublisher publisher,
            RegistrationProcessor processor, IntakeProperties props, RegistryMetrics metrics) {
        this.normalizer = normalizer;
        this.publisher = publisher;
        this.processor = processor;
        this.props = props;
        this.metrics = metrics;
        this.admission = new Semaphore(props.maxInFlight());
    }

    public RegistrationAcceptedResponse enqueue(UUID registrationId, RegistrationRequest request) {
        try {
            return admitted(() -> {
                switch (publisher.publish(command(registrationId, request))) {
                    case CONFIRMED -> {
                        metrics.intake("accepted");
                        return RegistrationAcceptedResponse.accepted(registrationId, Instant.now());
                    }
                    case REJECTED -> throw new IntakeRejectedException("broker_rejected");
                    case UNROUTABLE -> throw new IntakeRejectedException("unroutable");
                    case UNKNOWN -> throw new IntakeOutcomeUnknownException();
                }
                throw new IllegalStateException("Unreachable publisher outcome");
            });
        } catch (AmqpException e) {
            metrics.intake("broker_unavailable");
            throw new IntakeRejectedException("broker_unavailable");
        } catch (IntakeOverloadedException e) {
            metrics.intake("overloaded");
            throw e;
        } catch (IntakeRejectedException e) {
            metrics.intake(e.reason());
            throw e;
        } catch (IntakeOutcomeUnknownException e) {
            metrics.intake(e.reason());
            throw e;
        }
    }

    public RegistrationResult registerDirectly(UUID registrationId, RegistrationRequest request) {
        try {
            RegistrationResult result = admitted(
                    () -> processor.process(command(registrationId, request), RegistrationMode.SYNC));
            metrics.synchronous(result.outcome() == RegistrationOutcome.CREATED ? "created" : "replayed");
            return result;
        } catch (IntakeOverloadedException e) {
            metrics.synchronous("overloaded");
            throw e;
        } catch (ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            metrics.synchronous("conflict");
            throw e;
        } catch (RuntimeException e) {
            metrics.synchronous("failed");
            throw e;
        }
    }

    public int availablePermits() {
        return admission.availablePermits();
    }

    private RegisterUserCommand command(UUID id, RegistrationRequest request) {
        RegistrationPayload payload = normalizer.normalize(request);
        return new RegisterUserCommand(id, normalizer.fingerprint(payload), payload, Instant.now());
    }

    private <T> T admitted(Supplier<T> operation) {
        if (!tryAdmit()) {
            throw new IntakeOverloadedException();
        }
        try {
            return operation.get();
        } finally {
            // Sync retains its slot through the database commit.
            admission.release();
        }
    }

    private boolean tryAdmit() {
        try {
            return admission.tryAcquire(props.admissionWait().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
