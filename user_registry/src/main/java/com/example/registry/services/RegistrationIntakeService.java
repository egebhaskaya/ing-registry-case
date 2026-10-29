package com.example.registry.services;

import com.example.registry.config.IntakeProperties;
import com.example.registry.config.RegistryMetrics;
import com.example.registry.dtos.RegistrationAcceptedResponse;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.exceptions.IntakeOutcomeUnknownException;
import com.example.registry.exceptions.IntakeOverloadedException;
import com.example.registry.exceptions.IntakeRejectedException;
import com.example.registry.messaging.PublishOutcome;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationCommandPublisher;
import com.example.registry.messaging.RegistrationPayload;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.stereotype.Service;

/**
 * HTTP intake orchestration (design section 4): admission control, normalization, publish, and the
 * mapping of the broker verdict to an HTTP outcome.
 *
 * <p>This service never touches the database. Accepting a request means "the command is safely in
 * RabbitMQ", and that is all the 202 promises. Every outcome is counted in {@link RegistryMetrics}.
 */
@Service
public class RegistrationIntakeService {

    private static final Logger log = LoggerFactory.getLogger(RegistrationIntakeService.class);

    private final RegistrationNormalizer normalizer;
    private final RegistrationCommandPublisher publisher;
    private final IntakeProperties props;
    private final RegistryMetrics metrics;

    /**
     * The admission limit. A Semaphore is a counter of permits: acquire takes one (waiting if none is
     * free), release gives it back. With maxInFlight permits, at most that many requests can be inside
     * publish-and-wait at once, regardless of how many virtual threads Tomcat creates (design section 6).
     */
    private final Semaphore admission;

    public RegistrationIntakeService(RegistrationNormalizer normalizer,
                                     RegistrationCommandPublisher publisher,
                                     IntakeProperties props,
                                     RegistryMetrics metrics) {
        this.normalizer = normalizer;
        this.publisher = publisher;
        this.props = props;
        this.metrics = metrics;
        this.admission = new Semaphore(props.maxInFlight());
    }

    public RegistrationAcceptedResponse accept(UUID registrationId, RegistrationRequest request) {
        if (!tryAdmit()) {
            metrics.intake("overloaded");
            throw new IntakeOverloadedException();
        }
        try {
            RegistrationPayload payload = normalizer.normalize(request);
            RegisterUserCommand command = new RegisterUserCommand(
                    registrationId, normalizer.fingerprint(payload), payload, Instant.now());

            PublishOutcome outcome = publishOrFail(command);

            // A switch expression must cover every enum value, so adding an outcome later is a compile error here.
            return switch (outcome) {
                case CONFIRMED -> {
                    metrics.intake("accepted");
                    log.info("registration accepted registrationId={}", registrationId);
                    yield RegistrationAcceptedResponse.accepted(registrationId, Instant.now());
                }
                case REJECTED -> {
                    metrics.intake("broker_rejected");
                    throw new IntakeRejectedException("broker_rejected");
                }
                case UNROUTABLE -> {
                    metrics.intake("unroutable");
                    throw new IntakeRejectedException("unroutable");
                }
                case UNKNOWN -> {
                    metrics.intake("confirm_timeout");
                    throw new IntakeOutcomeUnknownException();
                }
            };
        } finally {
            // Every path out of the try block, success or exception, gives the permit back.
            admission.release();
        }
    }

    /** Number of free admission slots; exposed for tests and diagnostics. */
    public int availablePermits() {
        return admission.availablePermits();
    }

    private boolean tryAdmit() {
        try {
            return admission.tryAcquire(props.admissionWait().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private PublishOutcome publishOrFail(RegisterUserCommand command) {
        try {
            return publisher.publish(command);
        } catch (AmqpException e) {
            // Connection-level failure: nothing was sent, so this is a definite rejection, not "unknown".
            log.warn("broker unavailable registrationId={} cause={}", command.registrationId(), e.toString());
            metrics.intake("broker_unavailable");
            throw new IntakeRejectedException("broker_unavailable");
        }
    }
}
