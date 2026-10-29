package com.example.registry.services;

import com.example.registry.config.RegistryMetrics;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.exceptions.SynchronousRegistrationOverloadedException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** Executes on the HTTP handler thread and returns only after the Registry transaction commits. */
@Service
public class SynchronousRegistrationService {
    private final RegistrationNormalizer normalizer;
    private final RegistrationProcessor processor;
    private final RegistryMetrics metrics;

    public SynchronousRegistrationService(RegistrationNormalizer normalizer,
            RegistrationProcessor processor, RegistryMetrics metrics) {
        this.normalizer = normalizer;
        this.processor = processor;
        this.metrics = metrics;
    }

    public RegistrationResult register(UUID registrationId, RegistrationRequest request) {
        Instant submittedAt = Instant.now();
        RegistrationPayload payload = normalizer.normalize(request);
        RegisterUserCommand command = new RegisterUserCommand(registrationId,
                normalizer.fingerprint(payload), payload, submittedAt);
        try {
            RegistrationResult result = processor.processHttp(command);
            metrics.synchronous(result.outcome() == RegistrationOutcome.CREATED ? "created" : "replayed");
            return result;
        } catch (SynchronousRegistrationOverloadedException e) {
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
}
