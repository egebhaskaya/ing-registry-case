package com.example.registry.services;

import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

/** Runs the shared transaction for direct HTTP calls and Rabbit workers, measuring committed outcomes. */
@Service
public class RegistrationProcessor {
    private final RegistrationService transactions;
    private final RegistryMetrics metrics;

    public RegistrationProcessor(RegistrationService transactions, RegistryMetrics metrics) {
        this.transactions = transactions;
        this.metrics = metrics;
    }

    public RegistrationResult process(RegisterUserCommand command, RegistrationMode mode) {
        long processingStarted = System.nanoTime();
        metrics.workerStarted();
        try {
            RegistrationResult result;
            try {
                result = transactions.register(command);
            } catch (DuplicateKeyException e) {
                // The failed insert has rolled back before this new read-only transaction starts.
                result = transactions.resolveDuplicate(command);
            }
            if (result.outcome() == RegistrationOutcome.CREATED) {
                // The transaction proxy has committed; command.submittedAt is deliberately excluded.
                metrics.processing(mode, Duration.ofNanos(System.nanoTime() - processingStarted));
                metrics.completion(Duration.between(command.submittedAt(), Instant.now()));
            }
            metrics.registration(result.outcome().name().toLowerCase(Locale.ROOT));
            return result;
        } catch (ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            metrics.registration("conflict");
            throw e;
        } catch (RuntimeException e) {
            metrics.registrationFailed();
            throw e;
        } finally {
            metrics.workerFinished();
        }
    }
}
