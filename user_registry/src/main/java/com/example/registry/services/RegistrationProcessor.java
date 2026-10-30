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

/** Runs the shared transaction and measures its committed outcome. Rabbit consumers bound concurrency. */
@Service
public class RegistrationProcessor {
    private final RegistrationService transactions;
    private final RegistryMetrics metrics;

    public RegistrationProcessor(RegistrationService transactions, RegistryMetrics metrics) {
        this.transactions = transactions;
        this.metrics = metrics;
    }

    public RegistrationResult process(RegisterUserCommand command) {
        try {
            RegistrationResult result;
            try {
                result = transactions.register(command);
            } catch (DuplicateKeyException e) {
                // The failed insert has rolled back before this new read-only transaction starts.
                result = transactions.resolveDuplicate(command);
            }
            metrics.registration(result.outcome().name().toLowerCase(Locale.ROOT));
            if (result.outcome() == RegistrationOutcome.CREATED) {
                metrics.completion(Duration.between(command.submittedAt(), Instant.now()));
            }
            return result;
        } catch (ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            metrics.registration("conflict");
            throw e;
        } catch (RuntimeException e) {
            metrics.registrationFailed();
            throw e;
        }
    }
}
