package com.example.registry.services;

import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.messaging.RegisterUserCommand;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

/** Runs the shared transaction for direct HTTP calls and Rabbit workers, measuring committed outcomes. */
@Service
public class RegistrationProcessor {
    private static final Logger log = LoggerFactory.getLogger(RegistrationProcessor.class);
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

    /** Failed items are returned for the listener to hand to the normal queue's retry/DLQ path. */
    public List<BatchFailure> processBatch(List<RegisterUserCommand> commands) {
        if (commands.isEmpty()) {
            return List.of();
        }
        boolean isolateItems = false;
        long processingStarted = System.nanoTime();
        metrics.workerStarted();
        try {
            List<RegistrationResult> results = transactions.registerBatch(commands);
            Duration duration = Duration.ofNanos(System.nanoTime() - processingStarted);
            Instant committedAt = Instant.now();
            int createdCount = 0;
            for (int index = 0; index < results.size(); index++) {
                RegistrationResult result = results.get(index);
                metrics.registration(result.outcome().name().toLowerCase(Locale.ROOT));
                if (result.outcome() == RegistrationOutcome.CREATED) {
                    // Every new item waits through this shared commit; transaction metrics record it once separately.
                    metrics.processing(RegistrationMode.BATCHED, duration);
                    metrics.completion(Duration.between(commands.get(index).submittedAt(), committedAt));
                    createdCount++;
                }
            }
            if (createdCount > 0) {
                metrics.batchCommitted(createdCount, duration);
            }
            log.info("registration batch committed size={} created={} replayed={} durationMs={}",
                    results.size(), createdCount, results.size() - createdCount, duration.toMillis());
        } catch (DuplicateKeyException | ConflictingPayloadException | EmailAlreadyRegisteredException e) {
            // The bulk proxy has rolled back completely before any per-item transaction starts.
            isolateItems = true;
        } catch (RuntimeException e) {
            metrics.registrationFailed();
            throw e;
        } finally {
            metrics.workerFinished();
        }
        if (!isolateItems) {
            return List.of();
        }

        List<BatchFailure> failures = new ArrayList<>();
        for (int index = 0; index < commands.size(); index++) {
            try {
                process(commands.get(index), RegistrationMode.BATCHED);
            } catch (RuntimeException e) {
                failures.add(new BatchFailure(index, e));
            }
        }
        return List.copyOf(failures);
    }

    public record BatchFailure(int index, RuntimeException cause) {
    }
}
