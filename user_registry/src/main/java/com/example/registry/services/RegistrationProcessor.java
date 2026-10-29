package com.example.registry.services;

import com.example.registry.config.ProcessingProperties;
import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.exceptions.SynchronousRegistrationOverloadedException;
import com.example.registry.messaging.RegisterUserCommand;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

/**
 * Bounds registration work before the transactional bean acquires a database connection.
 * HTTP waits have a bounded count and duration. Rabbit waiters are bounded by listener concurrency
 * and wait for capacity without turning an already accepted command into an overload failure.
 */
@Service
@EnableConfigurationProperties(ProcessingProperties.class)
public class RegistrationProcessor {
    private final RegistrationService transactions;
    private final RegistryMetrics metrics;
    private final ProcessingProperties properties;
    private final Semaphore execution;
    private final Semaphore httpWaiters;

    public RegistrationProcessor(RegistrationService transactions, RegistryMetrics metrics,
            ProcessingProperties properties, MeterRegistry meterRegistry) {
        this.transactions = transactions;
        this.metrics = metrics;
        this.properties = properties;
        this.execution = new Semaphore(properties.maxConcurrency(), true);
        this.httpWaiters = new Semaphore(properties.maxWaitingRequests());
        Gauge.builder("registry.config.processing.concurrency", properties, ProcessingProperties::maxConcurrency)
                .description("Shared registration execution limit per instance")
                .register(meterRegistry);
        Gauge.builder("registry.processing.active", this,
                processor -> properties.maxConcurrency() - processor.availablePermits())
                .description("Registration executions holding capacity, including commit and duplicate resolution")
                .register(meterRegistry);
        Gauge.builder("registry.processing.waiting.requests", this, RegistrationProcessor::waitingRequests)
                .description("HTTP requests waiting for registration capacity")
                .register(meterRegistry);
    }

    public RegistrationResult processHttp(RegisterUserCommand command) {
        if (!admitHttp()) {
            throw new SynchronousRegistrationOverloadedException();
        }
        try {
            return execute(command);
        } finally {
            execution.release();
        }
    }

    public RegistrationResult processConsumer(RegisterUserCommand command) {
        try {
            execution.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for registration capacity", e);
        }
        try {
            return execute(command);
        } finally {
            execution.release();
        }
    }

    public int availablePermits() {
        return execution.availablePermits();
    }

    public int waitingRequests() {
        return properties.maxWaitingRequests() - httpWaiters.availablePermits();
    }

    private boolean admitHttp() {
        try {
            // Timed tryAcquire(0) respects fairness, so HTTP cannot jump ahead of waiting consumers.
            if (execution.tryAcquire(0, TimeUnit.NANOSECONDS)) {
                return true;
            }
            if (!httpWaiters.tryAcquire()) {
                return false;
            }
            try {
                return execution.tryAcquire(properties.admissionWait().toNanos(), TimeUnit.NANOSECONDS);
            } finally {
                httpWaiters.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private RegistrationResult execute(RegisterUserCommand command) {
        try {
            RegistrationResult result;
            try {
                result = transactions.registerWithResult(command);
            } catch (DuplicateKeyException e) {
                // The losing transaction has rolled back before the new read-only transaction starts.
                result = transactions.resolveDuplicateWithResult(command);
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
