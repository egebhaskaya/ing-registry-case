package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.models.User;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.mock.env.MockEnvironment;

class RegistrationWorkerMetricsTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void countsBlockedProcessingAttemptsAndReleasesThemAfterSuccessOrFailure(boolean fail) throws Exception {
        var registry = new SimpleMeterRegistry();
        try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
            RegistryMetrics metrics = new RegistryMetrics(registry, new RabbitProperties(), new MockEnvironment());
            RegistrationService transactions = mock(RegistrationService.class);
            RegistrationProcessor processor = new RegistrationProcessor(transactions, metrics);
            RegisterUserCommand command = new RegisterUserCommand(UUID.randomUUID(), "fingerprint",
                    new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"), Instant.now());
            User user = new User(UUID.randomUUID(), command.registrationId(), command.payloadFingerprint(),
                    command.payload().email(), command.payload().fullName(), command.payload().countryCode(), Instant.now());
            CountDownLatch enteredTransaction = new CountDownLatch(2);
            CountDownLatch finishTransaction = new CountDownLatch(1);
            when(transactions.register(any())).thenAnswer(invocation -> {
                enteredTransaction.countDown();
                assertThat(finishTransaction.await(5, TimeUnit.SECONDS)).isTrue();
                if (fail) {
                    throw new IllegalStateException("database unavailable");
                }
                return new RegistrationResult(RegistrationOutcome.CREATED, user);
            });
            var first = requests.submit(() -> processor.process(command));
            var second = requests.submit(() -> processor.process(command));
            try {
                assertThat(enteredTransaction.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(registry.get("registry.workers.active").gauge().value()).isEqualTo(2);
                assertThat(registry.get("registry.registration.completion").timer().count()).isZero();
            } finally {
                finishTransaction.countDown();
            }
            if (fail) {
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(IllegalStateException.class);
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(IllegalStateException.class);
                assertThat(registry.get("registry.registrations.failures").counter().count()).isEqualTo(2);
                assertThat(registry.get("registry.registration.completion").timer().count()).isZero();
            } else {
                assertThat(first.get(5, TimeUnit.SECONDS).user()).isEqualTo(user);
                assertThat(second.get(5, TimeUnit.SECONDS).user()).isEqualTo(user);
                assertThat(registry.get("registry.registration.completion").timer().count()).isEqualTo(2);
            }
            assertThat(registry.get("registry.workers.active").gauge().value()).isZero();
        } finally {
            registry.close();
        }
    }

    @Test
    void businessConflictAlsoReleasesTheWorkerGauge() {
        var registry = new SimpleMeterRegistry();
        try {
            RegistryMetrics metrics = new RegistryMetrics(registry, new RabbitProperties(), new MockEnvironment());
            RegistrationService transactions = mock(RegistrationService.class);
            RegistrationProcessor processor = new RegistrationProcessor(transactions, metrics);
            RegisterUserCommand command = new RegisterUserCommand(UUID.randomUUID(), "fingerprint",
                    new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"), Instant.now());
            when(transactions.register(command)).thenThrow(new ConflictingPayloadException(command.registrationId()));

            assertThatThrownBy(() -> processor.process(command)).isInstanceOf(ConflictingPayloadException.class);

            assertThat(registry.get("registry.workers.active").gauge().value()).isZero();
            assertThat(registry.get("registry.registrations").tag("outcome", "conflict").counter().count()).isEqualTo(1);
        } finally {
            registry.close();
        }
    }
}
