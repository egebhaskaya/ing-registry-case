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
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.mock.env.MockEnvironment;

class RegistrationWorkerMetricsTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void countsBlockedProcessingAndRecordsOnlyCommittedNewWritesByMode(boolean fail) throws Exception {
        var registry = new SimpleMeterRegistry();
        try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
            RegistryMetrics metrics = new RegistryMetrics(registry, new RabbitProperties(), new MockEnvironment());
            RegistrationService transactions = mock(RegistrationService.class);
            RegistrationProcessor processor = new RegistrationProcessor(transactions, metrics);
            // Earlier submission represents queue/admission waiting, which the new processing timer excludes.
            RegisterUserCommand command = command(Instant.now().minusSeconds(600));
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
            Timer sync = processing(registry, "sync");
            Timer async = processing(registry, "async");
            var first = requests.submit(() -> processor.process(command, RegistrationMode.SYNC));
            var second = requests.submit(() -> processor.process(command, RegistrationMode.ASYNC));
            long blockedNanos;
            try {
                assertThat(enteredTransaction.await(5, TimeUnit.SECONDS)).isTrue();
                long blockedAt = System.nanoTime();
                assertThat(finishTransaction.await(20, TimeUnit.MILLISECONDS)).isFalse();
                blockedNanos = System.nanoTime() - blockedAt;
                assertThat(registry.get("registry.workers.active").gauge().value()).isEqualTo(2);
                assertThat(registry.get("registry.registration.completion").timer().count()).isZero();
                assertThat(sync.count()).isZero();
                assertThat(async.count()).isZero();
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
                assertThat(sync.count()).isZero();
                assertThat(async.count()).isZero();
            } else {
                assertThat(first.get(5, TimeUnit.SECONDS).user()).isEqualTo(user);
                assertThat(second.get(5, TimeUnit.SECONDS).user()).isEqualTo(user);
                assertThat(sync.count()).isEqualTo(1);
                assertThat(async.count()).isEqualTo(1);
                // Each observation includes the wait inside the transactional proxy, but not the old submittedAt.
                for (Timer timer : new Timer[] { sync, async }) {
                    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThanOrEqualTo(blockedNanos);
                    assertThat(timer.totalTime(TimeUnit.SECONDS)).isLessThan(60);
                }
                Timer completion = registry.get("registry.registration.completion").timer();
                assertThat(completion.count()).isEqualTo(2);
                assertThat(completion.totalTime(TimeUnit.SECONDS)).isGreaterThanOrEqualTo(1_200);
            }
            assertThat(registry.get("registry.workers.active").gauge().value()).isZero();
        } finally {
            registry.close();
        }
    }

    @ParameterizedTest
    @EnumSource(RegistrationMode.class)
    void businessConflictReleasesTheWorkerGaugeWithoutRecordingNewWriteProcessing(RegistrationMode mode) {
        var registry = new SimpleMeterRegistry();
        try {
            RegistryMetrics metrics = new RegistryMetrics(registry, new RabbitProperties(), new MockEnvironment());
            RegistrationService transactions = mock(RegistrationService.class);
            RegistrationProcessor processor = new RegistrationProcessor(transactions, metrics);
            RegisterUserCommand command = command(Instant.now());
            when(transactions.register(command)).thenThrow(new ConflictingPayloadException(command.registrationId()));

            assertThatThrownBy(() -> processor.process(command, mode)).isInstanceOf(ConflictingPayloadException.class);

            assertThat(registry.get("registry.workers.active").gauge().value()).isZero();
            assertThat(registry.get("registry.registrations").tag("outcome", "conflict").counter().count()).isEqualTo(1);
            assertThat(processing(registry, "sync").count()).isZero();
            assertThat(processing(registry, "async").count()).isZero();
            assertThat(registry.get("registry.registration.completion").timer().count()).isZero();
        } finally {
            registry.close();
        }
    }

    @ParameterizedTest
    @EnumSource(RegistrationMode.class)
    void replayDoesNotAddANewWriteProcessingObservation(RegistrationMode mode) {
        var registry = new SimpleMeterRegistry();
        try {
            RegistryMetrics metrics = new RegistryMetrics(registry, new RabbitProperties(), new MockEnvironment());
            RegistrationService transactions = mock(RegistrationService.class);
            RegistrationProcessor processor = new RegistrationProcessor(transactions, metrics);
            RegisterUserCommand command = command(Instant.now().minusSeconds(600));
            User stored = new User(UUID.randomUUID(), command.registrationId(), command.payloadFingerprint(),
                    command.payload().email(), command.payload().fullName(), command.payload().countryCode(), Instant.now());
            when(transactions.register(command)).thenReturn(new RegistrationResult(RegistrationOutcome.DUPLICATE_IGNORED, stored));

            assertThat(processor.process(command, mode).user()).isEqualTo(stored);

            assertThat(registry.get("registry.registrations").tag("outcome", "duplicate_ignored").counter().count()).isEqualTo(1);
            assertThat(processing(registry, "sync").count()).isZero();
            assertThat(processing(registry, "async").count()).isZero();
            assertThat(registry.get("registry.registration.completion").timer().count()).isZero();
        } finally {
            registry.close();
        }
    }

    private static Timer processing(SimpleMeterRegistry registry, String mode) {
        return registry.get("registry.registration.processing").tag("mode", mode).timer();
    }

    private static RegisterUserCommand command(Instant submittedAt) {
        return new RegisterUserCommand(UUID.randomUUID(), "fingerprint",
                new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"), submittedAt);
    }
}
