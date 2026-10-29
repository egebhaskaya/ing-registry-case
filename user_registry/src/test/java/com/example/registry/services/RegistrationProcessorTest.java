package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.registry.config.ProcessingProperties;
import com.example.registry.config.RegistryMetrics;
import com.example.registry.exceptions.SynchronousRegistrationOverloadedException;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.models.User;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

/** Capacity is acquired before transactions, with bounded HTTP waits and non-rejecting Rabbit waits. */
class RegistrationProcessorTest {
    private RegistrationService transactions;
    private RegistryMetrics metrics;
    private SimpleMeterRegistry meterRegistry;
    private RegistrationProcessor processor;
    private final List<Thread> threads = new ArrayList<>();

    private final RegisterUserCommand command = new RegisterUserCommand(UUID.randomUUID(), "fingerprint",
            new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"), Instant.now());
    private final RegistrationResult created = new RegistrationResult(RegistrationOutcome.CREATED,
            new User(UUID.randomUUID(), command.registrationId(), command.payloadFingerprint(),
                    command.payload().email(), command.payload().fullName(), command.payload().countryCode(),
                    Instant.now()));

    @BeforeEach
    void setUp() {
        transactions = mock(RegistrationService.class);
        metrics = mock(RegistryMetrics.class);
        meterRegistry = new SimpleMeterRegistry();
    }

    @AfterEach
    void stopWorkers() throws InterruptedException {
        threads.forEach(Thread::interrupt);
        for (Thread thread : threads) {
            thread.join(2_000);
            assertThat(thread.isAlive()).as("worker terminated").isFalse();
        }
        meterRegistry.close();
    }

    @Test
    void httpAndRabbitShareTheSameExecutionLimit() throws Exception {
        configure(2, 2, Duration.ofSeconds(5));
        CountDownLatch firstTwoEntered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        when(transactions.registerWithResult(any())).thenAnswer(invocation -> {
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            firstTwoEntered.countDown();
            try {
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                return created;
            } finally {
                active.decrementAndGet();
            }
        });

        Worker http = start(() -> processor.processHttp(command));
        Worker rabbit = start(() -> processor.processConsumer(command));
        assertThat(firstTwoEntered.await(2, TimeUnit.SECONDS)).isTrue();
        Worker nextRabbit = start(() -> processor.processConsumer(command));
        awaitBlocked(nextRabbit);

        assertThat(processor.availablePermits()).isZero();
        verify(transactions, times(2)).registerWithResult(any());
        release.countDown();
        assertThat(http.result()).isEqualTo(created);
        assertThat(rabbit.result()).isEqualTo(created);
        assertThat(nextRabbit.result()).isEqualTo(created);
        assertThat(maximum.get()).isEqualTo(2);
        assertThat(processor.availablePermits()).isEqualTo(2);
    }

    @Test
    void rejectsAdditionalHttpWaitersWithoutWaitingForTheirAdmissionTimeout() throws Exception {
        configure(1, 1, Duration.ofSeconds(5));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = blockFirstTransaction(entered);
        Worker rabbit = start(() -> processor.processConsumer(command));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        Worker waitingHttp = start(() -> processor.processHttp(command));
        await().atMost(Duration.ofSeconds(2)).until(() -> processor.waitingRequests() == 1);

        assertTimeout(Duration.ofSeconds(1), () -> assertThatThrownBy(() -> processor.processHttp(command))
                .isInstanceOf(SynchronousRegistrationOverloadedException.class));
        assertThat(processor.waitingRequests()).isEqualTo(1);
        verify(transactions, times(1)).registerWithResult(any());

        release.countDown();
        assertThat(rabbit.result()).isEqualTo(created);
        assertThat(waitingHttp.result()).isEqualTo(created);
        assertThat(processor.waitingRequests()).isZero();
        assertThat(processor.availablePermits()).isEqualTo(1);
    }

    @Test
    void admissionTimeoutReleasesTheHttpWaiterWithoutReleasingAnotherExecution() throws Exception {
        configure(1, 1, Duration.ofMillis(25));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = blockFirstTransaction(entered);
        Worker rabbit = start(() -> processor.processConsumer(command));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();

        assertThatThrownBy(() -> processor.processHttp(command))
                .isInstanceOf(SynchronousRegistrationOverloadedException.class);
        assertThat(processor.waitingRequests()).isZero();
        assertThat(processor.availablePermits()).isZero();
        verify(transactions, times(1)).registerWithResult(any());

        release.countDown();
        assertThat(rabbit.result()).isEqualTo(created);
        assertThat(processor.processHttp(command)).isEqualTo(created);
        assertThat(processor.availablePermits()).isEqualTo(1);
    }

    @Test
    void rabbitWaitsForCapacityEvenWhenNoHttpWaitersAreAllowed() throws Exception {
        configure(1, 0, Duration.ZERO);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = blockFirstTransaction(entered);
        Worker http = start(() -> processor.processHttp(command));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        Worker rabbit = start(() -> processor.processConsumer(command));
        awaitBlocked(rabbit);

        assertThat(processor.waitingRequests()).isZero();
        verify(transactions, times(1)).registerWithResult(any());
        release.countDown();
        assertThat(http.result()).isEqualTo(created);
        assertThat(rabbit.result()).isEqualTo(created);
        assertThat(processor.availablePermits()).isEqualTo(1);
    }

    @Test
    void interruptedHttpWaitRestoresInterruptAndReleasesOnlyItsWaiterSlot() throws Exception {
        configure(1, 1, Duration.ofSeconds(5));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = blockFirstTransaction(entered);
        Worker rabbit = start(() -> processor.processConsumer(command));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        AtomicBoolean interrupted = new AtomicBoolean();
        Worker http = start(() -> {
            try {
                return processor.processHttp(command);
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        await().atMost(Duration.ofSeconds(2)).until(() -> processor.waitingRequests() == 1);
        http.thread().interrupt();

        assertThatThrownBy(http::result).isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(SynchronousRegistrationOverloadedException.class);
        assertThat(interrupted.get()).isTrue();
        assertThat(processor.waitingRequests()).isZero();
        assertThat(processor.availablePermits()).isZero();
        release.countDown();
        assertThat(rabbit.result()).isEqualTo(created);
        assertThat(processor.availablePermits()).isEqualTo(1);
    }

    @Test
    void interruptedRabbitWaitDoesNotReleaseAnUnacquiredPermit() throws Exception {
        configure(1, 0, Duration.ZERO);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = blockFirstTransaction(entered);
        Worker http = start(() -> processor.processHttp(command));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        AtomicBoolean interrupted = new AtomicBoolean();
        Worker rabbit = start(() -> {
            try {
                return processor.processConsumer(command);
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        awaitBlocked(rabbit);
        rabbit.thread().interrupt();

        assertThatThrownBy(rabbit::result).isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(interrupted.get()).isTrue();
        assertThat(processor.availablePermits()).isZero();
        release.countDown();
        assertThat(http.result()).isEqualTo(created);
        assertThat(processor.availablePermits()).isEqualTo(1);
    }

    @Test
    void transactionFailureReleasesCapacityForTheOtherEntryPoint() {
        configure(1, 0, Duration.ZERO);
        when(transactions.registerWithResult(any())).thenThrow(new IllegalStateException("database failed"))
                .thenReturn(created);

        assertThatThrownBy(() -> processor.processHttp(command)).isInstanceOf(IllegalStateException.class)
                .hasMessage("database failed");
        assertThat(processor.availablePermits()).isEqualTo(1);
        assertThat(processor.processConsumer(command)).isEqualTo(created);
        assertThat(processor.availablePermits()).isEqualTo(1);
        verify(metrics).registrationFailed();
    }

    @Test
    void duplicateResolutionKeepsTheExecutionPermitUntilItReturns() {
        configure(1, 0, Duration.ZERO);
        RegistrationResult duplicate = new RegistrationResult(RegistrationOutcome.DUPLICATE_IGNORED, created.user());
        when(transactions.registerWithResult(any())).thenThrow(new DuplicateKeyException("concurrent duplicate"));
        when(transactions.resolveDuplicateWithResult(any())).thenAnswer(invocation -> {
            assertThat(processor.availablePermits()).isZero();
            return duplicate;
        });

        assertThat(processor.processHttp(command)).isEqualTo(duplicate);
        assertThat(processor.availablePermits()).isEqualTo(1);
        verify(transactions).resolveDuplicateWithResult(command);
    }

    private void configure(int concurrency, int waiters, Duration admissionWait) {
        processor = new RegistrationProcessor(transactions, metrics,
                new ProcessingProperties(concurrency, waiters, admissionWait), meterRegistry);
    }

    private CountDownLatch blockFirstTransaction(CountDownLatch entered) {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        when(transactions.registerWithResult(any())).thenAnswer(invocation -> {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return created;
        });
        return release;
    }

    private Worker start(Callable<RegistrationResult> action) {
        FutureTask<RegistrationResult> task = new FutureTask<>(action);
        Thread thread = Thread.ofVirtual().start(task);
        threads.add(thread);
        return new Worker(thread, task);
    }

    private void awaitBlocked(Worker worker) {
        await().atMost(Duration.ofSeconds(2)).until(() -> worker.thread().getState() == Thread.State.WAITING);
        assertThat(worker.task().isDone()).isFalse();
    }

    private record Worker(Thread thread, FutureTask<RegistrationResult> task) {
        RegistrationResult result() throws Exception {
            return task.get(5, TimeUnit.SECONDS);
        }
    }
}
