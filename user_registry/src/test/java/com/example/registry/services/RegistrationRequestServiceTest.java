package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.registry.config.IntakeProperties;
import com.example.registry.config.RegistryMetrics;
import com.example.registry.config.RegistryBatchProperties;
import com.example.registry.dtos.RegistrationAcceptedResponse;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.exceptions.ConflictingPayloadException;
import com.example.registry.exceptions.EmailAlreadyRegisteredException;
import com.example.registry.exceptions.IntakeOutcomeUnknownException;
import com.example.registry.exceptions.IntakeOverloadedException;
import com.example.registry.exceptions.IntakeRejectedException;
import com.example.registry.messaging.PublishOutcome;
import com.example.registry.messaging.RegisterUserCommand;
import com.example.registry.messaging.RegistrationCommandPublisher;
import com.example.registry.messaging.RegistrationPayload;
import com.example.registry.models.User;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.mock.env.MockEnvironment;

/** Async uses broker confirmation; sync calls the transactional processor directly. Both bound HTTP admission. */
@ExtendWith(MockitoExtension.class)
class RegistrationRequestServiceTest {
    @Mock RegistrationCommandPublisher publisher;
    @Mock RegistrationProcessor processor;

    private MeterRegistry meterRegistry;
    private RegistrationRequestService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = newService(new IntakeProperties(2, Duration.ofMillis(100)));
    }

    @AfterEach
    void closeMeters() {
        meterRegistry.close();
    }

    @Test
    void confirmedPublishReturnsReceiptWithSameRegistrationId() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.CONFIRMED);
        UUID id = UUID.randomUUID();

        RegistrationAcceptedResponse response =
                service.enqueue(id, new RegistrationRequest(" Ada  Lovelace ", "ADA@Example.com", "nl"));

        assertThat(response.registrationId()).isEqualTo(id);
        assertThat(response.status()).isEqualTo("ACCEPTED");
        ArgumentCaptor<RegisterUserCommand> captor = ArgumentCaptor.forClass(RegisterUserCommand.class);
        verify(publisher).publish(captor.capture());
        assertNormalized(captor.getValue(), id);
        verifyNoInteractions(processor);
        assertThat(intakeCount("accepted")).isEqualTo(1);
    }

    @Test
    void brokerRejectionBecomes503Rejected() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.REJECTED);

        assertThatThrownBy(() -> service.enqueue(UUID.randomUUID(), validRequest()))
                .isInstanceOfSatisfying(IntakeRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo("broker_rejected"));
        assertThat(intakeCount("broker_rejected")).isEqualTo(1);
    }

    @Test
    void unroutableBecomes503Rejected() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.UNROUTABLE);

        assertThatThrownBy(() -> service.enqueue(UUID.randomUUID(), validRequest()))
                .isInstanceOfSatisfying(IntakeRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo("unroutable"));
    }

    @Test
    void unknownOutcomeBecomesOutcomeUnknown() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.UNKNOWN);

        assertThatThrownBy(() -> service.enqueue(UUID.randomUUID(), validRequest()))
                .isInstanceOf(IntakeOutcomeUnknownException.class);
        assertThat(intakeCount("confirm_timeout")).isEqualTo(1);
    }

    @Test
    void brokerConnectionFailureReleasesThePermit() {
        when(publisher.publish(any())).thenThrow(new AmqpConnectException(new RuntimeException("broker down")));

        assertThatThrownBy(() -> service.enqueue(UUID.randomUUID(), validRequest()))
                .isInstanceOfSatisfying(IntakeRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo("broker_unavailable"));
        assertThat(service.availablePermits()).isEqualTo(2);
        assertThat(intakeCount("broker_unavailable")).isEqualTo(1);
    }

    @Test
    void overloadedWhenAllPermitsAreHeld() throws Exception {
        service = newService(new IntakeProperties(1, Duration.ofMillis(100)));
        CountDownLatch publishStarted = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        when(publisher.publish(any())).thenAnswer(invocation -> {
            publishStarted.countDown();
            assertThat(releasePublish.await(5, TimeUnit.SECONDS)).isTrue();
            return PublishOutcome.CONFIRMED;
        });
        FutureTask<RegistrationAcceptedResponse> request =
                new FutureTask<>(() -> service.enqueue(UUID.randomUUID(), validRequest()));
        Thread caller = Thread.ofVirtual().start(request);
        try {
            assertThat(publishStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> service.enqueue(UUID.randomUUID(), validRequest()))
                    .isInstanceOf(IntakeOverloadedException.class);
            assertThat(intakeCount("overloaded")).isEqualTo(1);
        } finally {
            releasePublish.countDown();
            caller.join(2_000);
        }
        assertThat(request.get(2, TimeUnit.SECONDS).status()).isEqualTo("ACCEPTED");
        assertThat(service.availablePermits()).isEqualTo(1);
    }

    @Test
    void synchronousRegistrationProcessesNormalizedCommandWithoutPublishing() {
        UUID id = UUID.randomUUID();
        RegistrationResult result = completed(id, RegistrationOutcome.CREATED);
        when(processor.process(any(), eq(RegistrationMode.SYNC))).thenReturn(result);

        assertThat(service.registerDirectly(id, new RegistrationRequest(" Ada  Lovelace ", "ADA@Example.com", "nl")))
                .isEqualTo(result);

        ArgumentCaptor<RegisterUserCommand> captor = ArgumentCaptor.forClass(RegisterUserCommand.class);
        verify(processor).process(captor.capture(), eq(RegistrationMode.SYNC));
        assertNormalized(captor.getValue(), id);
        verifyNoInteractions(publisher);
        assertThat(syncCount("created")).isEqualTo(1);
        assertThat(service.availablePermits()).isEqualTo(2);
    }

    @Test
    void synchronousReplayReturnsTheStoredUser() {
        UUID id = UUID.randomUUID();
        RegistrationResult replay = completed(id, RegistrationOutcome.DUPLICATE_IGNORED);
        when(processor.process(any(), eq(RegistrationMode.SYNC))).thenReturn(replay);

        assertThat(service.registerDirectly(id, validRequest())).isEqualTo(replay);

        assertThat(syncCount("replayed")).isEqualTo(1);
        verifyNoInteractions(publisher);
    }

    @Test
    void synchronousBusinessConflictsRemainTheirHttpExceptions() {
        when(processor.process(any(), eq(RegistrationMode.SYNC)))
                .thenThrow(new ConflictingPayloadException(UUID.randomUUID()))
                .thenThrow(new EmailAlreadyRegisteredException(UUID.randomUUID()));

        assertThatThrownBy(() -> service.registerDirectly(UUID.randomUUID(), validRequest()))
                .isInstanceOf(ConflictingPayloadException.class);
        assertThatThrownBy(() -> service.registerDirectly(UUID.randomUUID(), validRequest()))
                .isInstanceOf(EmailAlreadyRegisteredException.class);

        assertThat(syncCount("conflict")).isEqualTo(2);
        assertThat(service.availablePermits()).isEqualTo(2);
        verifyNoInteractions(publisher);
    }

    @Test
    void synchronousProcessingHoldsTheSharedIntakePermitUntilItReturns() throws Exception {
        service = newService(new IntakeProperties(1, Duration.ofMillis(25)));
        UUID id = UUID.randomUUID();
        RegistrationResult result = completed(id, RegistrationOutcome.CREATED);
        CountDownLatch processing = new CountDownLatch(1);
        CountDownLatch releaseProcessing = new CountDownLatch(1);
        when(processor.process(any(), eq(RegistrationMode.SYNC))).thenAnswer(invocation -> {
            processing.countDown();
            assertThat(releaseProcessing.await(5, TimeUnit.SECONDS)).isTrue();
            return result;
        });
        FutureTask<RegistrationResult> request = new FutureTask<>(() -> service.registerDirectly(id, validRequest()));
        Thread caller = Thread.ofVirtual().start(request);
        try {
            assertThat(processing.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(service.availablePermits()).isZero();
            assertThat(request.isDone()).isFalse();
            assertThatThrownBy(() -> service.enqueue(UUID.randomUUID(), validRequest()))
                    .isInstanceOf(IntakeOverloadedException.class);
            assertThatThrownBy(() -> service.registerDirectly(UUID.randomUUID(), validRequest()))
                    .isInstanceOf(IntakeOverloadedException.class);
        } finally {
            releaseProcessing.countDown();
            caller.join(2_000);
        }

        assertThat(request.get(2, TimeUnit.SECONDS)).isEqualTo(result);
        assertThat(service.availablePermits()).isEqualTo(1);
        verifyNoInteractions(publisher);
    }

    @Test
    void synchronousProcessingFailurePropagatesAndReleasesThePermit() {
        when(processor.process(any(), eq(RegistrationMode.SYNC))).thenThrow(new IllegalStateException("database unavailable"));

        assertThatThrownBy(() -> service.registerDirectly(UUID.randomUUID(), validRequest()))
                .isInstanceOf(IllegalStateException.class).hasMessage("database unavailable");

        assertThat(service.availablePermits()).isEqualTo(2);
        assertThat(syncCount("failed")).isEqualTo(1);
        verifyNoInteractions(publisher);
    }

    private RegistrationRequestService newService(IntakeProperties props) {
        RegistryMetrics metrics = new RegistryMetrics(meterRegistry, new RabbitProperties(), new MockEnvironment());
        return new RegistrationRequestService(new RegistrationNormalizer(), publisher, processor, props, metrics,
                new RegistryBatchProperties(100, Duration.ofMillis(100), "registration.commands.batched",
                        "registration.register.batched"));
    }

    private static void assertNormalized(RegisterUserCommand command, UUID id) {
        assertThat(command.registrationId()).isEqualTo(id);
        assertThat(command.payload()).isEqualTo(new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"));
        assertThat(command.payloadFingerprint()).matches("^[0-9a-f]{64}$");
        assertThat(command.submittedAt()).isNotNull();
    }

    private double intakeCount(String outcome) {
        return meterRegistry.counter("registry.intake.requests", "outcome", outcome).count();
    }

    private double syncCount(String outcome) {
        return meterRegistry.counter("registry.sync.requests", "outcome", outcome).count();
    }

    private RegistrationResult completed(UUID id, RegistrationOutcome outcome) {
        return new RegistrationResult(outcome,
                new User(UUID.randomUUID(), id, "fingerprint", "ada@example.com", "Ada Lovelace", "NL", Instant.now()));
    }

    private static RegistrationRequest validRequest() {
        return new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL");
    }
}
