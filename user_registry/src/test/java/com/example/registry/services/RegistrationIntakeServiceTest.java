package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.mock.env.MockEnvironment;

/**
 * Unit test of the admission + outcome-mapping logic. The publisher is a Mockito mock so each broker
 * verdict can be simulated without a broker; the real normalizer and an in-memory meter registry are
 * used because they are cheap.
 */
@ExtendWith(MockitoExtension.class)
class RegistrationIntakeServiceTest {

    @Mock
    RegistrationCommandPublisher publisher;

    private MeterRegistry meterRegistry;
    private RegistrationIntakeService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = newService(new IntakeProperties(2, Duration.ofMillis(100)));
    }

    @Test
    void confirmedPublishReturnsReceiptWithSameRegistrationId() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.CONFIRMED);
        UUID id = UUID.randomUUID();

        RegistrationAcceptedResponse response =
                service.accept(id, new RegistrationRequest(" Ada  Lovelace ", "ADA@Example.com", "nl"));

        assertThat(response.registrationId()).isEqualTo(id);
        assertThat(response.status()).isEqualTo("ACCEPTED");

        // The command handed to the broker must carry the normalized payload and its fingerprint.
        ArgumentCaptor<RegisterUserCommand> captor = ArgumentCaptor.forClass(RegisterUserCommand.class);
        verify(publisher).publish(captor.capture());
        RegisterUserCommand command = captor.getValue();
        assertThat(command.registrationId()).isEqualTo(id);
        assertThat(command.payload()).isEqualTo(new RegistrationPayload("Ada Lovelace", "ada@example.com", "NL"));
        assertThat(command.payloadFingerprint()).matches("^[0-9a-f]{64}$");
        assertThat(command.submittedAt()).isNotNull();
        assertThat(intakeCount("accepted")).isEqualTo(1.0);
    }

    @Test
    void brokerRejectionBecomes503Rejected() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.REJECTED);

        assertThatThrownBy(() -> service.accept(UUID.randomUUID(), validRequest()))
                .isInstanceOfSatisfying(IntakeRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo("broker_rejected"));
        assertThat(intakeCount("broker_rejected")).isEqualTo(1.0);
    }

    @Test
    void unroutableBecomes503Rejected() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.UNROUTABLE);

        assertThatThrownBy(() -> service.accept(UUID.randomUUID(), validRequest()))
                .isInstanceOfSatisfying(IntakeRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo("unroutable"));
    }

    @Test
    void unknownOutcomeBecomesOutcomeUnknown() {
        when(publisher.publish(any())).thenReturn(PublishOutcome.UNKNOWN);

        assertThatThrownBy(() -> service.accept(UUID.randomUUID(), validRequest()))
                .isInstanceOf(IntakeOutcomeUnknownException.class);
        assertThat(intakeCount("confirm_timeout")).isEqualTo(1.0);
    }

    @Test
    void brokerConnectionFailureReleasesThePermit() {
        when(publisher.publish(any())).thenThrow(new AmqpConnectException(new RuntimeException("broker down")));

        assertThatThrownBy(() -> service.accept(UUID.randomUUID(), validRequest()))
                .isInstanceOfSatisfying(IntakeRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo("broker_unavailable"));
        // Review focus 4: a failure path must give its admission slot back.
        assertThat(service.availablePermits()).isEqualTo(2);
        assertThat(intakeCount("broker_unavailable")).isEqualTo(1.0);
    }

    @Test
    void overloadedWhenAllPermitsAreHeld() throws Exception {
        service = newService(new IntakeProperties(1, Duration.ofMillis(100)));
        CountDownLatch publishStarted = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        // The first publish blocks until we release it, holding the single permit.
        when(publisher.publish(any())).thenAnswer(invocation -> {
            publishStarted.countDown();
            releasePublish.await();
            return PublishOutcome.CONFIRMED;
        });

        Thread first = Thread.ofVirtual().start(() -> service.accept(UUID.randomUUID(), validRequest()));
        assertThat(publishStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // Second caller finds no permit, waits admissionWait (100ms), then is refused.
        assertThatThrownBy(() -> service.accept(UUID.randomUUID(), validRequest()))
                .isInstanceOf(IntakeOverloadedException.class);
        assertThat(intakeCount("overloaded")).isEqualTo(1.0);

        releasePublish.countDown();
        first.join();
        assertThat(service.availablePermits()).isEqualTo(1);
    }

    private RegistrationIntakeService newService(IntakeProperties props) {
        RegistryMetrics metrics = new RegistryMetrics(meterRegistry, new RabbitProperties(), new MockEnvironment());
        return new RegistrationIntakeService(new RegistrationNormalizer(), publisher, props, metrics);
    }

    private double intakeCount(String outcome) {
        return meterRegistry.counter("registry.intake.requests", "outcome", outcome).count();
    }

    private static RegistrationRequest validRequest() {
        return new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL");
    }
}
