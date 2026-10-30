package com.example.compliance.listeners;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.compliance.config.ComplianceMetrics;
import com.example.compliance.messaging.UserRegisteredEvent;
import com.example.compliance.services.ComplianceOutcome;
import com.example.compliance.services.ComplianceService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class ComplianceWorkerMetricsTest {
    @Mock ComplianceService service;
    @Mock ObjectMapper objectMapper;
    private SimpleMeterRegistry registry;
    private UserRegisteredEventListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        listener = new UserRegisteredEventListener(service, objectMapper,
                new ComplianceMetrics(registry, new KafkaProperties()));
    }

    @AfterEach
    void closeMeters() {
        registry.close();
    }

    @Test
    void waitingForTheTransactionalServiceCountsAsActiveUntilItReturns() throws Exception {
        UserRegisteredEvent event = event();
        when(objectMapper.readValue("event", UserRegisteredEvent.class)).thenReturn(event);
        CountDownLatch processing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(service.process(event)).thenAnswer(invocation -> {
            processing.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return ComplianceOutcome.PROCESSED;
        });

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var work = executor.submit(() -> listener.onUserRegistered(record("event")));
            try {
                assertThat(processing.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(active()).isEqualTo(1);
                assertThat(registry.get("compliance.completion").timer().count()).isZero();
            } finally {
                release.countDown();
            }
            work.get(5, TimeUnit.SECONDS);
        }
        assertThat(active()).isZero();
        assertThat(registry.get("compliance.completion").timer().count()).isEqualTo(1);
    }

    @Test
    void serviceFailureReleasesTheGaugeWithoutRecordingACompletion() {
        UserRegisteredEvent event = event();
        when(objectMapper.readValue("event", UserRegisteredEvent.class)).thenReturn(event);
        when(service.process(event)).thenAnswer(invocation -> {
            assertThat(active()).isEqualTo(1);
            throw new IllegalStateException("database unavailable");
        });

        assertThatThrownBy(() -> listener.onUserRegistered(record("event")))
                .isInstanceOf(IllegalStateException.class).hasMessage("database unavailable");

        assertThat(active()).isZero();
        assertThat(registry.get("compliance.completion").timer().count()).isZero();
    }

    @Test
    void parsingFailureAlsoReleasesTheGauge() {
        when(objectMapper.readValue("bad", UserRegisteredEvent.class)).thenAnswer(invocation -> {
            assertThat(active()).isEqualTo(1);
            throw new IllegalArgumentException("bad event");
        });

        assertThatThrownBy(() -> listener.onUserRegistered(record("bad")))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(active()).isZero();
        verifyNoInteractions(service);
    }

    @Test
    void tombstoneLeavesTheGaugeIdleWithoutInventingAnEvent() {
        listener.onUserRegistered(record(null));

        assertThat(active()).isZero();
        assertThat(registry.get("compliance.events").tag("outcome", "processed").counter().count()).isZero();
        verifyNoInteractions(service, objectMapper);
    }

    private double active() {
        return registry.get("compliance.workers.active").gauge().value();
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("outbox.event.user", 0, 0L, "user", value);
    }

    private static UserRegisteredEvent event() {
        return new UserRegisteredEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "ada@example.com", "Ada Lovelace", "NL", Instant.now(), Instant.now());
    }
}
