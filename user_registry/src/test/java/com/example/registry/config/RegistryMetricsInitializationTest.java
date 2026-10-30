package com.example.registry.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.mock.env.MockEnvironment;

class RegistryMetricsInitializationTest {
    @Test
    void idleApplicationHasZeroCountersAndNoInventedCompletionObservations() {
        var registry = new SimpleMeterRegistry();
        try {
            new RegistryMetrics(registry, new RabbitProperties(), new MockEnvironment());

            assertThat(registry.find("registry.workers.active").gauge().value()).isZero();
            for (String outcome : List.of("accepted", "overloaded", "broker_rejected", "unroutable",
                    "broker_unavailable", "confirm_timeout")) {
                assertZero(registry, "registry.intake.requests", outcome);
            }
            for (String outcome : List.of("created", "replayed", "conflict", "overloaded", "failed")) {
                assertZero(registry, "registry.sync.requests", outcome);
            }
            for (String outcome : List.of("created", "duplicate_ignored", "conflict")) {
                assertZero(registry, "registry.registrations", outcome);
            }
            assertThat(registry.find("registry.registrations.failures").counter().count()).isZero();
            assertThat(registry.find("registry.registrations.dead_lettered").counter().count()).isZero();
            var completion = registry.find("registry.registration.completion").timer();
            assertThat(completion).isNotNull();
            assertThat(completion.count()).isZero();
            assertThat(completion.totalTime(TimeUnit.SECONDS)).isZero();
            var httpSuccessTimers = registry.find("http.server.requests").timers();
            assertThat(httpSuccessTimers).hasSize(3).allSatisfy(timer -> {
                assertThat(timer.count()).isZero();
                assertThat(timer.totalTime(TimeUnit.SECONDS)).isZero();
            });
        } finally {
            registry.close();
        }
    }

    private static void assertZero(SimpleMeterRegistry registry, String name, String outcome) {
        Counter counter = registry.find(name).tag("outcome", outcome).counter();
        assertThat(counter).as("%s{outcome=%s}", name, outcome).isNotNull();
        assertThat(counter.count()).isZero();
    }
}
