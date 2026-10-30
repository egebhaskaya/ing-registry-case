package com.example.compliance.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

class ComplianceMetricsUnitTest {
    @Test
    void idleMetricsExistWithoutInventingCompletions() {
        var registry = new SimpleMeterRegistry();
        try {
            new ComplianceMetrics(registry, new KafkaProperties());

            assertThat(registry.get("compliance.workers.active").gauge().value()).isZero();
            assertThat(registry.get("compliance.events").tag("outcome", "processed").counter().count()).isZero();
            assertThat(registry.get("compliance.events").tag("outcome", "duplicate_ignored").counter().count()).isZero();
            assertThat(registry.get("compliance.events.dead_lettered").counter().count()).isZero();
            assertThat(registry.get("compliance.completion").timer().count()).isZero();
            assertThat(registry.get("compliance.completion").timer().totalTime(java.util.concurrent.TimeUnit.SECONDS))
                    .isZero();
        } finally {
            registry.close();
        }
    }
}
