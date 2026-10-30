package com.example.compliance.config;

import com.example.compliance.services.ComplianceOutcome;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Component;

/**
 * Compliance's own metrics (design sections 7 and 8); the Kafka-side counterpart of RegistryMetrics.
 * Labels are bounded ({@code outcome}); names are dotted here and rendered with underscores by
 * Prometheus ({@code compliance.events} becomes {@code compliance_events_total}).
 */
@Component
public class ComplianceMetrics {

    private final MeterRegistry registry;
    private final AtomicInteger activeWorkers = new AtomicInteger();

    public ComplianceMetrics(MeterRegistry registry, KafkaProperties kafkaProperties) {
        this.registry = registry;
        for (ComplianceOutcome outcome : ComplianceOutcome.values()) {
            registry.counter("compliance.events", "outcome", outcome.name().toLowerCase(Locale.ROOT));
        }
        registry.counter("compliance.events.dead_lettered");
        registry.timer("compliance.completion");
        Gauge.builder("compliance.workers.active", activeWorkers, AtomicInteger::get)
                .description("In-progress listener invocations, including parsing and database waits; not CPU threads")
                .register(registry);
        Integer concurrency = kafkaProperties.getListener().getConcurrency();
        Gauge.builder("compliance.config.concurrency", () -> concurrency == null ? 1 : concurrency)
                .description("Configured Kafka listener concurrency (at most one useful consumer per partition)")
                .register(registry);
    }

    /** One consumed event: processed or duplicate_ignored. */
    public void event(String outcome) {
        registry.counter("compliance.events", "outcome", outcome).increment();
    }

    /** One record published to the dead-letter topic after retries were exhausted or skipped. */
    public void deadLettered() {
        registry.counter("compliance.events.dead_lettered").increment();
    }

    /** From the API accepting the request (submittedAt) to the compliance result commit: the whole pipeline. */
    public void completion(Duration duration) {
        registry.timer("compliance.completion").record(duration);
    }

    public void workerStarted() {
        activeWorkers.incrementAndGet();
    }

    public void workerFinished() {
        activeWorkers.decrementAndGet();
    }
}
