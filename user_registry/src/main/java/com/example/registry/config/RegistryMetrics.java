package com.example.registry.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.thread.Threading;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Counters, completion timers and configured limits shared by both HTTP routes and the Rabbit worker. */
@Component
public class RegistryMetrics {

    private final MeterRegistry registry;

    public RegistryMetrics(MeterRegistry registry, RabbitProperties rabbitProperties, Environment environment) {
        this.registry = registry;

        // Configuration gauges for the dashboard's context row: what this instance is bounded by.
        RabbitProperties.SimpleContainer listener = rabbitProperties.getListener().getSimple();
        Gauge.builder("registry.config.consumers", () -> orDefault(listener.getConcurrency(), 1))
                .description("Configured Rabbit listener concurrency (messages processed at once)")
                .register(registry);
        Gauge.builder("registry.config.prefetch", () -> orDefault(listener.getPrefetch(), 250))
                .description("Configured prefetch per consumer (unacknowledged deliveries allowed)")
                .register(registry);
        Gauge.builder("registry.threads.virtual.enabled", () -> Threading.VIRTUAL.isActive(environment) ? 1 : 0)
                .description("1 when spring.threads.virtual.enabled is active, 0 on platform threads")
                .register(registry);
    }

    public void intake(String outcome) {
        registry.counter("registry.intake.requests", "outcome", outcome).increment();
    }

    public void synchronous(String outcome) {
        registry.counter("registry.sync.requests", "outcome", outcome).increment();
    }

    public void registration(String outcome) {
        registry.counter("registry.registrations", "outcome", outcome).increment();
    }

    public void registrationFailed() {
        registry.counter("registry.registrations.failures").increment();
    }

    public void deadLettered() {
        registry.counter("registry.registrations.dead_lettered").increment();
    }

    /** Submission to user + outbox commit, including queue waiting. */
    public void completion(Duration duration) {
        registry.timer("registry.registration.completion").record(duration);
    }

    private static int orDefault(Integer value, int fallback) {
        return value == null ? fallback : value;
    }
}
