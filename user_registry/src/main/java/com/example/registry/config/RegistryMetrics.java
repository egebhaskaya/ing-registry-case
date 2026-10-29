package com.example.registry.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.thread.Threading;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * The Registry's own metrics (design sections 7 and 8), in one place so the names and labels the
 * Grafana dashboard depends on are defined once.
 *
 * <p>Micrometer vocabulary: a <em>counter</em> only goes up (requests seen), a <em>timer</em> records
 * durations as a histogram (so p95 can be computed from buckets, never from an average), and a
 * <em>gauge</em> reports a current value (a configured limit). Labels ("tags") are bounded sets such
 * as {@code outcome}; never a registration id or an email, which would create one series per request.
 * Names are dotted here and rendered with underscores by Prometheus, e.g.
 * {@code registry.intake.requests} becomes {@code registry_intake_requests_total}.
 */
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

    /** One HTTP intake decision: accepted, overloaded, broker_rejected, unroutable, broker_unavailable, confirm_timeout. */
    public void intake(String outcome) {
        registry.counter("registry.intake.requests", "outcome", outcome).increment();
    }

    /** One synchronous HTTP decision: created, replayed, conflict, overloaded, failed. */
    public void synchronous(String outcome) {
        registry.counter("registry.sync.requests", "outcome", outcome).increment();
    }

    /** One committed processing result across HTTP and Rabbit: created, duplicate_ignored, conflict. */
    public void registration(String outcome) {
        registry.counter("registry.registrations", "outcome", outcome).increment();
    }

    /** One processing attempt that threw (retried consumer attempts count every time). */
    public void registrationFailed() {
        registry.counter("registry.registrations.failures").increment();
    }

    /** One command republished to the dead-letter queue. */
    public void deadLettered() {
        registry.counter("registry.registrations.dead_lettered").increment();
    }

    /** Submission to user + outbox commit across both routes, including any queue/admission waiting. */
    public void completion(Duration duration) {
        registry.timer("registry.registration.completion").record(duration);
    }

    private static int orDefault(Integer value, int fallback) {
        return value == null ? fallback : value;
    }
}
