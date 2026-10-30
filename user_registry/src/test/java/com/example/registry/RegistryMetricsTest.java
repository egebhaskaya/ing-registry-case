package com.example.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.repositories.UserRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.thread.Threading;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * Design section 8: the dashboard reads these exact series names and labels, so the test pins them.
 * Labels are bounded (application, outcome); never ids, emails or thread names.
 *
 * <p>{@code @AutoConfigureMetrics}: Spring Boot switches metrics exporters off in tests by default
 * (no Prometheus endpoint); this test needs the real endpoint.
 */
@AutoConfigureMetrics
@AutoConfigureRestTestClient
class RegistryMetricsTest extends AbstractIntegrationTest {

    @Autowired RestTestClient client;
    @Autowired UserRepository users;
    @Autowired RabbitProperties rabbitProperties;
    @Autowired Environment environment;
    @Autowired MeterRegistry meters;

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void exposesRegistrationMetricsOnThePrometheusEndpoint() {
        Timer async = singleSuccessTimer("/registrations", "202");
        Timer created = singleSuccessTimer("/registrations/sync", "201");
        Timer replayed = singleSuccessTimer("/registrations/sync", "200");
        assertThat(async.count()).isZero();
        assertThat(created.count()).isZero();
        assertThat(replayed.count()).isZero();
        Timer asyncProcessing = meters.get("registry.registration.processing").tag("mode", "async").timer();
        Timer syncProcessing = meters.get("registry.registration.processing").tag("mode", "sync").timer();
        assertThat(asyncProcessing.count()).isZero();
        assertThat(syncProcessing.count()).isZero();
        UUID key = UUID.randomUUID();
        client.post().uri("/registrations")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Ada Lovelace", "metrics-" + key + "@example.com", "NL"))
                .exchange()
                .expectStatus().isAccepted();
        await().atMost(Duration.ofSeconds(10)).until(() -> users.count() == 1);

        UUID syncKey = UUID.randomUUID();
        RegistrationRequest syncRequest = new RegistrationRequest("Grace Hopper", "metrics-" + syncKey + "@example.com", "NL");
        client.post().uri("/registrations/sync")
                .header("Idempotency-Key", syncKey.toString())
                .contentType(MediaType.APPLICATION_JSON).body(syncRequest)
                .exchange().expectStatus().isCreated();
        client.post().uri("/registrations/sync")
                .header("Idempotency-Key", syncKey.toString())
                .contentType(MediaType.APPLICATION_JSON).body(syncRequest)
                .exchange().expectStatus().isOk();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(singleSuccessTimer("/registrations", "202")).isSameAs(async);
            assertThat(singleSuccessTimer("/registrations/sync", "201")).isSameAs(created);
            assertThat(singleSuccessTimer("/registrations/sync", "200")).isSameAs(replayed);
            assertThat(async.count()).isEqualTo(1);
            assertThat(created.count()).isEqualTo(1);
            assertThat(replayed.count()).isEqualTo(1);
            assertThat(asyncProcessing.count()).isEqualTo(1);
            assertThat(syncProcessing.count()).isEqualTo(1);
        });

        String metrics = client.get().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(metrics)
                // our counters and timers (Micrometer renders dotted names with underscores and a _total suffix)
                .contains("registry_intake_requests_total{application=\"registry-service\",outcome=\"accepted\"}")
                .contains("registry_registrations_total{application=\"registry-service\",outcome=\"created\"}")
                .contains("registry_registration_completion_seconds_bucket")
                .contains("registry_registration_processing_seconds_bucket")
                // configuration and runtime gauges for the dashboard's context row
                .contains("registry_config_consumers{application=\"registry-service\"} "
                        + rabbitProperties.getListener().getSimple().getConcurrency().doubleValue())
                .contains("registry_config_prefetch{application=\"registry-service\"} "
                        + rabbitProperties.getListener().getSimple().getPrefetch().doubleValue())
                .contains("registry_threads_virtual_enabled{application=\"registry-service\"} "
                        + (Threading.VIRTUAL.isActive(environment) ? "1.0" : "0.0"))
                .contains("registry_workers_active{application=\"registry-service\"}")
                // built-in series the dashboard also relies on
                .contains("http_server_requests_seconds_bucket{")
                .contains("hikaricp_connections_max{")
                .contains("jvm_info{");
        assertThat(hasProcessingBuckets(metrics, "sync")).isTrue();
        assertThat(hasProcessingBuckets(metrics, "async")).isTrue();
    }

    private static boolean hasProcessingBuckets(String metrics, String mode) {
        return metrics.lines().anyMatch(line -> line.startsWith("registry_registration_processing_seconds_bucket{")
                && line.contains("mode=\"" + mode + "\""));
    }

    private Timer singleSuccessTimer(String uri, String status) {
        var matching = meters.find("http.server.requests").tags("method", "POST", "uri", uri, "status", status).timers();
        assertThat(matching).as("one actual HTTP timer for %s %s", uri, status).hasSize(1);
        return matching.iterator().next();
    }
}
