package com.example.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.repositories.UserRepository;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
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

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void exposesRegistrationMetricsOnThePrometheusEndpoint() {
        UUID key = UUID.randomUUID();
        client.post().uri("/registrations")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Ada Lovelace", "metrics-" + key + "@example.com", "NL"))
                .exchange()
                .expectStatus().isAccepted();
        await().atMost(Duration.ofSeconds(10)).until(() -> users.count() == 1);

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
                // configuration and runtime gauges for the dashboard's context row
                .contains("registry_config_consumers{application=\"registry-service\"} 4.0")
                .contains("registry_config_prefetch{application=\"registry-service\"} 8.0")
                .contains("registry_threads_virtual_enabled{application=\"registry-service\"} 1.0")
                // built-in series the dashboard also relies on
                .contains("http_server_requests_seconds_bucket{")
                .contains("hikaricp_connections_max{")
                .contains("jvm_info{");
    }
}
