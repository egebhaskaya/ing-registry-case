package com.example.compliance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.compliance.config.ComplianceProperties;
import com.example.compliance.messaging.UserRegisteredEvent;
import com.example.compliance.repositories.ComplianceResultRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.ObjectMapper;

/**
 * Design section 8: pins the series names and labels the dashboard reads from Compliance.
 * {@code @AutoConfigureMetrics} re-enables the exporters Boot switches off in tests.
 */
@AutoConfigureMetrics
@AutoConfigureRestTestClient
class ComplianceMetricsTest extends AbstractIntegrationTest {

    @Autowired RestTestClient client;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ComplianceProperties props;
    @Autowired ObjectMapper objectMapper;
    @Autowired ComplianceResultRepository results;

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void exposesComplianceMetricsOnThePrometheusEndpoint() {
        UserRegisteredEvent event = new UserRegisteredEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "ada@example.com", "Ada Lovelace", "NL", Instant.now(), Instant.now());
        kafkaTemplate.send(props.kafka().topic(), event.userId().toString(), objectMapper.writeValueAsString(event));
        await().atMost(Duration.ofSeconds(30)).until(() -> results.count() == 1);

        String metrics = client.get().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(metrics)
                .contains("compliance_events_total{application=\"compliance-service\",outcome=\"processed\"}")
                .contains("compliance_completion_seconds_bucket")
                .contains("compliance_config_concurrency{application=\"compliance-service\"} 1.0")
                .contains("jvm_info{");
    }
}
