package com.example.compliance.listeners;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.example.compliance.AbstractIntegrationTest;
import com.example.compliance.config.ComplianceProperties;
import com.example.compliance.messaging.UserRegisteredEvent;
import com.example.compliance.repositories.ComplianceResultRepository;
import com.example.compliance.repositories.ProcessedEventRepository;
import com.example.compliance.services.ComplianceService;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.ObjectMapper;

/**
 * End to end over a real Kafka: a record on the event topic becomes one ledger row and one result,
 * a redelivered record is consumed again and changes nothing, and a record that cannot be parsed goes
 * to the dead-letter topic at once (no retries) without blocking the partition.
 */
class UserRegisteredEventListenerTest extends AbstractIntegrationTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Probes {

        @Bean
        DeadLetterProbe deadLetterProbe() {
            return new DeadLetterProbe();
        }
    }

    /** A test-only consumer of the dead-letter topic, so the test can see what was published there. */
    static class DeadLetterProbe {

        final CompletableFuture<ConsumerRecord<String, String>> received = new CompletableFuture<>();

        @KafkaListener(topics = "${compliance.kafka.topic}.DLT", groupId = "dlt-probe")
        void on(ConsumerRecord<String, String> record) {
            received.complete(record);
        }
    }

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ComplianceProperties props;
    @Autowired ObjectMapper objectMapper;
    @Autowired ProcessedEventRepository processedEvents;
    @Autowired ComplianceResultRepository results;
    @Autowired DeadLetterProbe deadLetterProbe;

    /** Spy on the real service: the tests count how often the listener reached it. */
    @MockitoSpyBean ComplianceService complianceService;

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void consumedEventBecomesOneLedgerRowAndOneResult() {
        UserRegisteredEvent event = event();

        send(event);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(processedEvents.count()).isEqualTo(1);
            assertThat(results.count()).isEqualTo(1);
        });
        assertThat(results.findByEventId(event.eventId())).isPresent();
    }

    @Test
    void sameEventTwiceIsConsumedTwiceAndCreatesOneResult() {
        // Review focus 1: at-least-once delivery, exactly-once effect. The spy proves the second record
        // really reached the service (it was not silently dropped) and still changed nothing.
        UserRegisteredEvent event = event();

        send(event);
        send(event);

        verify(complianceService, timeout(30_000).times(2)).process(any());
        assertThat(processedEvents.count()).isEqualTo(1);
        assertThat(results.count()).isEqualTo(1);
    }

    @Test
    void unparseableRecordGoesToDeadLetterTopicWithoutRetries() throws Exception {
        // Review focus 2: a poison record must not be retried forever or block the partition.
        Instant sentAt = Instant.now();
        kafkaTemplate.send(props.kafka().topic(), "poison", "not json");

        ConsumerRecord<String, String> dead = deadLetterProbe.received.get(30, TimeUnit.SECONDS);

        assertThat(dead.value()).isEqualTo("not json");
        assertThat(header(dead, "kafka_dlt-original-topic")).isEqualTo(props.kafka().topic());
        assertThat(header(dead, "kafka_dlt-exception-message")).isNotBlank();
        // With retries it could not arrive before the 3.5 s backoff schedule (500 ms + 1 s + 2 s) ran out.
        assertThat(Duration.between(sentAt, Instant.now())).isLessThan(Duration.ofSeconds(3));
        verify(complianceService, never()).process(any());
        assertThat(processedEvents.count()).isZero();
        assertThat(results.count()).isZero();
    }

    /** Serializes with the application's own ObjectMapper so the JSON shape cannot drift from the contract. */
    private void send(UserRegisteredEvent event) {
        kafkaTemplate.send(props.kafka().topic(), event.userId().toString(), objectMapper.writeValueAsString(event));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static UserRegisteredEvent event() {
        return new UserRegisteredEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "ada@example.com", "Ada Lovelace", "NL", Instant.now(), Instant.now());
    }
}
