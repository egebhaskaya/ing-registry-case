package com.example.compliance.listeners;

import com.example.compliance.config.ComplianceMetrics;
import com.example.compliance.messaging.UserRegisteredEvent;
import com.example.compliance.services.ComplianceOutcome;
import com.example.compliance.services.ComplianceService;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * The Compliance consumer (design section 5, "CDC and Compliance").
 *
 * <p>{@code @KafkaListener} makes Spring poll the topic and call this method for every record. The
 * number of concurrent consumers is {@code spring.kafka.listener.concurrency} (1 by default); in a
 * consumer group each partition is read by at most one consumer, so more than 4 consumers would sit
 * idle on a 4-partition topic. Boot's Kafka container executor follows
 * {@code spring.threads.virtual.enabled}; {@code VirtualThreadVerificationTest} proves it.
 *
 * <p>The record's value is the JSON Debezium published (the outbox payload), parsed here into the
 * event record; the key is the user id. When this method returns, the container commits the offset
 * ({@code ack-mode: record}), so the offset only moves past a record whose transaction has committed.
 * When it throws, the configured error handler retries with backoff and finally publishes the record
 * to the dead-letter topic; unparseable JSON skips the retries because it can never succeed.
 *
 * <p>Measurement (design section 7): the completion timer runs from the API accepting the request
 * ({@code submittedAt} inside the event) to the result commit, so it spans queue, CDC and Kafka.
 * It is recorded only for a newly processed event, never for duplicates.
 */
@Component
public class UserRegisteredEventListener {

    private static final Logger log = LoggerFactory.getLogger(UserRegisteredEventListener.class);

    private final ComplianceService complianceService;
    private final ObjectMapper objectMapper;
    private final ComplianceMetrics metrics;

    public UserRegisteredEventListener(ComplianceService complianceService, ObjectMapper objectMapper,
                                       ComplianceMetrics metrics) {
        this.complianceService = complianceService;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${compliance.kafka.topic}")
    public void onUserRegistered(ConsumerRecord<String, String> record) {
        metrics.workerStarted();
        try {
            if (record.value() == null) {
                // Tombstones carry no event and can be acknowledged without business work.
                log.warn("skipping tombstone record partition={} offset={}", record.partition(), record.offset());
                return;
            }
            UserRegisteredEvent event = objectMapper.readValue(record.value(), UserRegisteredEvent.class);

            ComplianceOutcome outcome = complianceService.process(event);

            metrics.event(outcome.name().toLowerCase(Locale.ROOT));
            if (outcome == ComplianceOutcome.PROCESSED) {
                metrics.completion(Duration.between(event.submittedAt(), Instant.now()));
            }
            log.info("compliance processed eventId={} registrationId={} outcome={} partition={} offset={}",
                    event.eventId(), event.registrationId(), outcome, record.partition(), record.offset());
        } finally {
            metrics.workerFinished();
        }
    }
}
