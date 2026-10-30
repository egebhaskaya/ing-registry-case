package com.example.compliance.services;

import com.example.compliance.messaging.UserRegisteredEvent;
import com.example.compliance.models.ComplianceResult;
import com.example.compliance.models.ProcessedEvent;
import com.example.compliance.repositories.ComplianceResultRepository;
import com.example.compliance.repositories.ProcessedEventRepository;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The Compliance transaction (design section 5): deduplication state and the
 * result are committed
 * together, before the Kafka offset is allowed to advance (the listener
 * container commits the offset
 * only after this method has returned, see application.properties
 * {@code ack-mode: record}).
 *
 * <p>
 * Kafka is at-least-once: after a crash between this commit and the offset
 * commit, the record is
 * delivered again. The ledger row makes the second delivery a no-op, so the
 * business effect happens
 * exactly once even though delivery does not.
 */
@Service
public class ComplianceService {

    private static final Logger log = LoggerFactory.getLogger(ComplianceService.class);

    private final ProcessedEventRepository processedEvents;
    private final ComplianceResultRepository results;
    private final DemoComplianceRule rule;

    public ComplianceService(ProcessedEventRepository processedEvents,
            ComplianceResultRepository results,
            DemoComplianceRule rule) {
        this.processedEvents = processedEvents;
        this.results = results;
        this.rule = rule;
    }

    @Transactional
    public ComplianceOutcome process(UserRegisteredEvent event) {
        Instant now = Instant.now();
        boolean firstTime = processedEvents.insertIfAbsent(new ProcessedEvent(
                event.eventId(), event.userId(), event.registrationId(), event.submittedAt(), now));
        if (!firstTime) {
            log.info("compliance duplicate ignored eventId={} registrationId={}", event.eventId(),
                    event.registrationId());
            return ComplianceOutcome.DUPLICATE_IGNORED;
        }

        Verdict verdict = rule.decide(event);
        results.insert(new ComplianceResult(event.eventId(), event.userId(), event.registrationId(),
                verdict.decision(), verdict.reason(), now));

        log.info("compliance result committed eventId={} registrationId={} decision={}",
                event.eventId(), event.registrationId(), verdict.decision());
        return ComplianceOutcome.PROCESSED;
    }
}
