package com.example.compliance.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import com.example.compliance.AbstractIntegrationTest;
import com.example.compliance.messaging.UserRegisteredEvent;
import com.example.compliance.models.ComplianceResult;
import com.example.compliance.repositories.ComplianceResultRepository;
import com.example.compliance.repositories.ProcessedEventRepository;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * The Compliance transaction against a real PostgreSQL (design section 5: result + deduplication state
 * committed together; redelivery must not create another business effect).
 */
class ComplianceServiceTest extends AbstractIntegrationTest {

    @Autowired ComplianceService service;
    @Autowired ProcessedEventRepository processedEvents;

    /** A spy on the real repository so one test can make the result insert fail. */
    @MockitoSpyBean ComplianceResultRepository results;

    @BeforeEach
    void cleanDatabase() {
        truncateTables();
    }

    @Test
    void processesAnEventOnce() {
        UserRegisteredEvent event = event("NL");

        ComplianceOutcome outcome = service.process(event);

        assertThat(outcome).isEqualTo(ComplianceOutcome.PROCESSED);
        assertThat(processedEvents.count()).isEqualTo(1);
        assertThat(results.count()).isEqualTo(1);
        ComplianceResult result = results.findByEventId(event.eventId()).orElseThrow();
        assertThat(result.decision()).isEqualTo(ComplianceDecision.APPROVED);
        assertThat(result.registrationId()).isEqualTo(event.registrationId());
    }

    @Test
    void sameEventTwiceIsIgnoredTheSecondTime() {
        // Review focus 1: the same Kafka record delivered twice has exactly one effect.
        UserRegisteredEvent event = event("NL");
        service.process(event);

        ComplianceOutcome second = service.process(event);

        assertThat(second).isEqualTo(ComplianceOutcome.DUPLICATE_IGNORED);
        assertThat(processedEvents.count()).isEqualTo(1);
        assertThat(results.count()).isEqualTo(1);
    }

    @Test
    void flaggedCountryIsRecordedAsFlagged() {
        UserRegisteredEvent event = event("XX");

        service.process(event);

        ComplianceResult result = results.findByEventId(event.eventId()).orElseThrow();
        assertThat(result.decision()).isEqualTo(ComplianceDecision.FLAGGED);
        assertThat(result.reason()).contains("XX");
    }

    @Test
    void resultInsertFailureRollsBackTheLedgerRow() {
        doThrow(new RuntimeException("boom")).when(results).insert(any());

        assertThatThrownBy(() -> service.process(event("NL"))).hasMessage("boom");
        // Without the rollback the event would look "processed" forever, with no result ever written.
        assertThat(processedEvents.count()).isZero();
    }

    private static UserRegisteredEvent event(String countryCode) {
        return new UserRegisteredEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "ada@example.com", "Ada Lovelace", countryCode, Instant.now(), Instant.now());
    }
}
