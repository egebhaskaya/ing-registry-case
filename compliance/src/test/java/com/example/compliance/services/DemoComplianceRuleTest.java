package com.example.compliance.services;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.compliance.config.ComplianceProperties;
import com.example.compliance.messaging.UserRegisteredEvent;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure unit test of the one deterministic demo rule (design section 4). */
class DemoComplianceRuleTest {

    private final DemoComplianceRule rule = new DemoComplianceRule(new ComplianceProperties(
            new ComplianceProperties.Kafka("outbox.event.user", 4),
            new ComplianceProperties.DemoRule(List.of("XX", "ZZ"))));

    @Test
    void flagsCountriesOnTheDemoList() {
        Verdict verdict = rule.decide(event("XX"));

        assertThat(verdict.decision()).isEqualTo(ComplianceDecision.FLAGGED);
        assertThat(verdict.reason()).contains("XX");
    }

    @Test
    void approvesEverythingElse() {
        Verdict verdict = rule.decide(event("NL"));

        assertThat(verdict.decision()).isEqualTo(ComplianceDecision.APPROVED);
        assertThat(verdict.reason()).isNotBlank();
    }

    private static UserRegisteredEvent event(String countryCode) {
        return new UserRegisteredEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "ada@example.com", "Ada Lovelace", countryCode, Instant.now(), Instant.now());
    }
}
