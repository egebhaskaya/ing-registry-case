package com.example.compliance.models;

import com.example.compliance.services.ComplianceDecision;
import java.time.Instant;
import java.util.UUID;

/**
 * One row of {@code compliance_results}: the verdict recorded for one event.
 */
public record ComplianceResult(
                UUID eventId,
                UUID userId,
                UUID registrationId,
                ComplianceDecision decision,
                String reason,
                Instant checkedAt) {
}
