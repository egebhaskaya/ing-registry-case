package com.example.compliance.services;

/** A decision plus the human-readable reason the rule gives for it. */
public record Verdict(ComplianceDecision decision, String reason) {
}
