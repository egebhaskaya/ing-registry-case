package com.example.compliance.services;

import com.example.compliance.config.ComplianceProperties;
import com.example.compliance.messaging.UserRegisteredEvent;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

/**
 * The one deterministic example rule (design section 4): a country code on the
 * configured demo watch
 * list is FLAGGED, everything else is APPROVED. This is explicitly demo logic,
 * not regulatory screening;
 * the defaults ("XX", "ZZ") are ISO 3166 user-assigned codes that no real
 * country uses.
 *
 * <p>
 * Deterministic matters for the demonstration: the same input always produces
 * the same result, so
 * a redelivered event cannot produce a different verdict than the first
 * delivery would have.
 */
@Service
public class DemoComplianceRule {

    private final Set<String> flaggedCountries;

    public DemoComplianceRule(ComplianceProperties props) {
        this.flaggedCountries = props.demoRule().flaggedCountries().stream()
                .map(code -> code.trim().toUpperCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    public Verdict decide(UserRegisteredEvent event) {
        String country = event.countryCode().trim().toUpperCase(Locale.ROOT);
        if (flaggedCountries.contains(country)) {
            return new Verdict(ComplianceDecision.FLAGGED,
                    "demo rule: country code " + country + " is on the demo watch list");
        }
        return new Verdict(ComplianceDecision.APPROVED, "demo rule: no match");
    }
}
