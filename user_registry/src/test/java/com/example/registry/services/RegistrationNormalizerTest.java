package com.example.registry.services;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.messaging.RegistrationPayload;
import org.junit.jupiter.api.Test;

/**
 * Pure unit test: no Spring, no containers. The normalizer is plain logic, so
 * it is tested as plain Java.
 */
class RegistrationNormalizerTest {

    private final RegistrationNormalizer normalizer = new RegistrationNormalizer();

    @Test
    void normalizesWhitespaceAndCase() {
        RegistrationPayload payload = normalizer.normalize(
                new RegistrationRequest("  Ada   Lovelace ", " Ada.Lovelace@Example.COM ", " nl "));

        // records compare by value, so one equals() checks all three fields
        assertThat(payload).isEqualTo(new RegistrationPayload("Ada Lovelace", "ada.lovelace@example.com", "NL"));
    }

    @Test
    void collapsesTabsAndNewlinesInsideTheName() {
        // Any run of whitespace, not only spaces, becomes a single space.
        RegistrationPayload payload = normalizer.normalize(
                new RegistrationRequest("Ada\t\tLovelace\nByron", "ada@example.com", "NL"));

        assertThat(payload.fullName()).isEqualTo("Ada Lovelace Byron");
    }

    @Test
    void fingerprintIsStableAcrossCosmeticDifferences() {
        RegistrationPayload a = normalizer.normalize(new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL"));
        RegistrationPayload b = normalizer
                .normalize(new RegistrationRequest(" Ada  Lovelace", "ADA@example.com", "nl"));

        assertThat(normalizer.fingerprint(a)).isEqualTo(normalizer.fingerprint(b));
    }

    @Test
    void fingerprintChangesWhenContentChanges() {
        RegistrationPayload a = normalizer.normalize(new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL"));
        RegistrationPayload b = normalizer.normalize(new RegistrationRequest("Ada Byron", "ada@example.com", "NL"));

        assertThat(normalizer.fingerprint(a)).isNotEqualTo(normalizer.fingerprint(b));
    }

    @Test
    void fingerprintIsSha256Hex() {
        RegistrationPayload a = normalizer.normalize(new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL"));

        assertThat(normalizer.fingerprint(a)).matches("^[0-9a-f]{64}$");
    }
}
