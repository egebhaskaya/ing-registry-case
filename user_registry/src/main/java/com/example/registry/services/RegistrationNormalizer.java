package com.example.registry.services;

import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.messaging.RegistrationPayload;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.stereotype.Service;

/**
 * Turns a raw request into the canonical payload and computes its fingerprint.
 *
 * <p>
 * Design §4: "validate and normalize fields before publishing". Normalizing
 * before publishing means
 * two submissions that differ only cosmetically ("ADA@x.com" vs "ada@x.com")
 * produce the same payload,
 * the same fingerprint, and therefore hit the same database uniqueness rules.
 *
 * <p>
 * {@code @Service} registers this class as a Spring bean so other beans can
 * receive it through
 * their constructor (dependency injection). It has no state, so one shared
 * instance is enough.
 */
@Service
public class RegistrationNormalizer {

    /**
     * Trim, collapse any run of whitespace inside the name to one space, lower-case
     * the email,
     * upper-case the country. ({@code "\\s+"} is the regex for whitespace; a single
     * backslash would be
     * Java's own escape for a plain space and would miss tabs and newlines.)
     */
    public RegistrationPayload normalize(RegistrationRequest request) {
        String fullName = request.fullName().trim().replaceAll("\\s+", " ");
        String email = request.email().trim().toLowerCase();
        String countryCode = request.countryCode().trim().toUpperCase();
        return new RegistrationPayload(fullName, email, countryCode);
    }

    /**
     * SHA-256 of the three normalized fields, as 64 lowercase hex characters.
     *
     * <p>
     * The fingerprint is what lets the consumer tell "the same request delivered
     * twice" (same
     * registration id, same fingerprint → ignore) from "the same key reused with
     * different data"
     * (same registration id, different fingerprint → conflict). Fields are joined
     * with a newline so
     * that "ab"+"c" and "a"+"bc" cannot collide.
     */
    public String fingerprint(RegistrationPayload payload) {
        String canonical = payload.fullName() + "\n" + payload.email() + "\n" + payload.countryCode();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory in every Java runtime; this cannot happen in practice.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
