package com.example.registry.controllers;

import com.example.registry.AbstractIntegrationTest;
import com.example.registry.dtos.RegistrationRequest;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * HTTP contract of POST /registrations through the real embedded server (design
 * section 4).
 * The 503 paths are covered by RegistrationRequestServiceTest; here the broker
 * is real and healthy.
 */
@AutoConfigureRestTestClient
class RegistrationControllerTest extends AbstractIntegrationTest {

    @Autowired
    RestTestClient client;

    @Test
    void acceptsValidRegistrationWith202() {
        UUID key = UUID.randomUUID();

        // A unique email: this request really is consumed and persisted by the running
        // listener,
        // and the flow tests share this context's database.
        client.post().uri("/registrations")
                .header("Idempotency-Key", key.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Ada Lovelace", "controller-" + key + "@example.com", "NL"))
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.registrationId").isEqualTo(key.toString())
                .jsonPath("$.status").isEqualTo("ACCEPTED");
    }

    @Test
    void missingIdempotencyKeyIs400() {
        client.post().uri("/registrations")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL"))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void nonUuidIdempotencyKeyIs400() {
        client.post().uri("/registrations")
                .header("Idempotency-Key", "not-a-uuid")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Ada Lovelace", "ada@example.com", "NL"))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void invalidEmailIs400() {
        client.post().uri("/registrations")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new RegistrationRequest("Ada Lovelace", "not-an-email", "NL"))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void unknownJsonFieldIs400() {
        // Review focus 2: the contract is exactly three fields; an extra one is a
        // client error.
        client.post().uri("/registrations")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"fullName\":\"Ada\",\"email\":\"ada@example.com\",\"countryCode\":\"NL\",\"nickname\":\"x\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }
}
