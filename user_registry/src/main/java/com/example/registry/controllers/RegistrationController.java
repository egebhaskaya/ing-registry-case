package com.example.registry.controllers;

import com.example.registry.dtos.RegistrationAcceptedResponse;
import com.example.registry.dtos.RegistrationCompletedResponse;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.services.RegistrationIntakeService;
import com.example.registry.services.RegistrationOutcome;
import com.example.registry.services.RegistrationResult;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Both routes enqueue the same command; sync additionally waits for the
 * Registry commit.
 */
@RestController
@RequestMapping("/registrations")
public class RegistrationController {
    private final RegistrationIntakeService intake;

    public RegistrationController(RegistrationIntakeService intake) {
        this.intake = intake;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public RegistrationAcceptedResponse register(@RequestHeader("Idempotency-Key") UUID idempotencyKey,
            @Valid @RequestBody RegistrationRequest request) {
        return intake.accept(idempotencyKey, request);
    }

    @PostMapping("/sync")
    public ResponseEntity<RegistrationCompletedResponse> registerSynchronously(
            @RequestHeader("Idempotency-Key") UUID idempotencyKey,
            @Valid @RequestBody RegistrationRequest request) {
        RegistrationResult result = intake.register(idempotencyKey, request);
        HttpStatus status = result.outcome() == RegistrationOutcome.CREATED ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(RegistrationCompletedResponse.from(result.user()));
    }
}