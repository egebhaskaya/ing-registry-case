package com.example.registry.controllers;

import com.example.registry.dtos.RegistrationCompletedResponse;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.services.RegistrationOutcome;
import com.example.registry.services.RegistrationResult;
import com.example.registry.services.SynchronousRegistrationService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SynchronousRegistrationController {
    private final SynchronousRegistrationService service;

    public SynchronousRegistrationController(SynchronousRegistrationService service) {
        this.service = service;
    }

    @PostMapping("/registrations/sync")
    public ResponseEntity<RegistrationCompletedResponse> register(
            @RequestHeader("Idempotency-Key") UUID idempotencyKey,
            @Valid @RequestBody RegistrationRequest request) {
        RegistrationResult result = service.register(idempotencyKey, request);
        HttpStatus status = result.outcome() == RegistrationOutcome.CREATED ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(RegistrationCompletedResponse.from(result.user()));
    }
}
