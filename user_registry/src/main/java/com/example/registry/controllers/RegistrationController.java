package com.example.registry.controllers;

import com.example.registry.dtos.RegistrationAcceptedResponse;
import com.example.registry.dtos.RegistrationRequest;
import com.example.registry.services.RegistrationIntakeService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * POST /registrations: the asynchronous registration intake (design section 4).
 *
 * <p>
 * The controller is deliberately thin (design section 10): it maps HTTP to a
 * service call and back.
 * Spring does the plumbing declared by the annotations:
 * <ul>
 * <li>{@code @RequestHeader("Idempotency-Key") UUID}: the header is required
 * and must parse as a UUID;
 * missing or malformed gives a 400 before this method runs.</li>
 * <li>{@code @Valid @RequestBody}: the JSON body is converted into the record
 * and its constraints are
 * checked; a violation (or an unknown field) is a 400.</li>
 * <li>{@code @ResponseStatus(ACCEPTED)}: a normal return is a 202 with the
 * receipt as JSON.</li>
 * </ul>
 * There is no status or lookup endpoint by design; completion is verified
 * through the backend.
 */
@RestController
public class RegistrationController {

    private final RegistrationIntakeService intake;

    public RegistrationController(RegistrationIntakeService intake) {
        this.intake = intake;
    }

    @PostMapping("/registrations")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public RegistrationAcceptedResponse register(@RequestHeader("Idempotency-Key") UUID idempotencyKey,
            @Valid @RequestBody RegistrationRequest request) {
        return intake.accept(idempotencyKey, request);
    }
}
