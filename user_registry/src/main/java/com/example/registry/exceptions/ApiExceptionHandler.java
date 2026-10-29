package com.example.registry.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps registration conflicts and intake/processing overload to distinguishable HTTP responses.
 *
 * <p>
 * {@code @RestControllerAdvice} applies to every controller. Each
 * {@code @ExceptionHandler} method
 * turns one exception type into an RFC 9457 problem detail, e.g.
 * 
 * <pre>
 * {"status":503,"title":"Intake overloaded","detail":"...","reason":"overloaded"}
 * </pre>
 * 
 * The 400 cases (missing header, bad UUID, validation, unknown JSON field) are
 * handled by Spring itself
 * because {@code spring.mvc.problemdetails.enabled=true}; this class adds synchronous 409 conflicts
 * and the 503 family.
 *
 * <p>
 * Intake failures are 503 (the service cannot take the request right now) but the
 * {@code reason} property
 * tells a client whether the command definitely did not get through (it may
 * retry immediately) or
 * whether the outcome is unknown (it should retry with the same
 * Idempotency-Key).
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(SynchronousRegistrationOverloadedException.class)
    ResponseEntity<ProblemDetail> synchronousOverloaded(SynchronousRegistrationOverloadedException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "1")
                .body(problem("Registration processing overloaded", e.getMessage(), "sync_overloaded"));
    }

    @ExceptionHandler(ConflictingPayloadException.class)
    ProblemDetail conflictingPayload(ConflictingPayloadException e) {
        return conflict(e.getMessage(), "idempotency_conflict");
    }

    @ExceptionHandler(EmailAlreadyRegisteredException.class)
    ProblemDetail emailAlreadyRegistered(EmailAlreadyRegisteredException e) {
        return conflict(e.getMessage(), "email_already_registered");
    }

    private static ProblemDetail conflict(String detail, String reason) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, detail);
        problem.setTitle("Registration conflict");
        problem.setProperty("reason", reason);
        return problem;
    }

    @ExceptionHandler(IntakeOverloadedException.class)
    ProblemDetail overloaded(IntakeOverloadedException e) {
        return problem("Intake overloaded", e.getMessage(), "overloaded");
    }

    @ExceptionHandler(IntakeRejectedException.class)
    ProblemDetail rejected(IntakeRejectedException e) {
        return problem("Registration command not accepted", e.getMessage(), e.reason());
    }

    @ExceptionHandler(IntakeOutcomeUnknownException.class)
    ProblemDetail unknown(IntakeOutcomeUnknownException e) {
        return problem("Registration outcome unknown", e.getMessage(), "confirm_timeout");
    }

    private static ProblemDetail problem(String title, String detail, String reason) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, detail);
        problem.setTitle(title);
        problem.setProperty("reason", reason);
        return problem;
    }
}
