package com.example.registry.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps worker business conflicts to 409 and intake failures or uncertain outcomes to distinct 503 reasons. */
@RestControllerAdvice
public class ApiExceptionHandler {

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
        return problem("Registration outcome unknown", e.getMessage(), e.reason());
    }

    private static ProblemDetail problem(String title, String detail, String reason) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, detail);
        problem.setTitle(title);
        problem.setProperty("reason", reason);
        return problem;
    }
}
