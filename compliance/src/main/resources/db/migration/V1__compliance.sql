-- Compliance schema, version 1 (design section 4 "Compliance data").
-- Two tables written in ONE transaction per event: the ledger that makes redelivery harmless,
-- and the business result. Kafka delivers at least once; this is what turns it into exactly-once effect.

-- Idempotency ledger: one row per event id ever processed. The PRIMARY KEY is the deduplication rule;
-- the service inserts with ON CONFLICT DO NOTHING and treats "0 rows inserted" as "already done".
CREATE TABLE processed_events (
    event_id        UUID PRIMARY KEY,          -- stable id from the outbox row / event payload
    user_id         UUID NOT NULL,             -- kept for correlation with the Registry database
    registration_id UUID NOT NULL,             -- the client's Idempotency-Key, for log correlation
    submitted_at    TIMESTAMPTZ NOT NULL,      -- when the API accepted the request: end-to-end latency start
    processed_at    TIMESTAMPTZ NOT NULL
);

-- The deterministic demo verdict for the event. Referencing the ledger row guarantees a result can
-- never exist without its ledger entry, whatever order the inserts run in.
CREATE TABLE compliance_results (
    event_id        UUID PRIMARY KEY REFERENCES processed_events (event_id),
    user_id         UUID NOT NULL,
    registration_id UUID NOT NULL,
    decision        VARCHAR(16) NOT NULL,      -- APPROVED or FLAGGED
    reason          VARCHAR(255) NOT NULL,     -- human-readable explanation of the demo rule outcome
    checked_at      TIMESTAMPTZ NOT NULL
);
