# How /sync works

All three routes use the same validation, command format, registration rules and Compliance pipeline.
Async queues its command in RabbitMQ; sync calls the processor directly. The third `/batched` route
also queues one command per request, then groups commands into database transactions.

## Async: accepted and queued

1. Validate the request, including its `Idempotency-Key`, and acquire a shared HTTP intake slot.
2. Normalize the payload, build a command and publish it to `registration.commands`.
3. Wait for RabbitMQ's publisher confirmation; if accepted and routed, return `202 Accepted`.
4. Release the HTTP slot. A Rabbit worker later calls the shared processor and commits user + outbox.

The response establishes queue acceptance. A business conflict found later follows the async
worker's dead-letter path. Transient worker failures retain their bounded retries.

## Sync: direct processing, then returned

1. Validate the same request, acquire a shared HTTP intake slot and normalize its payload.
2. Call `RegistrationProcessor` directly on the HTTP handler thread. No Rabbit publication occurs.
3. Obtain a connection from the same Hikari pool used by Rabbit workers.
4. Commit the user and outbox event together through the shared transactional service.
5. Return the stored user: `201 Created`, `200 OK` for an identical replay, or `409 Conflict`.
6. Release the HTTP intake slot on success or failure.

The transaction proxy commits before the processor returns a successful result. There is no sync
reply address, reply queue or broker reply timeout. Sync database failures propagate to HTTP without
Rabbit retries. The thread may be virtual, but its JDBC operations still use a database connection.

Sync success contains the committed user id, registration id, normalized data and `status=REGISTERED`.
It does not establish that Compliance finished. After any Registry path commits, Debezium and
Kafka feed the same Compliance consumer independently.

## Admission and retries

`registry.intake.max-in-flight=64` bounds admitted HTTP work across all routes. Queued requests hold a slot
until publisher confirmation; sync keeps it through processing and commit. A request that cannot
acquire a slot within `registry.intake.admission-wait=200ms` returns `503` before publishing or writing.
Rabbit consumer concurrency and prefetch apply only to queued work. Sync has no separate worker pool;
its concurrency is limited by HTTP admission and database connection availability.

If an HTTP response is lost after a commit, retry using the **same idempotency key and body**. An
identical replay returns the stored user without another outbox event. Concurrent identical requests
are resolved through database uniqueness. Reusing a key with different normalized data conflicts.
Async publisher-confirmation timeouts can still have an unknown outcome; retain the same key there too.

## Comparing the endpoints

`make k6` measures HTTP response after Rabbit queue acceptance; `make k6-sync` measures direct HTTP
processing through Registry commit. Both use the same pool, transaction, threading mode and downstream
Compliance pipeline. Rabbit worker count and prefetch affect only the async run.

In the default burst, each simulated client waits for its response before submitting another request. Sync therefore
paces submissions by registration completion and does not add work to RabbitMQ. Compare processing
rate, database waiting and the common processor-entry-to-commit latency alongside HTTP latency; async
acceptance speed alone is not database throughput. This processing timer records only new successful
registrations and excludes Rabbit queue waiting and downstream Compliance. Its equal timing boundary
does not equalize HTTP and Rabbit worker concurrency. Wait for an earlier async backlog to drain before running sync for a
clean comparison, because the two paths share database connections.

`make k6-batched` adds queued group commits to this comparison. Its HTTP response still measures
acceptance; successful bulk commits record the full group-processing duration for each new registration,
excluding collection waiting. The batched line also includes individual fallback commits; commands
forwarded to the ordinary retry queue can appear under async processing.

For equal scheduled arrival rates, add `cont 200` to any of these commands (replace 200 with your rate). k6 starts new
requests independently of prior response times, up to its configured VU capacity. Stop with Ctrl+C
once; compare observed rates and dropped iterations as well as server metrics. Accepted queued work
can continue after the generator stops.
