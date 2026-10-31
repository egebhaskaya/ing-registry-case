# Demo

1. Start Docker, then run `make up` from the repository root.
2. Run `make registry` and `make compliance` in separate terminals.
3. Open [Registration Case](http://localhost:3000/d/registration-case): Last 5 minutes, refresh 1s.
4. Run `make k6`. Watch HTTP acceptance, Rabbit backlog, Registry commits and Compliance catching up.
   Wait for the queue to drain before the next run.
5. Run `make k6-sync`. The HTTP handler calls the shared processor directly and returns after the
   Registry commit. Rabbit queue activity is not expected from this run. Compliance still runs
   independently. Response timing also affects the closed-loop submission rate.
6. Run `make k6-batched`. Requests still arrive one at a time and receive `202`; workers group them
   into database batches on a separate Rabbit queue. Watch Registry commits/s and database waiting,
   then wait for the batch queue and Compliance to drain. HTTP latency measures acceptance here too.
7. Stop Registry, edit its `application.properties`, and restart with `make registry`. Change one
   setting at a time: async Rabbit workers/prefetch, shared pool size or virtual-thread mode. Rabbit
   worker settings affect queued runs; pool and HTTP admission also govern `make k6-sync`.
   For batch comparisons, try `registry.batch.size=10`, `100`, then `250` with the same arrival rate.
8. Use `make smoke` for an original async end-to-end count check, `make connector` for CDC status and `make lag`
   for Compliance's Kafka backlog. `make test` runs automated correctness checks.

For a sustained comparison, run `make k6 cont 200`, stop with Ctrl+C once and wait for
the async backlog to drain. Then run `make k6-sync cont 200` and `make k6-batched cont 200` separately
at the same target rate, allowing each pipeline to finish between runs. Replace 200 with your desired
requests per second. The 100 ms collection target does not require reaching 100 registrations;
smaller batches flush on a receive or gathering timeout.
The shared HTML report includes each stopped run and any arrivals dropped by the load generator.

Keep client settings fixed when comparing application settings. Repeat measurements; all components
share one machine. Retry a lost HTTP response with the same key and body to preserve idempotency.
