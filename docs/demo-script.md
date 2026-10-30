# Demo

1. Start Docker, then run `make up` from the repository root.
2. Run `make registry` and `make compliance` in separate terminals.
3. Open [Registration Case](http://localhost:3000/d/registration-case): Last 15 minutes, refresh 5s.
4. Run `make k6`. Watch HTTP acceptance, Rabbit backlog, Registry commits and Compliance catching up.
   Wait for the queue to drain before the next run.
5. Run `make k6-sync`. HTTP success now follows the same worker's Registry commit. Compliance still
   runs independently. Response timing also affects the closed-loop submission rate.
6. Stop Registry, edit its `application.properties`, and restart with `make registry`. Change one
   setting at a time: workers, prefetch, pool size or virtual-thread mode. Re-run either load test.
7. Use `make smoke` for a real end-to-end count check, `make connector` for CDC status and `make lag`
   for Compliance's Kafka backlog. `make test` runs automated correctness checks.

Keep client settings fixed when comparing application settings. Repeat measurements; all components
share one machine. A sync timeout may still finish later: retry the same key and body.
