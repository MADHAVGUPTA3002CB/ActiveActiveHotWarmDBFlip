# Flip variant reference

Every variant moves the same retiring timeslot and must reach the same final invariant: retiring leaves are detached, all accepted retiring writes are durably present on warm, and ownership changes through an exact compare-and-set. The variants change how traffic is isolated, how new retiring work is fenced, and how completion is proven.

| Variant | Source topology | Foreground write rule | Completion proof | Detach behavior |
|---|---|---|---|---|
| A | Shared | Original retiring park | LSN + sink offsets | Concurrent, serial |
| B | Isolated | Original retiring park | LSN + sink offsets | Concurrent, serial |
| B+ | Isolated | Original retiring park | LSN + sink offsets; immediate heartbeat | Concurrent, serial |
| D | Isolated | Gate lock + epoch per table operation | LSN + sink offsets | Concurrent |
| E | Isolated | One optimistic gate admission per API batch | LSN + sink offsets | Concurrent |
| F | Isolated | E foreground path | Exact per-leaf Kafka marker + warm receipt | Concurrent |
| G | Isolated | E foreground path | Exact per-leaf marker + receipt | Atomic detach-marker, serial |
| H | Isolated | State-only admission once per API batch | Exact per-leaf marker + receipt | Atomic detach-marker, parallel |

## A — Shared CDC baseline

A uses one publication, logical replication slot, and Debezium source connector for both active and retiring leaves. The flip uses a PostgreSQL WAL fence and Kafka sink consumer offsets to prove that all retiring data reached warm.

Heavy active traffic can delay retiring progress inside the shared Debezium connector queue and Kafka emission path. This is the baseline against which source isolation is measured.

## B — Isolated CDC with passive heartbeat

B separates active and retiring leaves into disjoint publications, slots, and source connectors while preserving the same leaf topic names and shared JDBC sink. Active connector work no longer occupies the migration connector's internal queue.

The migration slot still scans global PostgreSQL WAL. With no retiring activity, source progress can wait for the normal periodic heartbeat, so isolation alone does not guarantee a fast fence proof.

## B+ — Isolated CDC with immediate heartbeat

B+ keeps B's topology and updates the migration lane's existing heartbeat row once immediately after the fence is recorded. That creates relevant migration-lane work so Debezium promptly advances and acknowledges the fence.

The heartbeat is only a wake-up hint. Correctness still requires the original `confirmed_flush_lsn >= fence_lsn` check and the component-wise sink-offset proof.

## D — Hot-local transactional fence

D moves the ownership gate into hot PostgreSQL. Every selected-table operation calls a restricted function that takes a shared gate-row lock, checks the exact ownership epoch, validates the route, writes one table operation, and commits.

The flip parks the retiring gate with an epoch compare-and-set. PostgreSQL waits for earlier guarded transactions, rejects later retiring transactions, and lets active operations continue through a separate gate row. The per-operation lock/read is safe but adds foreground overhead.

## E — Batch-admitted optimistic detach

E reads the ownership gate once for a bounded API-style batch, then performs one selected-table operation per PostgreSQL transaction without rereading the gate. This models the accepted application rule that a detach race may make a later operation fail and application retry/error handling will resolve it.

Earlier table commits remain durable if a later operation loses the detach race, so E does not provide whole-API-batch atomicity. Stable per-operation idempotency keys are required for safe retries.

## F — Exact per-leaf marker receipts

F keeps E's foreground path but replaces the global LSN and consumer-offset ownership gate with exact markers. After retiring admission is stopped and detaches resolve, it emits one unique marker per retiring leaf into that leaf's existing Kafka topic-partition.

The coordinator observes each exact marker offset and waits for the JDBC sink to commit every exact marker receipt on warm. This directly proves each leaf's data stream has passed the marker without relying on an aggregate offset comparison.

## G — Serial atomic detach and marker

G strengthens F's ordering by running `DETACH PARTITION` and that leaf's marker insert inside the same hot PostgreSQL transaction. Commit makes both visible together; rollback undoes both.

It processes leaves serially. Non-concurrent detach needs a stronger parent-table lock and may briefly block active operations on that parent. More retiring tables increase the serial detach wall time.

## H — Parallel atomic detach and marker

H checks only that the hot gate is `open` on the first operation of each API-style batch; the application sends no ownership epoch. Later operations remain separate commits and rely on detach failure plus application retry/error handling for the accepted race. The flip coordinator still uses the ownership epoch to park and recover the gate safely.

H runs G's exact per-leaf transaction concurrently, using one PostgreSQL connection and transaction per retiring leaf. Independent parent tables can detach in parallel, reducing the all-leaf detach wall time.

All workers must finish successfully before marker observation begins. If any worker fails, ownership is not granted and catalog-driven recovery reattaches every leaf that committed successfully. Parallelism improves latency at the cost of a short burst of database connections and lock work.

## How to choose a variant for a test

- Use A as the shared-source baseline.
- Use B versus A to isolate connector head-of-line effects.
- Use B+ versus B to measure source wake-up delay.
- Use D versus E to measure per-operation gate safety overhead against optimistic batch admission.
- Use F versus E to compare exact marker proof with LSN/offset proof.
- Use G versus F to measure the cost/benefit of atomic detach-marker ordering.
- Use H versus G to measure parallel detach scaling as table count increases.

Do not compare variants from different table counts, traffic mixes, admission thresholds, Docker generations, or source topologies as if they were matched. Use the plan runner with repeated, randomized cases and preserve the raw run evidence.
