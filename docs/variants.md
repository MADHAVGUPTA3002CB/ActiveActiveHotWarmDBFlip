# Flip variant reference

Every variant moves the same retiring timeslot and must reach the same final invariant: retiring leaves are detached, all accepted retiring writes are durably present on warm, and ownership changes through an exact compare-and-set. The variants change how traffic is isolated, how new retiring work is fenced, and how completion is proven.

| Variant | Source topology | Foreground write rule | Completion proof | Detach behavior |
|---|---|---|---|---|
| A | Shared | Hot state-only admission once per API batch | LSN + sink offsets | Concurrent, serial |
| B | Isolated | Original retiring park | LSN + sink offsets | Concurrent, serial |
| B+ | Isolated | Original retiring park | LSN + sink offsets; immediate heartbeat | Concurrent, serial |
| D | Isolated | Gate lock + epoch per table operation | LSN + sink offsets | Concurrent |
| E | Isolated | One optimistic gate admission per API batch | LSN + sink offsets | Concurrent |
| F | Isolated | E foreground path | Exact per-leaf Kafka marker + warm receipt | Concurrent |
| G | Isolated | E foreground path | Exact per-leaf marker + receipt | Atomic detach-marker, serial |
| H | Isolated | State-only admission once per API batch | Exact per-leaf marker + receipt | Atomic detach-marker, parallel |
| H-Prod | Shared (single connector) | State-only admission once per API batch | Exact per-leaf marker + receipt | Atomic detach-marker, parallel |
| H-DD-Prod | Generation-pinned lanes (two persistent connectors) | State-only admission once per API batch | Exact per-leaf marker + receipt | Atomic detach-marker, parallel; rolling generations |

## A — Shared CDC baseline

A uses one publication, logical replication slot, and Debezium source connector for both active and retiring leaves. Its foreground path now matches H-Prod: the first operation reads the hot gate state once for the whole API batch, later operations do not reread it, and each table operation commits separately. A still uses a PostgreSQL WAL fence and transaction-aware Kafka sink consumer offsets—not markers—to prove that all retiring data reached warm.

Heavy active traffic can delay retiring progress inside the shared Debezium connector queue and Kafka emission path. This is the baseline against which source isolation and proof choice are measured. Results produced by the older warm-tracker form of A must be labeled as legacy and are not foreground-throughput comparable with the revised A.

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

The prototype currently requires isolated active/migration sources when H is selected. H's exact-marker correctness proof does not fundamentally require that split. The [single-source production guide](variant-h-production-single-debezium.md) defines the H-Prod lifecycle, while the [generation-pinned connector-lane design](variant-h-generation-pinned-connectors.md) proposes alternating persistent source lanes when a shared connector cannot meet the marker-latency SLO. The [feasibility research](variant-h-production-feasibility.md) keeps the earlier topology comparison.

## H-Prod — H on the single shared source

H-Prod runs exactly H's flip algorithm — state-only batch admission, parallel atomic
detach-marker transactions, exact marker observation, and warm receipts — while one shared
publication, slot, and Debezium source connector carries both active and retiring changes.

This is the production topology candidate from the
[single-Debezium implementation guide](variant-h-production-single-debezium.md): the marker
proof never reads slot LSN, so removing the second connector does not weaken correctness. What
changes is latency coupling: markers share the one connector's queue with active traffic, so
marker emission inherits any active-lane source backlog present at flip time. Admission
thresholds on source lag bound that cost.

H-Prod requires an environment created with `SOURCE_TOPOLOGY=shared`. The serial marker
variants F and G remain isolated-only.

## H-DD-Prod — rolling generations on pinned connector lanes

H-DD-Prod implements the full
[generation-pinned connector-lane design](variant-h-generation-pinned-connectors.md) as a
running system. `SOURCE_TOPOLOGY=lanes` creates two persistent Debezium connectors (lane A and
lane B) with generation-independent regex capture patterns and one empty publication each. The
rolling driver then repeats the complete lifecycle per 12-hour generation:

1. **Provision** the next generation on the free lane: create the leaves (with the
   key-immutability trigger and bound `CHECK`), private marker tables, one-partition Kafka
   topics, publication membership, timeslot window, routes, gate, and warm tracker row — then
   prove the whole CDC path with an exact canary marker and warm receipt, all without a
   connector restart.
2. **Rotate** at the boundary: the new generation becomes active on its lane while the previous
   generation demotes to retiring on its original lane, which quiesces because the new active
   traffic flows through the other connector.
3. **Flip** the retiring generation with Variant H's parallel atomic detach-marker
   transactions (`lock_timeout` protected), exact marker observation, warm receipts, the
   conservative sink-offset gate, hot/warm row parity, and catalog verification before the
   ownership compare-and-set.
4. **Release** the lane for the generation after next.

The hot admission layer is timeslot-window driven (`flipbench_guard.timeslot_windows`), so new
generations are data, not schema changes. Run it with:

```bash
make up-rf3 SOURCE_TOPOLOGY=lanes
make setup-rf3 SOURCE_TOPOLOGY=lanes TABLE_COUNT=5
make h-dd-prod-rolling-rf3 SOURCE_TOPOLOGY=lanes GENERATIONS=3
```

## How to choose a variant for a test

- Use A as the shared-source baseline.
- Use B versus A to isolate connector head-of-line effects.
- Use B+ versus B to measure source wake-up delay.
- Use D versus E to measure per-operation gate safety overhead against optimistic batch admission.
- Use F versus E to compare exact marker proof with LSN/offset proof.
- Use G versus F to measure the cost/benefit of atomic detach-marker ordering.
- Use H versus G to measure parallel detach scaling as table count increases.
- Use H-Prod versus H to measure the single shared connector's marker-latency cost against the
  isolated migration lane at matched traffic.
- Use H-DD-Prod to validate the rolling production lifecycle: repeated provision/rotate/flip
  cycles on generation-pinned lanes with no connector restarts.

Do not compare variants from different table counts, traffic mixes, admission thresholds, Docker generations, or source topologies as if they were matched. Use the plan runner with repeated, randomized cases and preserve the raw run evidence.
