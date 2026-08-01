# Flipbench: hot-to-warm ownership prototype

Flipbench measures and verifies this non-barrier ownership path:

```text
hot PostgreSQL -> Debezium PostgreSQL source -> one Kafka topic per leaf
               -> one shared Debezium JDBC sink -> warm PostgreSQL
               -> LSN proof + component-wise offset proof -> warm_primary
```

The selected design is `publish_via_partition_root=false`. Current and next-timeslot leaves have separate topics, and every leaf topic has exactly one Kafka partition. Active traffic continues while only the retiring timeslot is quiesced and transferred.

## Implemented production-parity controls

- 5, 10, 15, or 20 partitioned tables generated from one immutable manifest.
- One publication, logical slot and source connector per cell.
- One anchored-regex JDBC sink connector for all allowlisted active and retiring leaf topics. `RegexRouter` maps both timeslots to the correct warm parent table.
- Exact manifest alternation prevents a five-table sink from subscribing to table 06–99 topics. Source and sink use separate least-privilege, non-superuser database roles.
- A timeslot-scoped lifecycle lock, guarded ownership CAS, and independent active-heavy/retiring-light writers.
- Serial `DETACH PARTITION CONCURRENTLY`, exact hot system identity and fence LSN, durable per-topic target vector `E`, and component-wise committed-next-offset checks.
- Four ownership proofs before grant: catalog detached, source fence reached, target vector frozen, and every sink offset reached its target.
- `warm_primary` ends the ownership transfer. Full row-count/checksum reconciliation runs afterward and separately controls garbage-collection eligibility.
- Automatic fail-closed recovery: pending detaches are finalized, detached leaves are reattached, the catalog is verified, and ownership returns to `hot_primary`. A failed recovery remains `recovering`.
- Configurable Connect offset interval/timeout, distributed-worker rebalance/session/heartbeat settings, JDBC batching/pool size, Kafka replication factor and `min.insync.replicas`.
- Optional three-broker local Kafka profile with data/internal-topic RF=3 and `min.insync.replicas=2`.
- Machine-readable topology, workload snapshots, stage timings, recovery outcome, verification outcome and effective tuning in `results/<run-id>/run.json`.

## Local profiles and limits

`make up` uses one Kafka broker (RF=1). Use it for fast correctness and relative tuning only.

`make up-rf3` runs three Kafka broker/controller containers on the same laptop. It verifies Kafka durability configuration and protocol overhead, but it is not equivalent to three failure-independent hosts. The default local ceilings are 512 MB per Kafka JVM and 768 MB per Connect JVM because the Docker VM exposes about 6.2 GB. Every result records these values.

The prototype intentionally skips separate physical nodes, production hardware matching, network emulation, TLS and SASL. Exact production schema parity and traffic replay remain blocked until the real DDL/index/constraint set and a sanitized traffic distribution are supplied.

This remains a non-adversarial local benchmark. Connect REST and Kafka PLAINTEXT listeners have no authentication, and the lifecycle runner intentionally retains an administrative PostgreSQL role. Those controls must change before deployment.

## Run

```bash
cd /Users/madhav.gupta/Documents/DB_PROJECT/prototype
install -m 600 .env.example .env
# Replace POSTGRES_PASSWORD in .env.
make preflight
make test

# Fast RF=1 path
make up
make setup TABLE_COUNT=5
make benchmark-prodlike TABLE_COUNT=5

# Production-shaped local Kafka durability
make reset
make up-rf3
make setup-rf3 TABLE_COUNT=5
make benchmark-prodlike-rf3 TABLE_COUNT=5
```

The production-shaped defaults enforce a 200 ms writer-park budget and reserve 50 ms for safe revert. On this laptop that budget usually exercises the revert path. A wider diagnostic budget can characterize the complete forward path:

The source, sink and PostgreSQL administrator credentials are always distinct. Blank `CDC_PASSWORD` and `SINK_PASSWORD` values derive role-specific one-way credentials in memory for this local prototype; explicit distinct values may be supplied instead.

```bash
make benchmark-prodlike-rf3 \
  TABLE_COUNT=5 \
  PRODLIKE_ACTIVE_BATCH_PER_TABLE=20 \
  PRODLIKE_PARK_BUDGET_MS=1500 \
  PRODLIKE_REVERT_RESERVE_MS=100
```

Successful flips intentionally leave retiring leaves detached. Reset volumes before another independent ownership run:

```bash
make reset-rf3
```

## Interactive playground

The playground controls the real local writers and reads live PostgreSQL slot and Kafka consumer-offset metrics. It does not generate simulated latency numbers.

For a fresh RF3 experiment:

```bash
make reset-rf3
make up-rf3
make setup-rf3 TABLE_COUNT=5
make playground-api-rf3
make playground-supervisor   # separate terminal
make playground-ui           # separate terminal
```

Open [http://localhost:3000](http://localhost:3000). From the UI you can start or stop active/retiring traffic, change both batch sizes and pauses while traffic is running, edit admission thresholds, start the guarded flip, and inspect the t1→t13 timing breakdown.

Live batch changes reset the stable admission window, and both workload and threshold settings freeze once a flip starts. The API limits the largest cross-table transaction payload to 16 MiB so an accidental UI value cannot request an unbounded local batch.

After the first setup, `make playground-rf3` starts the API, restart supervisor and UI together. The UI's **New experiment** control performs the same scoped RF3 reset/setup sequence. It atomically puts the API into maintenance mode before checking ownership, refuses to reset during a running flip, preserves the host-mounted `results/` directory, and requires typing `RESET` exactly. The supervisor retries a transient control-API disconnect before failing closed, preserves precise safety rejections such as a running flip, and reports a recovery hint without deleting volumes. If it reports `control_api_unavailable`, restore the current API with `make playground-api-rf3 TABLE_COUNT=<current table count>`, wait for `http://localhost:8090/api/health`, and retry from the UI.

When `warm_primary` is granted, the runner atomically saves `results/<run-id>/ownership-grant.json` before post-grant verification. This file is historical evidence only; PostgreSQL remains the live ownership authority. The later `run.json` replaces the pending checkpoint in the UI's saved-run history once verification finishes.

`TABLE_COUNT` supports 5, 10, 15, or 20 and is fixed when the local environment is set up. Each table currently has exactly two database leaves—active and retiring—and every leaf topic has one Kafka partition. Those topology fields are read-only in the live UI because changing them requires real DDL, publication, topic, and connector reconfiguration; the UI does not pretend an unsupported topology was tested.

The control API and restart supervisor are published only on host loopback at `127.0.0.1:8090` and `127.0.0.1:8091`. They accept browser mutations only from `http://localhost:3000`, cap JSON request bodies, and use fixed reset commands and the existing guarded insert/flip paths. They assume a trusted single-user laptop: Origin/Host checks protect the browser workflow, but the loopback service is not an authorization boundary against another local process. This is a local benchmark control plane, not a production service.

`down` does not erase data. Topic deletion is deliberately outside the flip and must wait until the replay/recovery retention policy permits garbage collection.

## Production-shaped admission

Two independent writers model the selected workload:

- active timeslot: heavy, continuous writes;
- retiring timeslot: light writes that are stopped at `t2q`;
- source and sink connectors remain `RUNNING`;
- admission requires bounded source-slot WAL lag, bounded retiring sink lag, all tasks running, and a stable sample window;
- active insert counts are captured at `t1`, `t2q`, and `t13` to prove active traffic continued through the retiring flip.

The lag thresholds are configuration, not a hardcoded sleep or manufactured backlog. A preparation failure occurs before ownership locks and leaves the tracker at `hot_primary`.

## Timing fields

| Field | Meaning |
|---|---|
| `t0` | Preflight starts |
| `t1` | Connector and lag admission passes |
| `t2` | Durable retiring-timeslot ownership lock is created |
| `t2q` | Lifecycle locks acquired and retiring writer joined; active writer remains live |
| `t3_i`, `t4_i` | Per-table detach start/end |
| `t5`, `t6` | Exact hot fence LSN captured and persisted on warm |
| `t7` | Slot `confirmed_flush_lsn` reaches the fence |
| `t8`, `t9` | Kafka target next-offset vector `E` captured and persisted |
| `t10`, `t11` | Tested sink commit contract and component-wise `C >= E` hold |
| `t12` | Durable `drained` CAS completes |
| `t13` | Durable `warm_primary` CAS completes; writer park ends |
| `tverify` | Post-grant hot/warm reconciliation completes; separately sets GC eligibility |
| `trevert_start`, `trevert_end` | Safe reattach/revert interval after a forward-path failure |

`writer_park_ns` is `t13 - t2`. `validation_ns` is post-grant verification. `whole_lifecycle_ns` includes both. All use one process-local monotonic clock.

For reverted results, `writer_park_ns` is `trevert_end - t2`, `forward_until_failure_ns` isolates the forward attempt, and `revert_ns` measures the reserved recovery interval.

## Hard correctness limits

- A missing target offset is corruption; a missing committed offset is pending.
- Kafka offsets are next offsets and are never summed to prove completion.
- Sink errors stop the connector (`errors.tolerance=none`); skipped records invalidate the proof.
- `created_at` and `id` key movement is rejected.
- The source proof fails if cell, PostgreSQL system identifier, database or slot identity differs.
- Recovery claims `hot_primary` only after every leaf is catalog-verified as attached and all exact-attempt CAS operations succeed.
- Offset proof grants ownership; checksum verification grants cleanup eligibility. Failed verification does not silently delete data.

Measured evidence and interpretation are in [the measured-results report](../docs/research/flip-prototype-measured-results-2026-08-01.md).

## Verification status

- 84 tests are discovered locally; environment-dependent Kafka/live-stack tests run inside the runner image.
- Five RF3 live integration contracts pass.
- The deterministic safety layer (`core`, settings, routing and result validation) has 82% branch-aware coverage.
- Full-package branch coverage is currently 38% because the database/Connect/Kafka orchestration and crash-recovery paths are exercised live but not yet driven under coverage. This does **not** meet the project-wide 80% production gate; automated partial-detach/crash injection remains required before production qualification.
