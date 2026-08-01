from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from psycopg import sql
from psycopg.types.json import Jsonb

from .connect_api import ConnectClient
from .connector_configs import shared_sink_config, source_config
from .core import (
    AttemptState,
    GateEvidence,
    HotSourceIdentity,
    OffsetVector,
    TimingError,
    TopicPartition,
    build_manifest,
    canonical_manifest_json,
    derive_revert_durations,
    derive_stage_durations,
    overload_lag_vectors,
    offset_gate,
    production_admission_ready,
    source_fence_satisfied,
    transition,
)
from .kafka_io import KafkaControl
from .lifecycle import lifecycle_lock_name
from .postgres_io import connect, hot_identity, parity_for_run, slot_status, wait_slot_lsn
from .results import write_json_atomic, write_ownership_checkpoint_atomic
from .recovery import revert_to_hot
from .settings import Settings


class FlipRunner:
    def __init__(
        self,
        settings: Settings,
        run_id: uuid.UUID,
        timeout_seconds: float,
        poll_seconds: float,
        scenario_metadata: Mapping[str, Any] | None = None,
        writer_quiesce: Callable[[float], int] | None = None,
        writer_inserted_snapshot: Callable[[], int] | None = None,
        writer_is_alive: Callable[[], bool] | None = None,
        active_writer_inserted_snapshot: Callable[[], int] | None = None,
        active_writer_is_alive: Callable[[], bool] | None = None,
        recovery_timeout_seconds: float = 30.0,
    ) -> None:
        self.settings = settings
        self.run_id = run_id
        if timeout_seconds <= 0 or recovery_timeout_seconds <= 0 or not 0.001 <= poll_seconds <= 5:
            raise ValueError("timeout_seconds must be positive and poll_seconds must be between 0.001 and 5")
        self.timeout_seconds = timeout_seconds
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.poll_seconds = poll_seconds
        self.manifest = build_manifest(settings.table_count, settings.cell, settings.timeslot)
        self.timestamps: dict[str, int] = {}
        self.events: list[dict[str, Any]] = []
        self.detach_ns: dict[str, int] = {}
        self._base_ns = time.perf_counter_ns()
        self._attempt_epoch: int | None = None
        self.scenario_metadata = dict(scenario_metadata or {})
        self.writer_quiesce = writer_quiesce
        self.writer_inserted_snapshot = writer_inserted_snapshot
        self.writer_is_alive = writer_is_alive
        self.active_writer_inserted_snapshot = active_writer_inserted_snapshot
        self.active_writer_is_alive = active_writer_is_alive

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("overall writer-park deadline expired")
        return remaining

    def _acquire_session_lock(self, connection: Any, deadline: float) -> None:
        lock_name = lifecycle_lock_name(self.settings.cell, self.settings.timeslot)
        while self._remaining(deadline) > 0:
            acquired = connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (lock_name,),
            ).fetchone()[0]
            if acquired:
                return
            time.sleep(self.poll_seconds)
        raise TimeoutError(f"could not acquire lifecycle lock for {lock_name}")

    def _release_session_lock(self, connection: Any) -> bool:
        lock_name = lifecycle_lock_name(self.settings.cell, self.settings.timeslot)
        return bool(
            connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (lock_name,),
            ).fetchone()[0]
        )

    def mark(self, stage: str, **details: Any) -> None:
        relative_ns = time.perf_counter_ns() - self._base_ns
        self.timestamps[stage] = relative_ns
        self.events.append({"stage": stage, "monotonic_ns": relative_ns, **details})

    def elapsed_ns(self) -> int:
        return time.perf_counter_ns() - self._base_ns

    def run(self, resume_paused: bool, require_nonzero_lag: bool = False) -> Mapping[str, Any]:
        started = datetime.now(timezone.utc)
        attempt_id = uuid.uuid4()
        manifest_json = canonical_manifest_json(self.manifest)
        result_path = self.settings.results_dir / str(self.run_id) / "run.json"
        hot_source: HotSourceIdentity | None = None
        fence_lsn: str | None = None
        target_values: Mapping[TopicPartition, int] = {}
        current_values: Mapping[TopicPartition, int] = {}
        parity_rows: tuple[dict[str, object], ...] = ()
        admission: dict[str, Any] = {}
        outcome = "failed"
        error_payload: dict[str, str] | None = None
        connector_config_sha256: str | None = None
        kafka: KafkaControl | None = None
        workload_inserted_total: int | None = None
        verification_outcome = "not_run"
        verification_error: dict[str, str] | None = None
        workload_by_timeslot: dict[str, Any] = {}
        ownership_granted = False

        with connect(self.settings.warm_dsn, autocommit=True) as warm, connect(
            self.settings.hot_dsn, autocommit=True
        ) as hot:
            try:
                self.mark("t0")
                source = ConnectClient(self.settings.source_connect_url)
                sink = ConnectClient(self.settings.sink_connect_url)
                expected_state = "PAUSED" if resume_paused else "RUNNING"
                source.wait_state(self.settings.source_connector, expected_state)
                live_source_config = source.config(self.settings.source_connector)
                expected_source_config = source_config(self.settings, self.manifest)
                self._verify_live_config("source", live_source_config, expected_source_config)
                sink_connector = self.manifest.tables[0].sink_connector
                sink.wait_state(sink_connector, expected_state)
                live_config = sink.config(sink_connector)
                live_sink_configs: dict[str, Mapping[str, str]] = {sink_connector: live_config}
                self._verify_live_config(
                    sink_connector,
                    live_config,
                    shared_sink_config(self.settings, self.manifest),
                )
                sanitized_configs = {
                    "source": {key: value for key, value in live_source_config.items() if "password" not in key.lower()},
                    "sinks": {
                        name: {key: value for key, value in config.items() if "password" not in key.lower()}
                        for name, config in live_sink_configs.items()
                    },
                }
                connector_config_sha256 = hashlib.sha256(
                    json.dumps(sanitized_configs, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
                partitions = tuple(TopicPartition(route.topic, route.partition) for route in self.manifest.tables)
                group_by_partition = {
                    TopicPartition(route.topic, route.partition): route.sink_group for route in self.manifest.tables
                }
                kafka = KafkaControl(self.settings.kafka_bootstrap)
                healthy_overload = self.scenario_metadata.get("mode") == "healthy-overload"
                production_shaped = self.scenario_metadata.get("mode") == "production-shaped"
                source_lag_records: Mapping[str, int] = {}
                inserted_at_t1: int | None = None
                if healthy_overload:
                    if self.writer_is_alive is None or self.writer_inserted_snapshot is None:
                        raise RuntimeError("healthy-overload writer evidence callbacks are unavailable")
                    baseline_end = self.scenario_metadata["baseline_end_offsets"]
                    min_source_bytes = int(self.scenario_metadata["min_source_lag_bytes"])
                    min_source_records = int(
                        self.scenario_metadata["min_source_lag_records_per_partition"]
                    )
                    min_sink_records = int(
                        self.scenario_metadata["t1_min_sink_lag_records_per_partition"]
                    )
                    max_admitted_rows = int(self.scenario_metadata["max_admitted_rows_per_partition"])
                    admission_deadline = time.monotonic() + float(
                        self.scenario_metadata.get("t1_recheck_timeout_seconds", 10.0)
                    )
                    while time.monotonic() < admission_deadline:
                        if not self.writer_is_alive():
                            raise RuntimeError("healthy-overload writer is not active at t1")
                        inserted_at_t1 = self.writer_inserted_snapshot()
                        if inserted_at_t1 % len(partitions) != 0:
                            raise RuntimeError("healthy-overload writer count is not divisible by table count")
                        committed_per_table = inserted_at_t1 // len(partitions)
                        if committed_per_table > max_admitted_rows:
                            raise RuntimeError(
                                "healthy-overload workload exceeded its admission cap before t1"
                            )
                        before = slot_status(hot, self.settings.cell, self.settings.slot_name)
                        admission_end = kafka.end_offsets(partitions)
                        admission_committed = kafka.committed_offsets(group_by_partition, 2.0)
                        admission_end_by_key = {
                            partition.key: admission_end[partition] for partition in partitions
                        }
                        admission_committed_by_key = {
                            partition.key: admission_committed.get(partition, 0) for partition in partitions
                        }
                        try:
                            source_lag_records, sink_lag_records = overload_lag_vectors(
                                baseline_end,
                                admission_end_by_key,
                                admission_committed_by_key,
                                {partition.key: committed_per_table for partition in partitions},
                            )
                        except TimingError:
                            time.sleep(self.poll_seconds)
                            continue
                        sink_lag = dict(sink_lag_records)
                        if (
                            before.lag_bytes >= min_source_bytes
                            and min(source_lag_records.values()) >= min_source_records
                            and min(sink_lag.values()) >= min_sink_records
                        ):
                            break
                        time.sleep(self.poll_seconds)
                    else:
                        raise TimeoutError("healthy-overload lag did not remain coherent through t1")
                else:
                    before = slot_status(hot, self.settings.cell, self.settings.slot_name)
                    admission_end = kafka.end_offsets(partitions)
                    admission_committed = kafka.committed_offsets(group_by_partition)
                    sink_lag = {
                        item.key: max(0, admission_end[item] - admission_committed.get(item, 0))
                        for item in partitions
                    }
                admission = {
                    "source_lag_bytes": before.lag_bytes,
                    "sink_lag_records": sum(sink_lag.values()),
                    "sink_lag_by_partition": sink_lag,
                    "connector_state": expected_state,
                    "nonzero_lag_required": resume_paused or require_nonzero_lag,
                }
                if production_shaped and not production_admission_ready(
                    before.lag_bytes,
                    sink_lag,
                    int(self.scenario_metadata["max_source_lag_bytes"]),
                    int(self.scenario_metadata["max_sink_lag_records_per_partition"]),
                ):
                    raise RuntimeError("production-shaped lag crossed its ceiling before t1")
                if healthy_overload:
                    admission["writer_inserted_at_t1"] = inserted_at_t1
                    admission["writer_active_at_t1"] = True
                    admission["source_lag_records_by_partition"] = dict(source_lag_records)
                if (resume_paused or require_nonzero_lag) and (
                    before.lag_bytes <= 0 or admission["sink_lag_records"] <= 0
                ):
                    profile = "paused-backlog" if resume_paused else "healthy-overload"
                    raise RuntimeError(f"{profile} scenario requires non-zero source and sink lag at admission")
                self.mark("t1", **admission)
                if self.active_writer_inserted_snapshot is not None and self.active_writer_is_alive is not None:
                    if not self.active_writer_is_alive():
                        raise RuntimeError("active-timeslot writer is not running at t1")
                    workload_by_timeslot["t1"] = {
                        "active": self.active_writer_inserted_snapshot(),
                        "retiring": None if self.writer_inserted_snapshot is None else self.writer_inserted_snapshot(),
                    }

                if self.writer_is_alive is not None and not self.writer_is_alive():
                    raise RuntimeError("healthy-overload writer stopped between t1 and ownership lock")

                with warm.transaction():
                    attempt_epoch = warm.execute(
                        """
                        INSERT INTO public.flip_attempts
                            (attempt_id, cell, timeslot, state, table_count, slot_name, publication_name, manifest, manifest_sha256, connector_config_sha256)
                        VALUES (%s, %s, %s, 'locked', %s, %s, %s, %s, %s, %s)
                        RETURNING attempt_epoch
                        """,
                        (
                            attempt_id,
                            self.settings.cell,
                            self.settings.timeslot,
                            self.settings.table_count,
                            self.settings.slot_name,
                            self.settings.publication_name,
                            Jsonb(json.loads(manifest_json)),
                            hashlib.sha256(manifest_json.encode("utf-8")).hexdigest(),
                            connector_config_sha256,
                        ),
                    ).fetchone()[0]
                    tracker_updated = warm.execute(
                        """
                        UPDATE public.partition_tracker
                        SET state='locked', attempt_epoch=%s, version=version+1, updated_at=clock_timestamp()
                        WHERE cell=%s AND timeslot=%s AND state='hot_primary' AND attempt_epoch IS NULL
                        """,
                        (attempt_epoch, self.settings.cell, self.settings.timeslot),
                    ).rowcount
                    if tracker_updated != 1:
                        raise RuntimeError("ownership CAS hot_primary -> locked failed")
                    with warm.cursor() as cursor:
                        cursor.executemany(
                            """
                            INSERT INTO public.flip_table_states (attempt_epoch, parent_name, leaf_name, state)
                            VALUES (%s, %s, %s, 'attached')
                            """,
                            ((attempt_epoch, route.parent, route.leaf) for route in self.manifest.tables),
                        )
                self._attempt_epoch = attempt_epoch
                transition(AttemptState.HOT_PRIMARY, AttemptState.LOCKED)
                self.mark("t2", attempt_epoch=self._attempt_epoch)
                park_deadline = time.monotonic() + self.timeout_seconds
                self._acquire_session_lock(warm, park_deadline)
                self._acquire_session_lock(hot, park_deadline)
                if self.writer_quiesce is not None:
                    workload_inserted_total = self.writer_quiesce(self._remaining(park_deadline))
                active_at_t2q = None
                if self.active_writer_inserted_snapshot is not None and self.active_writer_is_alive is not None:
                    if not self.active_writer_is_alive():
                        raise RuntimeError("active-timeslot writer stopped while retiring ownership locked")
                    active_at_t2q = self.active_writer_inserted_snapshot()
                    workload_by_timeslot["t2q"] = {
                        "active": active_at_t2q,
                        "retiring": workload_inserted_total,
                    }
                self.mark(
                    "t2q",
                    guarded_writer_quiesced=self.writer_quiesce is not None,
                    workload_inserted_total=workload_inserted_total,
                    active_workload_inserted=active_at_t2q,
                )

                if resume_paused:
                    source.set_paused(self.settings.source_connector, False)
                    sink.set_paused(self.manifest.tables[0].sink_connector, False)

                for index, route in enumerate(self.manifest.tables, start=1):
                    remaining_ms = max(1, int(self._remaining(park_deadline) * 1000))
                    hot.execute("SELECT set_config('statement_timeout', %s, false)", (f"{remaining_ms}ms",))
                    hot.execute("SELECT set_config('lock_timeout', %s, false)", (f"{min(5000, remaining_ms)}ms",))
                    start_ns = time.perf_counter_ns()
                    self.mark(f"t3_{index}", table=route.parent)
                    updated = warm.execute(
                        """
                        UPDATE public.flip_table_states
                        SET state='detach_started', detach_started_at=clock_timestamp()
                        WHERE attempt_epoch=%s AND parent_name=%s AND state='attached'
                        """,
                        (self._attempt_epoch, route.parent),
                    ).rowcount
                    if updated != 1:
                        raise RuntimeError(f"table-state CAS failed before detaching {route.parent}")
                    try:
                        hot.execute(
                            sql.SQL("ALTER TABLE {} DETACH PARTITION {} CONCURRENTLY").format(
                                sql.Identifier(route.parent), sql.Identifier(route.leaf)
                            )
                        )
                    except Exception:
                        pending = hot.execute(
                            """
                            SELECT i.inhdetachpending
                            FROM pg_inherits i
                            WHERE i.inhparent=%s::regclass AND i.inhrelid=%s::regclass
                            """,
                            (f"public.{route.parent}", f"public.{route.leaf}"),
                        ).fetchone()
                        state = "detached" if pending is None else ("pending_finalize" if pending[0] else "detach_started")
                        warm.execute(
                            "UPDATE public.flip_table_states SET state=%s WHERE attempt_epoch=%s AND parent_name=%s",
                            (state, self._attempt_epoch, route.parent),
                        )
                        raise
                    duration = time.perf_counter_ns() - start_ns
                    self.detach_ns[route.parent] = duration
                    updated = warm.execute(
                        """
                        UPDATE public.flip_table_states
                        SET state='detached', detach_finished_at=clock_timestamp(), detach_duration_ns=%s
                        WHERE attempt_epoch=%s AND parent_name=%s AND state='detach_started'
                        """,
                        (duration, self._attempt_epoch, route.parent),
                    ).rowcount
                    if updated != 1:
                        raise RuntimeError(f"table-state CAS failed after detaching {route.parent}")
                    self.mark(f"t4_{index}", table=route.parent, duration_ns=duration)

                hot_source, fence_lsn = hot_identity(hot, self.settings.cell, self.settings.slot_name)
                self.mark("t5", fence_lsn=fence_lsn, hot_system_identifier=hot_source.system_identifier)
                updated = warm.execute(
                    """
                    UPDATE public.flip_attempts
                    SET hot_system_identifier=%s, hot_database=%s, fence_lsn=%s::pg_lsn, updated_at=clock_timestamp()
                    WHERE attempt_epoch=%s AND state='locked'
                    """,
                    (hot_source.system_identifier, hot_source.database, fence_lsn, self._attempt_epoch),
                ).rowcount
                if updated != 1:
                    raise RuntimeError("fence persistence CAS failed")
                self.mark("t6")

                confirmed = wait_slot_lsn(
                    hot,
                    self.settings.cell,
                    self.settings.slot_name,
                    fence_lsn,
                    self._remaining(park_deadline),
                    self.poll_seconds,
                )
                if not source_fence_satisfied(hot_source, confirmed.identity, confirmed.confirmed_lsn, fence_lsn):
                    raise RuntimeError("source fence predicate unexpectedly remained false")
                self.mark("t7", confirmed_flush_lsn=confirmed.confirmed_lsn)

                target_values = kafka.end_offsets(partitions)
                target = OffsetVector(
                    self.settings.cell,
                    self.settings.timeslot,
                    self._attempt_epoch,
                    target_values,
                )
                self.mark("t8", target_offsets={item.key: target.values[item] for item in partitions})
                with warm.transaction():
                    with warm.cursor() as cursor:
                        cursor.executemany(
                            """
                            INSERT INTO public.flip_offsets
                                (attempt_epoch, consumer_group, topic, partition_id, target_next_offset)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                (
                                    self._attempt_epoch,
                                    route.sink_group,
                                    route.topic,
                                    route.partition,
                                    target.values[TopicPartition(route.topic, route.partition)],
                                )
                                for route in self.manifest.tables
                            )
                        )
                    stored_count = warm.execute(
                        "SELECT count(*) FROM public.flip_offsets WHERE attempt_epoch=%s",
                        (self._attempt_epoch,),
                    ).fetchone()[0]
                    if stored_count != self.settings.table_count:
                        raise RuntimeError("target offset vector was not persisted atomically")
                self.mark("t9")

                while time.monotonic() < park_deadline:
                    observed_values = kafka.committed_offsets(
                        group_by_partition,
                        min(10.0, self._remaining(park_deadline)),
                    )
                    self._remaining(park_deadline)
                    current_values = {
                        partition: observed_values.get(partition, 0)
                        for partition in partitions
                        if partition in observed_values or target.values[partition] == 0
                    }
                    current = OffsetVector(
                        self.settings.cell,
                        self.settings.timeslot,
                        self._attempt_epoch,
                        current_values,
                    )
                    gate = offset_gate(partitions, target, current)
                    if gate.ready:
                        self.mark("t10", warm_commit_inferred_from_sink_offset_contract=True)
                        self.mark("t11", committed_offsets={item.key: current.values[item] for item in partitions})
                        break
                    time.sleep(self.poll_seconds)
                else:
                    raise TimeoutError(f"sink offset vector did not reach target before the writer-park deadline")

                for partition in partitions:
                    updated = warm.execute(
                        """
                        UPDATE public.flip_offsets
                        SET final_committed_next_offset=%s
                        WHERE attempt_epoch=%s AND topic=%s AND partition_id=%s
                        """,
                        (current_values[partition], self._attempt_epoch, partition.topic, partition.partition),
                    ).rowcount
                    if updated != 1:
                        raise RuntimeError(f"final offset persistence failed for {partition.key}")

                detached_count = warm.execute(
                    "SELECT count(*) FROM public.flip_table_states WHERE attempt_epoch=%s AND state='detached'",
                    (self._attempt_epoch,),
                ).fetchone()[0]
                catalog_detached = all(
                    hot.execute(
                        """
                        SELECT NOT EXISTS (
                            SELECT 1 FROM pg_inherits
                            WHERE inhparent=%s::regclass AND inhrelid=%s::regclass
                        )
                        """,
                        (f"public.{route.parent}", f"public.{route.leaf}"),
                    ).fetchone()[0]
                    for route in self.manifest.tables
                )
                stored_rows = warm.execute(
                    """
                    SELECT topic, partition_id, target_next_offset
                    FROM public.flip_offsets
                    WHERE attempt_epoch=%s
                    ORDER BY topic, partition_id
                    """,
                    (self._attempt_epoch,),
                ).fetchall()
                stored_target = {TopicPartition(row[0], row[1]): row[2] for row in stored_rows}
                confirmed_final = slot_status(hot, self.settings.cell, self.settings.slot_name)
                source_ready = source_fence_satisfied(
                    hot_source,
                    confirmed_final.identity,
                    confirmed_final.confirmed_lsn,
                    fence_lsn,
                )
                final_observed_values = kafka.committed_offsets(
                    group_by_partition,
                    min(10.0, self._remaining(park_deadline)),
                )
                final_current_values = {
                    partition: final_observed_values.get(partition, 0)
                    for partition in partitions
                    if partition in final_observed_values or target.values[partition] == 0
                }
                final_current = OffsetVector(
                    self.settings.cell,
                    self.settings.timeslot,
                    self._attempt_epoch,
                    final_current_values,
                )
                final_gate = offset_gate(partitions, target, final_current)
                current_values = final_current_values
                evidence = GateEvidence(
                    detached_count == self.settings.table_count and catalog_detached,
                    source_ready,
                    stored_target == dict(target.values),
                    final_gate.ready,
                )
                self._remaining(park_deadline)
                transition(AttemptState.LOCKED, AttemptState.DRAINED, evidence)
                with warm.transaction():
                    attempt_updated = warm.execute(
                        "UPDATE public.flip_attempts SET state='drained', updated_at=clock_timestamp() WHERE attempt_epoch=%s AND state='locked'",
                        (self._attempt_epoch,),
                    ).rowcount
                    tracker_updated = warm.execute(
                        """
                        UPDATE public.partition_tracker
                        SET state='drained', version=version+1, updated_at=clock_timestamp()
                        WHERE cell=%s AND timeslot=%s AND state='locked' AND attempt_epoch=%s
                        """,
                        (self.settings.cell, self.settings.timeslot, self._attempt_epoch),
                    ).rowcount
                    if attempt_updated != 1 or tracker_updated != 1:
                        raise RuntimeError("attempt/tracker CAS failed while entering drained")
                self.mark("t12")
                transition(AttemptState.DRAINED, AttemptState.WARM_PRIMARY)
                with warm.transaction():
                    attempt_updated = warm.execute(
                        "UPDATE public.flip_attempts SET state='warm_primary', updated_at=clock_timestamp() WHERE attempt_epoch=%s AND state='drained'",
                        (self._attempt_epoch,),
                    ).rowcount
                    tracker_updated = warm.execute(
                        """
                        UPDATE public.partition_tracker
                        SET state='warm_primary', version=version+1, updated_at=clock_timestamp()
                        WHERE cell=%s AND timeslot=%s AND state='drained' AND attempt_epoch=%s
                        """,
                        (self.settings.cell, self.settings.timeslot, self._attempt_epoch),
                    ).rowcount
                    if attempt_updated != 1 or tracker_updated != 1:
                        raise RuntimeError("attempt/tracker CAS failed while granting warm_primary")
                ownership_granted = True
                outcome = "success"
                self.mark("t13")
                try:
                    released_hot = self._release_session_lock(hot)
                except Exception:
                    released_hot = False
                try:
                    released_warm = self._release_session_lock(warm)
                except Exception:
                    released_warm = False
                if not released_hot or not released_warm:
                    self.events.append(
                        {
                            "stage": "lifecycle_unlock_warning",
                            "monotonic_ns": time.perf_counter_ns() - self._base_ns,
                            "hot_released": released_hot,
                            "warm_released": released_warm,
                        }
                    )
                topology_label_at_grant = (
                    "local-rf1"
                    if self.settings.kafka_topic_replication_factor == 1
                    else f"local-rf{self.settings.kafka_topic_replication_factor}-single-host"
                )
                if resume_paused:
                    profile_at_grant = f"{topology_label_at_grant}-paused-backlog"
                elif self.scenario_metadata.get("mode") == "healthy-overload":
                    profile_at_grant = f"{topology_label_at_grant}-healthy-overload"
                elif self.scenario_metadata.get("mode") == "production-shaped":
                    profile_at_grant = f"{topology_label_at_grant}-production-shaped"
                else:
                    profile_at_grant = f"{topology_label_at_grant}-running"
                checkpoint = {
                    "schema_version": 1,
                    "artifact_type": "ownership_grant",
                    "non_authoritative": True,
                    "run_id": str(self.run_id),
                    "attempt_id": str(attempt_id),
                    "attempt_epoch": self._attempt_epoch,
                    "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                    "cell": self.settings.cell,
                    "timeslot": self.settings.timeslot,
                    "table_count": self.settings.table_count,
                    "profile": profile_at_grant,
                    "environment_generation_id": self.scenario_metadata.get(
                        "environment_generation_id"
                    ) or "legacy",
                    "ownership_outcome": "success",
                    "verification_outcome": "pending",
                    "fence_lsn": fence_lsn,
                    "stages_ns": dict(self.timestamps),
                    "durations_ns": dict(derive_stage_durations(self.timestamps)),
                    "detach_ns_by_table": dict(self.detach_ns),
                    "admission": admission,
                    "target_next_offsets": {
                        item.key: value for item, value in target_values.items()
                    },
                    "final_committed_next_offsets": {
                        item.key: value for item, value in current_values.items()
                    },
                }
                try:
                    write_ownership_checkpoint_atomic(
                        result_path.parent / "ownership-grant.json", checkpoint
                    )
                except Exception as checkpoint_error:
                    self.events.append(
                        {
                            "stage": "ownership_checkpoint_failed",
                            "monotonic_ns": time.perf_counter_ns() - self._base_ns,
                            "error": f"{type(checkpoint_error).__name__}: {checkpoint_error}",
                        }
                    )
                else:
                    self.events.append(
                        {
                            "stage": "ownership_checkpoint_saved",
                            "monotonic_ns": time.perf_counter_ns() - self._base_ns,
                            "path": "ownership-grant.json",
                        }
                    )
                if self.active_writer_inserted_snapshot is not None and self.active_writer_is_alive is not None:
                    try:
                        workload_by_timeslot["t13"] = {
                            "active": self.active_writer_inserted_snapshot(),
                            "retiring": workload_inserted_total,
                            "writer_alive": self.active_writer_is_alive(),
                        }
                    except Exception as snapshot_error:
                        self.events.append({
                            "stage": "post_grant_snapshot_error",
                            "monotonic_ns": time.perf_counter_ns() - self._base_ns,
                            "error": f"{type(snapshot_error).__name__}: {snapshot_error}",
                        })
                validation_timeout_ms = max(1, int(self.timeout_seconds * 1000))
                validation_started_ns = time.perf_counter_ns()
                try:
                    hot.execute("SELECT set_config('statement_timeout', %s, false)", (f"{validation_timeout_ms}ms",))
                    warm.execute("SELECT set_config('statement_timeout', %s, false)", (f"{validation_timeout_ms}ms",))
                    parity, parity_rows = parity_for_run(hot, warm, self.manifest, self.run_id)
                    verification_outcome = "passed" if parity else "failed"
                except Exception as validation_error:
                    verification_outcome = "failed"
                    verification_error = {
                        "type": type(validation_error).__name__,
                        "message": str(validation_error),
                    }
                self.mark(
                    "tverify",
                    parity=parity_rows,
                    verification_outcome=verification_outcome,
                    validation_duration_ns=time.perf_counter_ns() - validation_started_ns,
                    verification_error=verification_error,
                )
            except Exception as error:
                if ownership_granted:
                    outcome = "success"
                    verification_outcome = "failed"
                    verification_error = {
                        "type": type(error).__name__,
                        "message": f"post-grant bookkeeping failed: {error}",
                    }
                    self.events.append({
                        "stage": "post_grant_error",
                        "monotonic_ns": time.perf_counter_ns() - self._base_ns,
                        "error": verification_error,
                    })
                else:
                    error_payload = {"type": type(error).__name__, "message": str(error)}
                if self._attempt_epoch is not None and not ownership_granted:
                    recovery_deadline = time.monotonic() + self.recovery_timeout_seconds
                    try:
                        remaining_ms = max(1, int(self._remaining(recovery_deadline) * 1000))
                        warm.execute(
                            "SELECT set_config('statement_timeout', %s, false)",
                            (f"{remaining_ms}ms",),
                        )
                        with warm.transaction():
                            remaining_ms = max(1, int(self._remaining(recovery_deadline) * 1000))
                            warm.execute(
                                "SELECT set_config('statement_timeout', %s, false)",
                                (f"{remaining_ms}ms",),
                            )
                            warm.execute(
                                """
                                UPDATE public.flip_attempts
                                SET state='recovering', error=%s, stage_timestamps=%s, updated_at=clock_timestamp()
                                WHERE attempt_epoch=%s AND state IN ('locked', 'drained')
                                """,
                                (Jsonb(error_payload), Jsonb(self.timestamps), self._attempt_epoch),
                            )
                            remaining_ms = max(1, int(self._remaining(recovery_deadline) * 1000))
                            warm.execute(
                                "SELECT set_config('statement_timeout', %s, false)",
                                (f"{remaining_ms}ms",),
                            )
                            warm.execute(
                                """
                                UPDATE public.partition_tracker
                                SET state='recovering', version=version+1, updated_at=clock_timestamp()
                                WHERE cell=%s AND timeslot=%s AND attempt_epoch=%s AND state IN ('locked', 'drained')
                                """,
                                (self.settings.cell, self.settings.timeslot, self._attempt_epoch),
                            )
                            remaining_ms = max(1, int(self._remaining(recovery_deadline) * 1000))
                            warm.execute(
                                "SELECT set_config('statement_timeout', %s, false)",
                                (f"{remaining_ms}ms",),
                            )
                    except Exception as recording_error:
                        error_payload = {
                            **error_payload,
                            "recovery_recording_error": f"{type(recording_error).__name__}: {recording_error}",
                        }
                    else:
                        self.mark("trevert_start", original_error=error_payload)
                        try:
                            recovered = revert_to_hot(
                                hot,
                                warm,
                                self.manifest,
                                self._attempt_epoch,
                                timeout_seconds=max(0.001, recovery_deadline - time.monotonic()),
                            )
                        except Exception as revert_error:
                            error_payload = {
                                **error_payload,
                                "revert_error": f"{type(revert_error).__name__}: {revert_error}",
                            }
                        else:
                            self._remaining(recovery_deadline)
                            self._release_session_lock(hot)
                            self._remaining(recovery_deadline)
                            self._release_session_lock(warm)
                            self._remaining(recovery_deadline)
                            self.mark("trevert_end", recovered_tables=recovered)
                            outcome = "reverted"
            finally:
                if kafka is not None:
                    try:
                        kafka.close()
                    except Exception as close_error:
                        self.events.append({
                            "stage": "kafka_observer_close_error",
                            "monotonic_ns": time.perf_counter_ns() - self._base_ns,
                            "error": f"{type(close_error).__name__}: {close_error}",
                        })
                if self._attempt_epoch is not None:
                    try:
                        warm.execute(
                            "UPDATE public.flip_attempts SET stage_timestamps=%s, updated_at=clock_timestamp() WHERE attempt_epoch=%s",
                            (Jsonb(self.timestamps), self._attempt_epoch),
                        )
                    except Exception as bookkeeping_error:
                        self.events.append({
                            "stage": "final_bookkeeping_error",
                            "monotonic_ns": time.perf_counter_ns() - self._base_ns,
                            "error": f"{type(bookkeeping_error).__name__}: {bookkeeping_error}",
                        })

        if outcome == "success":
            durations = derive_stage_durations(self.timestamps)
        elif outcome == "reverted":
            durations = derive_revert_durations(self.timestamps)
        else:
            durations = {}
        topology_label = (
            "local-rf1"
            if self.settings.kafka_topic_replication_factor == 1
            else f"local-rf{self.settings.kafka_topic_replication_factor}-single-host"
        )
        if resume_paused:
            profile = f"{topology_label}-paused-backlog"
        elif self.scenario_metadata.get("mode") == "healthy-overload":
            profile = f"{topology_label}-healthy-overload"
        elif self.scenario_metadata.get("mode") == "production-shaped":
            profile = f"{topology_label}-production-shaped"
        else:
            profile = f"{topology_label}-running"
        payload: dict[str, Any] = {
            "schema_version": 1,
            "run_id": str(self.run_id),
            "attempt_id": str(attempt_id),
            "attempt_epoch": self._attempt_epoch,
            "started_at_utc": started.isoformat(),
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "outcome": outcome,
            "profile": profile,
            "topology": {
                "single_host": True,
                "kafka_topic_replication_factor": self.settings.kafka_topic_replication_factor,
                "kafka_min_insync_replicas": self.settings.kafka_min_insync_replicas,
            },
            "declared_versions": {
                "postgresql_image": "postgres:17.10-bookworm",
                "kafka_image": "apache/kafka:4.3.1",
                "debezium_connect_image": "quay.io/debezium/connect:3.6.0.Final",
                "runner_python": "3.13.5",
            },
            "connect_worker_config": {
                "distributed_group": {
                    "offset_flush_interval_ms": self.settings.connect_offset_flush_interval_ms,
                    "offset_flush_timeout_ms": self.settings.connect_offset_flush_timeout_ms,
                    "scheduled_rebalance_max_delay_ms": self.settings.connect_scheduled_rebalance_max_delay_ms,
                    "session_timeout_ms": self.settings.connect_session_timeout_ms,
                    "heartbeat_interval_ms": self.settings.connect_heartbeat_interval_ms,
                },
                "worker_heap_opts": self.settings.connect_worker_heap_opts,
                "internal_topic_replication_factor": self.settings.connect_internal_topic_replication_factor,
            },
            "local_resource_config": {
                "kafka_broker_heap_opts": self.settings.kafka_broker_heap_opts,
            },
            "manifest_sha256": hashlib.sha256(manifest_json.encode("utf-8")).hexdigest(),
            "connector_config_sha256": connector_config_sha256,
            "table_count": self.settings.table_count,
            "scenario": self.scenario_metadata,
            "workload_inserted_total": workload_inserted_total,
            "workload_by_timeslot": workload_by_timeslot,
            "admission": admission,
            "hot_identity": None if hot_source is None else asdict(hot_source),
            "fence_lsn": fence_lsn,
            "target_next_offsets": {item.key: value for item, value in target_values.items()},
            "final_committed_next_offsets": {item.key: value for item, value in current_values.items()},
            "stages_ns": self.timestamps,
            "durations_ns": dict(durations),
            "detach_ns_by_table": self.detach_ns,
            "parity": parity_rows,
            "run_row_count": sum(int(row["hot"][0]) for row in parity_rows) if outcome == "success" else 0,
            "validation_ns": durations.get("validation_ns", 0),
            "events": self.events,
            "error": error_payload,
            "ownership_outcome": outcome,
            "verification_outcome": verification_outcome,
            "verification_error": verification_error,
            "gc_eligible": outcome == "success" and verification_outcome == "passed",
        }
        write_json_atomic(result_path, payload)
        if outcome != "success":
            disposition = "reverted safely" if outcome == "reverted" else "failed closed"
            raise RuntimeError(f"flip {disposition}; result={result_path}; error={error_payload}")
        return payload

    @staticmethod
    def _verify_live_config(
        label: str,
        live: Mapping[str, str],
        expected: Mapping[str, str],
    ) -> None:
        mismatched = tuple(key for key, value in expected.items() if live.get(key) != value)
        if mismatched:
            raise RuntimeError(f"{label} connector config drift in keys: {', '.join(sorted(mismatched))}")
