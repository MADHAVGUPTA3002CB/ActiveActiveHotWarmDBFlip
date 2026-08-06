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
from .connector_configs import (
    SourceConnectorSpec,
    fence_source_spec,
    shared_sink_config,
    source_specs,
)
from .core import (
    AttemptState,
    FenceWakeupMode,
    GateEvidence,
    HotSourceIdentity,
    LeafFenceMarker,
    OffsetVector,
    OptimisticAdmissionCheckMode,
    SourceProofMode,
    TimingError,
    TopicPartition,
    WriteFenceMode,
    build_leaf_fence_markers,
    build_manifest,
    canonical_manifest_json,
    derive_revert_durations,
    derive_stage_durations,
    overload_lag_vectors,
    offset_gate,
    production_admission_ready,
    source_fence_satisfied,
    state_only_batch_admission_supported,
    transition,
)
from .kafka_io import KafkaControl
from .lifecycle import lifecycle_lock_name
from .parallel_detach import ParallelDetachError, run_all_parallel
from .postgres_io import (
    atomic_detach_and_emit_leaf_fence_marker,
    connect,
    current_source_wal_flush_lsn,
    bind_hot_write_gate_attempt,
    hot_identity,
    hot_write_gate_status,
    emit_leaf_fence_markers,
    observed_leaf_fence_receipts,
    parity_for_run,
    park_hot_write_gate,
    reopen_hot_write_gate,
    slot_status,
    trigger_source_heartbeat,
    verify_source_publication,
    wait_slot_lsn,
)
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
        writer_fence: Callable[[float], int] | None = None,
        recovery_timeout_seconds: float = 30.0,
        fence_wakeup_mode: FenceWakeupMode | str = FenceWakeupMode.PASSIVE,
        source_proof_mode: SourceProofMode | str = SourceProofMode.SLOT_LSN,
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
        try:
            selected_wakeup_mode = FenceWakeupMode(fence_wakeup_mode)
        except ValueError as error:
            raise ValueError(
                "fence_wakeup_mode must be passive or immediate_heartbeat"
            ) from error
        supplied_mode = (scenario_metadata or {}).get("fence_wakeup_mode")
        if supplied_mode is not None and supplied_mode != selected_wakeup_mode.value:
            raise ValueError("scenario fence_wakeup_mode contradicts the selected mode")
        self.fence_wakeup_mode = selected_wakeup_mode
        try:
            selected_proof_mode = SourceProofMode(source_proof_mode)
        except ValueError as error:
            raise ValueError("unknown source_proof_mode") from error
        supplied_proof_mode = (scenario_metadata or {}).get("source_proof_mode")
        if supplied_proof_mode is not None and supplied_proof_mode != selected_proof_mode.value:
            raise ValueError("scenario source_proof_mode contradicts the selected mode")
        self.source_proof_mode = selected_proof_mode
        fence_source = fence_source_spec(settings, self.manifest)
        self._fence_wakeup_evidence: Mapping[str, Any] = {
            "mode": selected_wakeup_mode.value,
            "lane": fence_source.lane,
            "heartbeat_table": fence_source.heartbeat_table,
            "attempted": False,
            "applied": False,
            "rows_updated": 0,
            "post_update_wal_lsn": None,
            "duration_ns": 0,
            "confirmed_flush_lsn_at_t7": None,
        }
        self.scenario_metadata = {
            **dict(scenario_metadata or {}),
            "fence_wakeup_mode": selected_wakeup_mode.value,
            "source_proof_mode": selected_proof_mode.value,
        }
        try:
            self.write_fence_mode = WriteFenceMode(
                self.scenario_metadata.get(
                    "write_fence_mode", WriteFenceMode.WARM_TRACKER_ADVISORY.value
                )
            )
        except ValueError as error:
            raise ValueError("unknown write_fence_mode") from error
        try:
            self.optimistic_admission_check_mode = OptimisticAdmissionCheckMode(
                self.scenario_metadata.get(
                    "optimistic_admission_check_mode",
                    OptimisticAdmissionCheckMode.STATE_AND_EPOCH.value,
                )
            )
        except ValueError as error:
            raise ValueError("unknown optimistic admission check mode") from error
        admitted_gate_epoch = self.scenario_metadata.get("retiring_write_gate_epoch")
        if self.write_fence_mode in (
            WriteFenceMode.HOT_TRANSACTIONAL,
            WriteFenceMode.OPTIMISTIC_DETACH,
        ) and (
            not isinstance(admitted_gate_epoch, int)
            or isinstance(admitted_gate_epoch, bool)
            or admitted_gate_epoch <= 0
        ):
            raise ValueError(
                f"{self.write_fence_mode.value} requires the retiring workload's admitted gate epoch"
            )
        self.writer_fence = writer_fence
        self.writer_quiesce = writer_quiesce
        self.writer_inserted_snapshot = writer_inserted_snapshot
        self.writer_is_alive = writer_is_alive
        self.active_writer_inserted_snapshot = active_writer_inserted_snapshot
        self.active_writer_is_alive = active_writer_is_alive

        if selected_proof_mode in (
            SourceProofMode.PER_LEAF_MARKER,
            SourceProofMode.ATOMIC_DETACH_MARKER,
            SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
        ):
            allowed_topologies = (
                ("isolated", "shared")
                if selected_proof_mode
                is SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER
                else ("isolated",)
            )
            if (
                settings.source_topology not in allowed_topologies
                or self.write_fence_mode is not WriteFenceMode.OPTIMISTIC_DETACH
                or selected_wakeup_mode is not FenceWakeupMode.PASSIVE
            ):
                raise ValueError(
                    "marker source proof requires optimistic detach and passive heartbeat"
                    " mode; F and G require isolated sources, and only the parallel"
                    " marker variant (H-Prod) may use the shared source"
                )
        state_only_contract = state_only_batch_admission_supported(
            settings.source_topology,
            selected_wakeup_mode,
            self.write_fence_mode,
            selected_proof_mode,
        )
        if (
            state_only_contract
            and self.optimistic_admission_check_mode
            is not OptimisticAdmissionCheckMode.STATE_ONLY
        ):
            raise ValueError(
                "Variant H and revised Variant A require state-only API batch admission"
            )
        if (
            not state_only_contract
            and self.optimistic_admission_check_mode
            is OptimisticAdmissionCheckMode.STATE_ONLY
        ):
            raise ValueError(
                "state-only API batch admission is reserved for Variant H or revised Variant A"
            )

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

    def _set_statement_timeout(self, connection: Any, deadline: float) -> None:
        remaining_ms = max(1, int(self._remaining(deadline) * 1000))
        connection.execute(
            "SELECT set_config('statement_timeout', %s, false)",
            (f"{remaining_ms}ms",),
        )

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

    def _source_lane_snapshot(
        self,
        source: ConnectClient,
        hot: Any,
        configured_sources: tuple[SourceConnectorSpec, ...],
        expected_states: Mapping[str, str],
        deadline: float | None = None,
    ) -> dict[str, Any]:
        evidence: dict[str, Any] = {}
        for spec in configured_sources:
            payload = source.status(
                spec.connector_name,
                timeout_seconds=None if deadline is None else self._remaining(deadline),
            )
            connector_state = str(payload.get("connector", {}).get("state", ""))
            task_states = tuple(str(task.get("state", "")) for task in payload.get("tasks", ()))
            expected_state = expected_states[spec.lane]
            if (
                connector_state != expected_state
                or not task_states
                or any(state != expected_state for state in task_states)
            ):
                raise RuntimeError(
                    f"source lane {spec.lane!r} is not {expected_state}: "
                    f"connector={connector_state!r}, tasks={task_states!r}"
                )
            status = slot_status(hot, self.settings.cell, spec.slot_name)
            if expected_state == "RUNNING" and not status.active:
                raise RuntimeError(f"source lane {spec.lane!r} logical slot is not active")
            evidence[spec.lane] = {
                "connector_name": spec.connector_name,
                "connector_state": connector_state,
                "connector_worker_id": str(payload.get("connector", {}).get("worker_id", "")),
                "task_states": list(task_states),
                "task_worker_ids": [str(task.get("worker_id", "")) for task in payload.get("tasks", ())],
                "slot_name": spec.slot_name,
                "publication_name": spec.publication_name,
                "topic_prefix": spec.topic_prefix,
                "slot_active": status.active,
                "confirmed_lsn": status.confirmed_lsn,
                "restart_lsn": status.restart_lsn,
                "lag_bytes": status.lag_bytes,
            }
        return evidence

    @staticmethod
    def _connector_worker_snapshot(
        client: ConnectClient,
        connector_name: str,
        expected_state: str,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        payload = client.status(connector_name, timeout_seconds=timeout_seconds)
        connector_state = str(payload.get("connector", {}).get("state", ""))
        tasks = tuple(payload.get("tasks", ()))
        task_states = tuple(str(task.get("state", "")) for task in tasks)
        if (
            connector_state != expected_state
            or not task_states
            or any(state != expected_state for state in task_states)
        ):
            raise RuntimeError(
                f"connector {connector_name!r} is not {expected_state}: "
                f"connector={connector_state!r}, tasks={task_states!r}"
            )
        return {
            "connector_name": connector_name,
            "connector_state": connector_state,
            "connector_worker_id": str(payload.get("connector", {}).get("worker_id", "")),
            "task_states": list(task_states),
            "task_worker_ids": [str(task.get("worker_id", "")) for task in tasks],
        }

    def _resume_paused_connectors(
        self,
        source: ConnectClient,
        sink: ConnectClient,
        fence_source: SourceConnectorSpec,
        sink_connector: str,
        park_deadline: float,
    ) -> None:
        source.set_paused(
            fence_source.connector_name,
            False,
            timeout_seconds=self._remaining(park_deadline),
        )
        sink.set_paused(
            sink_connector,
            False,
            timeout_seconds=self._remaining(park_deadline),
        )
        source.wait_state(
            fence_source.connector_name,
            "RUNNING",
            self._remaining(park_deadline),
        )
        sink.wait_state(
            sink_connector,
            "RUNNING",
            self._remaining(park_deadline),
        )
        self.events.append(
            {
                "stage": "paused_connectors_resumed",
                "monotonic_ns": time.perf_counter_ns() - self._base_ns,
            }
        )

    def _capture_and_confirm_source_fence(
        self,
        hot: Any,
        warm: Any,
        fence_source: SourceConnectorSpec,
        park_deadline: float,
    ) -> tuple[HotSourceIdentity, str, Any, Mapping[str, Any]]:
        hot_source, fence_lsn = hot_identity(
            hot, self.settings.cell, fence_source.slot_name
        )
        self.mark(
            "t5",
            fence_lsn=fence_lsn,
            hot_system_identifier=hot_source.system_identifier,
        )
        updated = warm.execute(
            """
            UPDATE public.flip_attempts
            SET hot_system_identifier=%s, hot_database=%s, fence_lsn=%s::pg_lsn, updated_at=clock_timestamp()
            WHERE attempt_epoch=%s AND state='locked'
            """,
            (
                hot_source.system_identifier,
                hot_source.database,
                fence_lsn,
                self._attempt_epoch,
            ),
        ).rowcount
        if updated != 1:
            raise RuntimeError("fence persistence CAS failed")
        self.mark("t6")

        wakeup_started_ns = time.perf_counter_ns()
        if self.fence_wakeup_mode is FenceWakeupMode.IMMEDIATE_HEARTBEAT:
            self._set_statement_timeout(hot, park_deadline)
            self._fence_wakeup_evidence = {
                "mode": self.fence_wakeup_mode.value,
                "lane": fence_source.lane,
                "heartbeat_table": fence_source.heartbeat_table,
                "attempted": True,
                "applied": False,
                "rows_updated": 0,
                "post_update_wal_lsn": None,
                "duration_ns": 0,
                "confirmed_flush_lsn_at_t7": None,
            }
            try:
                rows_updated = trigger_source_heartbeat(hot, fence_source)
                self._fence_wakeup_evidence = {
                    **self._fence_wakeup_evidence,
                    "applied": True,
                    "rows_updated": rows_updated,
                }
                self._set_statement_timeout(hot, park_deadline)
                post_update_wal_lsn = current_source_wal_flush_lsn(hot)
                self._fence_wakeup_evidence = {
                    **self._fence_wakeup_evidence,
                    "post_update_wal_lsn": post_update_wal_lsn,
                }
            finally:
                self._fence_wakeup_evidence = {
                    **self._fence_wakeup_evidence,
                    "duration_ns": time.perf_counter_ns() - wakeup_started_ns,
                }
            wakeup = dict(self._fence_wakeup_evidence)
            wakeup.pop("confirmed_flush_lsn_at_t7")
        else:
            wakeup = {
                "mode": self.fence_wakeup_mode.value,
                "lane": fence_source.lane,
                "heartbeat_table": fence_source.heartbeat_table,
                "attempted": False,
                "applied": False,
                "rows_updated": 0,
                "post_update_wal_lsn": None,
                "duration_ns": 0,
            }
        self._fence_wakeup_evidence = {
            **wakeup,
            "confirmed_flush_lsn_at_t7": None,
        }
        self.mark("t6w", **wakeup)

        confirmed = wait_slot_lsn(
            hot,
            self.settings.cell,
            fence_source.slot_name,
            fence_lsn,
            self._remaining(park_deadline),
            self.poll_seconds,
        )
        if not source_fence_satisfied(
            hot_source, confirmed.identity, confirmed.confirmed_lsn, fence_lsn
        ):
            raise RuntimeError("source fence predicate unexpectedly remained false")
        self.mark("t7", confirmed_flush_lsn=confirmed.confirmed_lsn)
        self._fence_wakeup_evidence = {
            **self._fence_wakeup_evidence,
            "confirmed_flush_lsn_at_t7": confirmed.confirmed_lsn,
        }
        return hot_source, fence_lsn, confirmed, self._fence_wakeup_evidence

    def _prepare_leaf_marker_source_fence(
        self,
        hot: Any,
        warm: Any,
        kafka: KafkaControl,
        fence_source: SourceConnectorSpec,
        partitions: tuple[TopicPartition, ...],
        attempt_id: uuid.UUID,
    ) -> tuple[
        HotSourceIdentity,
        str,
        tuple[LeafFenceMarker, ...],
        Mapping[TopicPartition, int],
    ]:
        if self._attempt_epoch is None:
            raise RuntimeError("leaf fence requires a durable attempt epoch")
        hot_source, fence_lsn = hot_identity(
            hot, self.settings.cell, fence_source.slot_name
        )
        updated = warm.execute(
            """
            UPDATE public.flip_attempts
            SET hot_system_identifier=%s, hot_database=%s, fence_lsn=%s::pg_lsn,
                updated_at=clock_timestamp()
            WHERE attempt_epoch=%s AND state='locked'
            """,
            (
                hot_source.system_identifier,
                hot_source.database,
                fence_lsn,
                self._attempt_epoch,
            ),
        ).rowcount
        if updated != 1:
            raise RuntimeError("marker fence provenance persistence CAS failed")
        markers = build_leaf_fence_markers(self.manifest, attempt_id, self._attempt_epoch)
        baselines = kafka.end_offsets(partitions)
        with warm.transaction():
            with warm.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO public.flip_leaf_fence_intents
                        (attempt_epoch, marker_id, parent_name, leaf_name, topic,
                         partition_id, scan_start_offset)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        (
                            self._attempt_epoch,
                            marker.marker_id,
                            marker.parent,
                            marker.leaf,
                            marker.partition.topic,
                            marker.partition.partition,
                            baselines[marker.partition],
                        )
                        for marker in markers
                    ),
                )
            stored = warm.execute(
                "SELECT count(*) FROM public.flip_leaf_fence_intents WHERE attempt_epoch=%s",
                (self._attempt_epoch,),
            ).fetchone()[0]
            if stored != len(markers):
                raise RuntimeError("leaf fence intent vector was not persisted atomically")
        return hot_source, fence_lsn, markers, baselines

    def _mark_leaf_marker_source_fence_start(
        self,
        hot_source: HotSourceIdentity,
        fence_lsn: str,
        partitions: tuple[TopicPartition, ...],
        baselines: Mapping[TopicPartition, int],
    ) -> None:
        self.mark(
            "t5",
            fence_lsn=fence_lsn,
            hot_system_identifier=hot_source.system_identifier,
            source_proof_mode=self.source_proof_mode.value,
        )
        self.mark(
            "t6",
            marker_scan_starts={partition.key: baselines[partition] for partition in partitions},
        )

    def _observe_leaf_marker_source_fence(
        self,
        warm: Any,
        kafka: KafkaControl,
        markers: tuple[LeafFenceMarker, ...],
        baselines: Mapping[TopicPartition, int],
        partitions: tuple[TopicPartition, ...],
        emission_duration_ns: int,
        park_deadline: float,
        atomic_transaction_ns_by_leaf: Mapping[str, int] | None = None,
        parallel_wall_duration_ns: int | None = None,
    ) -> tuple[Mapping[TopicPartition, int], Mapping[str, Any]]:
        self.mark(
            "t6w",
            marker_count=len(markers),
            marker_emission_ns=emission_duration_ns,
        )
        targets = kafka.wait_leaf_fence_markers(
            markers,
            baselines,
            self._remaining(park_deadline),
        )
        if set(targets) != set(partitions):
            raise RuntimeError("leaf fence observer returned an incomplete target vector")
        with warm.transaction():
            for marker in markers:
                changed = warm.execute(
                    """
                    UPDATE public.flip_leaf_fence_intents
                    SET marker_next_offset=%s, observed_at=clock_timestamp()
                    WHERE attempt_epoch=%s AND marker_id=%s AND topic=%s
                      AND partition_id=%s AND marker_next_offset IS NULL
                    """,
                    (
                        targets[marker.partition],
                        self._attempt_epoch,
                        marker.marker_id,
                        marker.partition.topic,
                        marker.partition.partition,
                    ),
                ).rowcount
                if changed != 1:
                    raise RuntimeError(f"leaf fence target persistence failed for {marker.leaf}")
        self.mark(
            "t7",
            source_proof_mode=self.source_proof_mode.value,
            marker_next_offsets={partition.key: targets[partition] for partition in partitions},
        )
        evidence = {
            "mode": self.source_proof_mode.value,
            "marker_schema_version": 1,
            "exactly_once_source": True,
            "consumer_isolation": "read_committed",
            "marker_ids": {marker.partition.key: str(marker.marker_id) for marker in markers},
            "scan_start_offsets": {
                partition.key: baselines[partition] for partition in partitions
            },
            "marker_next_offsets": {
                partition.key: targets[partition] for partition in partitions
            },
            "emission_duration_ns": emission_duration_ns,
            "warm_receipts_complete": False,
        }
        if atomic_transaction_ns_by_leaf is not None:
            evidence = {
                **evidence,
                "detach_marker_contract": (
                    "per_leaf_parallel_transactions_v1"
                    if parallel_wall_duration_ns is not None
                    else "per_leaf_single_transaction_v1"
                ),
                "detach_before_marker": True,
                "atomic_transaction_ns_by_leaf": dict(
                    atomic_transaction_ns_by_leaf
                ),
            }
        if parallel_wall_duration_ns is not None:
            evidence = {
                **evidence,
                "detach_execution_mode": "all_parallel_v1",
                "detach_parallelism": len(markers),
                "parallel_wall_duration_ns": parallel_wall_duration_ns,
            }
        return targets, evidence

    def _capture_leaf_marker_source_fence(
        self,
        hot: Any,
        warm: Any,
        kafka: KafkaControl,
        fence_source: SourceConnectorSpec,
        partitions: tuple[TopicPartition, ...],
        attempt_id: uuid.UUID,
        ownership_epoch: int,
        park_deadline: float,
    ) -> tuple[HotSourceIdentity, str, Mapping[TopicPartition, int], Mapping[str, Any]]:
        hot_source, fence_lsn, markers, baselines = (
            self._prepare_leaf_marker_source_fence(
                hot,
                warm,
                kafka,
                fence_source,
                partitions,
                attempt_id,
            )
        )
        self._mark_leaf_marker_source_fence_start(
            hot_source, fence_lsn, partitions, baselines
        )
        emission_started_ns = time.perf_counter_ns()
        emit_leaf_fence_markers(hot, markers, ownership_epoch)
        emission_duration_ns = time.perf_counter_ns() - emission_started_ns
        targets, evidence = self._observe_leaf_marker_source_fence(
            warm,
            kafka,
            markers,
            baselines,
            partitions,
            emission_duration_ns,
            park_deadline,
        )
        return hot_source, fence_lsn, targets, evidence

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
        source_lane_evidence: dict[str, Any] = {}
        sink_connector_evidence: dict[str, Any] = {}
        marker_fence_evidence: dict[str, Any] | None = None
        atomic_markers: tuple[LeafFenceMarker, ...] = ()
        atomic_marker_baselines: Mapping[TopicPartition, int] = {}
        atomic_transaction_ns_by_leaf: dict[str, int] = {}
        parallel_detach_wall_ns: int | None = None
        ownership_granted = False
        hot_gate_claimed = False
        hot_coordinator_lock_acquired = False
        hot_gate_ownership_epoch: int | None = None
        hot_gate_version_before: int | None = None
        write_fence_evidence: dict[str, Any] = {
            "mode": self.write_fence_mode.value,
            "state_before": None,
            "state_after": None,
            "park_attempt_id": None,
            "ownership_epoch": None,
            "attempt_epoch": None,
            "park_duration_ns": 0,
            "reopened": False,
        }
        configured_sources = source_specs(self.settings, self.manifest)
        fence_source = fence_source_spec(self.settings, self.manifest)
        fence_wakeup_evidence = self._fence_wakeup_evidence

        with connect(self.settings.warm_dsn, autocommit=True) as warm, connect(
            self.settings.hot_dsn, autocommit=True
        ) as hot:
            try:
                self.mark("t0")
                source = ConnectClient(self.settings.source_connect_url)
                sink = ConnectClient(self.settings.sink_connect_url)
                expected_state = "PAUSED" if resume_paused else "RUNNING"
                live_source_configs: dict[str, Mapping[str, str]] = {}
                for spec in configured_sources:
                    state = expected_state if spec == fence_source else "RUNNING"
                    source.wait_state(spec.connector_name, state)
                    live_source_config = source.config(spec.connector_name)
                    self._verify_live_config(
                        f"source:{spec.lane}", live_source_config, spec.config
                    )
                    live_source_configs[spec.lane] = live_source_config
                sink_connector = self.manifest.tables[0].sink_connector
                sink.wait_state(sink_connector, expected_state)
                live_config = sink.config(sink_connector)
                live_sink_configs: dict[str, Mapping[str, str]] = {sink_connector: live_config}
                self._verify_live_config(
                    sink_connector,
                    live_config,
                    shared_sink_config(self.settings, self.manifest),
                )
                sink_connector_evidence["t1"] = self._connector_worker_snapshot(
                    sink, sink_connector, expected_state
                )
                sanitized_configs = {
                    "sources": {
                        lane: {
                            key: value
                            for key, value in config.items()
                            if "password" not in key.lower()
                        }
                        for lane, config in live_source_configs.items()
                    },
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
                        before = slot_status(hot, self.settings.cell, fence_source.slot_name)
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
                    before = slot_status(hot, self.settings.cell, fence_source.slot_name)
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
                admission_states = {
                    spec.lane: expected_state if spec == fence_source else "RUNNING"
                    for spec in configured_sources
                }
                source_lane_evidence["t1"] = self._source_lane_snapshot(
                    source, hot, configured_sources, admission_states
                )
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

                hot_gate_mode = self.write_fence_mode in (
                    WriteFenceMode.HOT_TRANSACTIONAL,
                    WriteFenceMode.OPTIMISTIC_DETACH,
                )
                optimistic_detach = (
                    self.write_fence_mode is WriteFenceMode.OPTIMISTIC_DETACH
                )
                parallel_atomic_detach_marker = (
                    self.source_proof_mode
                    is SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER
                )
                atomic_detach_marker = self.source_proof_mode in (
                    SourceProofMode.ATOMIC_DETACH_MARKER,
                    SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
                )
                if hot_gate_mode:
                    park_deadline = time.monotonic() + self.timeout_seconds
                    self._acquire_session_lock(hot, park_deadline)
                    hot_coordinator_lock_acquired = True
                    self.mark("t2", write_fence_mode=self.write_fence_mode.value)
                    self._set_statement_timeout(hot, park_deadline)
                    gate_before = hot_write_gate_status(
                        hot, self.settings.cell, self.settings.timeslot
                    )
                    if gate_before.state != "open":
                        raise RuntimeError("hot transactional write gate is not open at flip admission")
                    hot_gate_version_before = gate_before.version
                    park_started_ns = time.perf_counter_ns()
                    hot_gate_ownership_epoch = park_hot_write_gate(
                        hot,
                        self.settings.cell,
                        self.settings.timeslot,
                        attempt_id,
                        int(self.scenario_metadata["retiring_write_gate_epoch"]),
                    )
                    hot_gate_claimed = True
                    park_duration_ns = time.perf_counter_ns() - park_started_ns
                    self.mark(
                        "t2h",
                        ownership_epoch=hot_gate_ownership_epoch,
                        park_attempt_id=str(attempt_id),
                        park_duration_ns=park_duration_ns,
                    )
                    write_fence_evidence = {
                        **write_fence_evidence,
                        "state_before": gate_before.state,
                        "state_after": "parked",
                        "park_attempt_id": str(attempt_id),
                        "ownership_epoch": hot_gate_ownership_epoch,
                        "park_duration_ns": park_duration_ns,
                    }

                with warm.transaction():
                    attempt_epoch = warm.execute(
                        """
                        INSERT INTO public.flip_attempts
                            (attempt_id, cell, timeslot, state, table_count, slot_name, publication_name,
                             manifest, manifest_sha256, connector_config_sha256,
                             write_fence_mode, hot_ownership_epoch, hot_gate_version)
                        VALUES (%s, %s, %s, 'locked', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING attempt_epoch
                        """,
                        (
                            attempt_id,
                            self.settings.cell,
                            self.settings.timeslot,
                            self.settings.table_count,
                            fence_source.slot_name,
                            fence_source.publication_name,
                            Jsonb(json.loads(manifest_json)),
                            hashlib.sha256(manifest_json.encode("utf-8")).hexdigest(),
                            connector_config_sha256,
                            self.write_fence_mode.value,
                            hot_gate_ownership_epoch,
                            hot_gate_version_before,
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
                if hot_gate_mode:
                    self._set_statement_timeout(warm, park_deadline)
                    self._set_statement_timeout(hot, park_deadline)
                    bind_hot_write_gate_attempt(
                        hot,
                        self.settings.cell,
                        self.settings.timeslot,
                        attempt_id,
                        attempt_epoch,
                    )
                    self.mark("t2w", attempt_epoch=self._attempt_epoch)
                    write_fence_evidence = {
                        **write_fence_evidence,
                        "attempt_epoch": attempt_epoch,
                    }
                else:
                    self.mark("t2", attempt_epoch=self._attempt_epoch)
                    park_deadline = time.monotonic() + self.timeout_seconds
                    self._set_statement_timeout(warm, park_deadline)
                    self._set_statement_timeout(hot, park_deadline)
                    self._acquire_session_lock(warm, park_deadline)
                    self._acquire_session_lock(hot, park_deadline)
                if optimistic_detach:
                    if self.writer_fence is None or self.writer_quiesce is None:
                        raise RuntimeError(
                            "optimistic detach requires separate admission-fence and in-flight-join callbacks"
                        )
                    fenced_inserted_total = self.writer_fence(
                        self._remaining(park_deadline)
                    )
                    self.mark(
                        "t2f",
                        retiring_admission_stopped=True,
                        workload_inserted_total=fenced_inserted_total,
                    )
                else:
                    if self.writer_quiesce is not None:
                        workload_inserted_total = self.writer_quiesce(
                            self._remaining(park_deadline)
                        )
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

                for spec in configured_sources:
                    verify_source_publication(hot, self.manifest, spec)

                if resume_paused:
                    self._resume_paused_connectors(
                        source,
                        sink,
                        fence_source,
                        sink_connector,
                        park_deadline,
                    )

                if atomic_detach_marker:
                    if hot_gate_ownership_epoch is None:
                        raise RuntimeError(
                            "atomic detach marker proof requires a hot ownership epoch"
                        )
                    (
                        hot_source,
                        fence_lsn,
                        atomic_markers,
                        atomic_marker_baselines,
                    ) = self._prepare_leaf_marker_source_fence(
                        hot,
                        warm,
                        kafka,
                        fence_source,
                        partitions,
                        attempt_id,
                    )
                    if len(atomic_markers) != len(self.manifest.tables):
                        raise RuntimeError(
                            "atomic detach marker plan does not cover every retiring leaf"
                        )

                if parallel_atomic_detach_marker:
                    for index, (route, marker) in enumerate(
                        zip(self.manifest.tables, atomic_markers, strict=True), start=1
                    ):
                        if marker.parent != route.parent or marker.leaf != route.leaf:
                            raise RuntimeError(
                                "parallel atomic detach marker order contradicts the manifest"
                            )
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
                            raise RuntimeError(
                                f"table-state CAS failed before detaching {route.parent}"
                            )

                    parallel_started_ns = time.perf_counter_ns()
                    self.mark(
                        "t3_parallel",
                        detach_execution_mode="all_parallel_v1",
                        detach_parallelism=len(atomic_markers),
                    )

                    def detach_one(marker: LeafFenceMarker) -> None:
                        remaining_ms = max(
                            1, int(self._remaining(park_deadline) * 1000)
                        )
                        connection_options = (
                            f"-c statement_timeout={remaining_ms}ms "
                            f"-c lock_timeout={min(5000, remaining_ms)}ms"
                        )
                        with connect(
                            self.settings.hot_dsn,
                            autocommit=True,
                            options=connection_options,
                        ) as parallel_hot:
                            atomic_detach_and_emit_leaf_fence_marker(
                                parallel_hot,
                                marker,
                                hot_gate_ownership_epoch,
                            )

                    try:
                        parallel_results = run_all_parallel(
                            atomic_markers, detach_one
                        )
                    except ParallelDetachError as parallel_error:
                        parallel_detach_wall_ns = (
                            time.perf_counter_ns() - parallel_started_ns
                        )
                        succeeded = {
                            result.item.leaf: result.duration_ns
                            for result in parallel_error.succeeded
                        }
                        for index, (route, marker) in enumerate(
                            zip(self.manifest.tables, atomic_markers, strict=True),
                            start=1,
                        ):
                            duration = succeeded.get(marker.leaf)
                            if duration is not None:
                                self.detach_ns[route.parent] = duration
                                atomic_transaction_ns_by_leaf[route.leaf] = duration
                                warm.execute(
                                    """
                                    UPDATE public.flip_table_states
                                    SET state='detached', detach_finished_at=clock_timestamp(),
                                        detach_duration_ns=%s
                                    WHERE attempt_epoch=%s AND parent_name=%s
                                      AND state='detach_started'
                                    """,
                                    (duration, self._attempt_epoch, route.parent),
                                )
                                self.mark(
                                    f"t4_{index}",
                                    table=route.parent,
                                    duration_ns=duration,
                                )
                                continue
                            pending = hot.execute(
                                """
                                SELECT i.inhdetachpending
                                FROM pg_inherits i
                                WHERE i.inhparent=%s::regclass
                                  AND i.inhrelid=%s::regclass
                                """,
                                (
                                    f"public.{route.parent}",
                                    f"public.{route.leaf}",
                                ),
                            ).fetchone()
                            state = (
                                "detached"
                                if pending is None
                                else (
                                    "pending_finalize"
                                    if pending[0]
                                    else "detach_started"
                                )
                            )
                            warm.execute(
                                """
                                UPDATE public.flip_table_states SET state=%s
                                WHERE attempt_epoch=%s AND parent_name=%s
                                """,
                                (state, self._attempt_epoch, route.parent),
                            )
                        self.mark(
                            "t4_parallel",
                            success=False,
                            duration_ns=parallel_detach_wall_ns,
                            succeeded_leaves=sorted(succeeded),
                            failed_leaves=[
                                result.item.leaf
                                for result in parallel_error.failed
                            ],
                        )
                        raise RuntimeError(str(parallel_error)) from parallel_error
                    else:
                        parallel_detach_wall_ns = (
                            time.perf_counter_ns() - parallel_started_ns
                        )
                        for index, result in enumerate(parallel_results, start=1):
                            route = self.manifest.tables[index - 1]
                            duration = result.duration_ns
                            self.detach_ns[route.parent] = duration
                            atomic_transaction_ns_by_leaf[route.leaf] = duration
                            updated = warm.execute(
                                """
                                UPDATE public.flip_table_states
                                SET state='detached', detach_finished_at=clock_timestamp(),
                                    detach_duration_ns=%s
                                WHERE attempt_epoch=%s AND parent_name=%s
                                  AND state='detach_started'
                                """,
                                (duration, self._attempt_epoch, route.parent),
                            ).rowcount
                            if updated != 1:
                                raise RuntimeError(
                                    f"table-state CAS failed after detaching {route.parent}"
                                )
                            self.mark(
                                f"t4_{index}",
                                table=route.parent,
                                duration_ns=duration,
                            )
                        self.mark(
                            "t4_parallel",
                            success=True,
                            duration_ns=parallel_detach_wall_ns,
                            succeeded_leaves=[
                                result.item.leaf for result in parallel_results
                            ],
                            failed_leaves=[],
                        )
                else:
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
                            if atomic_detach_marker:
                                marker = atomic_markers[index - 1]
                                if marker.parent != route.parent or marker.leaf != route.leaf:
                                    raise RuntimeError(
                                        "atomic detach marker order contradicts the manifest"
                                    )
                                atomic_detach_and_emit_leaf_fence_marker(
                                    hot,
                                    marker,
                                    hot_gate_ownership_epoch,
                                )
                            else:
                                hot.execute(
                                    sql.SQL(
                                        "ALTER TABLE {} DETACH PARTITION {} CONCURRENTLY"
                                    ).format(
                                        sql.Identifier(route.parent),
                                        sql.Identifier(route.leaf),
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
                        if atomic_detach_marker:
                            atomic_transaction_ns_by_leaf[route.leaf] = duration
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

                if optimistic_detach:
                    workload_inserted_total = self.writer_quiesce(
                        self._remaining(park_deadline)
                    )
                    active_at_t2q = None
                    if self.active_writer_inserted_snapshot is not None and self.active_writer_is_alive is not None:
                        if not self.active_writer_is_alive():
                            raise RuntimeError(
                                "active-timeslot writer stopped during optimistic detach"
                            )
                        active_at_t2q = self.active_writer_inserted_snapshot()
                        workload_by_timeslot["t2q"] = {
                            "active": active_at_t2q,
                            "retiring": workload_inserted_total,
                        }
                    self.mark(
                        "t2q",
                        optimistic_in_flight_resolved=True,
                        workload_inserted_total=workload_inserted_total,
                        active_workload_inserted=active_at_t2q,
                    )

                running_states = {spec.lane: "RUNNING" for spec in configured_sources}
                source_lane_evidence["t5"] = self._source_lane_snapshot(
                    source, hot, configured_sources, running_states, park_deadline
                )
                target_scan_starts: Mapping[TopicPartition, int] | None = None
                if self.source_proof_mode is SourceProofMode.PER_LEAF_MARKER:
                    if hot_gate_ownership_epoch is None:
                        raise RuntimeError("leaf marker proof requires a hot ownership epoch")
                    hot_source, fence_lsn, target_values, marker_fence_evidence = (
                        self._capture_leaf_marker_source_fence(
                            hot,
                            warm,
                            kafka,
                            fence_source,
                            partitions,
                            attempt_id,
                            hot_gate_ownership_epoch,
                            park_deadline,
                        )
                    )
                    fence_wakeup_evidence = self._fence_wakeup_evidence
                elif atomic_detach_marker:
                    if hot_source is None or fence_lsn is None:
                        raise RuntimeError(
                            "atomic detach marker provenance was not prepared"
                        )
                    self._mark_leaf_marker_source_fence_start(
                        hot_source,
                        fence_lsn,
                        partitions,
                        atomic_marker_baselines,
                    )
                    target_values, marker_fence_evidence = (
                        self._observe_leaf_marker_source_fence(
                            warm,
                            kafka,
                            atomic_markers,
                            atomic_marker_baselines,
                            partitions,
                            (
                                parallel_detach_wall_ns
                                if parallel_atomic_detach_marker
                                else sum(atomic_transaction_ns_by_leaf.values())
                            ),
                            park_deadline,
                            atomic_transaction_ns_by_leaf,
                            parallel_detach_wall_ns,
                        )
                    )
                    fence_wakeup_evidence = self._fence_wakeup_evidence
                else:
                    hot_source, fence_lsn, _confirmed, fence_wakeup_evidence = (
                        self._capture_and_confirm_source_fence(
                            hot,
                            warm,
                            fence_source,
                            park_deadline,
                        )
                    )
                    observed_starts = kafka.committed_offsets(
                        group_by_partition,
                        min(10.0, self._remaining(park_deadline)),
                    )
                    target_scan_starts = {
                        partition: observed_starts.get(partition, 0)
                        for partition in partitions
                    }
                    target_values = kafka.read_committed_target_offsets(
                        partitions,
                        target_scan_starts,
                        self._remaining(park_deadline),
                    )
                target = OffsetVector(
                    self.settings.cell,
                    self.settings.timeslot,
                    self._attempt_epoch,
                    target_values,
                )
                target_event: dict[str, Any] = {
                    "target_offsets": {
                        item.key: target.values[item] for item in partitions
                    },
                    "target_offset_semantics": (
                        "exact_leaf_marker_next_offset_v1"
                        if self.source_proof_mode
                        in (
                            SourceProofMode.PER_LEAF_MARKER,
                            SourceProofMode.ATOMIC_DETACH_MARKER,
                            SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
                        )
                        else "read_committed_visible_records_v1"
                    ),
                }
                if target_scan_starts is not None:
                    target_event = {
                        **target_event,
                        "target_scan_start_offsets": {
                            partition.key: target_scan_starts[partition]
                            for partition in partitions
                        },
                    }
                self.mark("t8", **target_event)
                source_lane_evidence["t7"] = self._source_lane_snapshot(
                    source, hot, configured_sources, running_states, park_deadline
                )
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

                if self.source_proof_mode in (
                    SourceProofMode.PER_LEAF_MARKER,
                    SourceProofMode.ATOMIC_DETACH_MARKER,
                    SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
                ):
                    markers = build_leaf_fence_markers(
                        self.manifest, attempt_id, self._attempt_epoch
                    )
                    while time.monotonic() < park_deadline:
                        receipts = observed_leaf_fence_receipts(
                            warm, markers, hot_gate_ownership_epoch
                        )
                        if receipts == frozenset(partitions):
                            self.mark(
                                "t10",
                                warm_commit_proven_by_leaf_marker_receipts=True,
                            )
                            self.mark(
                                "t11",
                                receipt_count=len(receipts),
                                marker_next_offsets={
                                    item.key: target.values[item] for item in partitions
                                },
                            )
                            marker_fence_evidence = {
                                **dict(marker_fence_evidence or {}),
                                "warm_receipts_complete": True,
                            }
                            break
                        time.sleep(self.poll_seconds)
                    else:
                        raise TimeoutError(
                            "warm JDBC marker receipts did not complete before the writer-park deadline"
                        )
                    observed_values = kafka.committed_offsets(
                        group_by_partition,
                        min(0.25, self._remaining(park_deadline)),
                    )
                    current_values = {
                        partition: observed_values.get(partition, 0)
                        for partition in partitions
                    }
                else:
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
                confirmed_final = slot_status(hot, self.settings.cell, fence_source.slot_name)
                if self.source_proof_mode in (
                    SourceProofMode.PER_LEAF_MARKER,
                    SourceProofMode.ATOMIC_DETACH_MARKER,
                    SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
                ):
                    source_ready = confirmed_final.identity == hot_source
                    markers = build_leaf_fence_markers(
                        self.manifest, attempt_id, self._attempt_epoch
                    )
                    receipt_ready = observed_leaf_fence_receipts(
                        warm, markers, hot_gate_ownership_epoch
                    ) == frozenset(partitions)
                    final_observed_values = kafka.committed_offsets(
                        group_by_partition,
                        min(0.25, self._remaining(park_deadline)),
                    )
                    final_current_values = {
                        partition: final_observed_values.get(partition, 0)
                        for partition in partitions
                    }
                    final_gate_ready = receipt_ready
                else:
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
                    final_gate_ready = offset_gate(partitions, target, final_current).ready
                current_values = final_current_values
                self._set_statement_timeout(hot, park_deadline)
                self._set_statement_timeout(warm, park_deadline)
                for spec in configured_sources:
                    verify_source_publication(hot, self.manifest, spec)
                    self._verify_live_config(
                        f"source:{spec.lane}",
                        source.config(
                            spec.connector_name,
                            timeout_seconds=self._remaining(park_deadline),
                        ),
                        spec.config,
                    )
                self._verify_live_config(
                    sink_connector,
                    sink.config(
                        sink_connector,
                        timeout_seconds=self._remaining(park_deadline),
                    ),
                    shared_sink_config(self.settings, self.manifest),
                )
                sink_connector_evidence["t13"] = self._connector_worker_snapshot(
                    sink,
                    sink_connector,
                    "RUNNING",
                    self._remaining(park_deadline),
                )
                source_lane_evidence["t13"] = self._source_lane_snapshot(
                    source, hot, configured_sources, running_states, park_deadline
                )
                if hot_gate_mode:
                    retiring_gate = hot_write_gate_status(
                        hot, self.settings.cell, self.settings.timeslot
                    )
                    active_gate = hot_write_gate_status(hot, self.settings.cell, "active")
                    if (
                        retiring_gate.state != "parked"
                        or retiring_gate.park_attempt_id != str(attempt_id)
                        or retiring_gate.attempt_epoch != self._attempt_epoch
                        or retiring_gate.ownership_epoch != hot_gate_ownership_epoch
                    ):
                        raise RuntimeError(
                            "hot transactional retiring gate lost exact flip ownership"
                        )
                    if active_gate.state != "open":
                        raise RuntimeError(
                            "active hot write gate changed during retiring flip"
                        )
                    write_fence_evidence = {
                        **write_fence_evidence,
                        "verified_before_grant": True,
                        "active_gate_state": active_gate.state,
                    }
                evidence = GateEvidence(
                    detached_count == self.settings.table_count and catalog_detached,
                    source_ready,
                    stored_target == dict(target.values),
                    final_gate_ready,
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
                released_hot = True
                released_warm = True
                if hot_gate_mode:
                    try:
                        released_hot = self._release_session_lock(hot)
                        hot_coordinator_lock_acquired = not released_hot
                    except Exception:
                        released_hot = False
                else:
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
                    "schema_version": (
                        7
                        if self.source_proof_mode
                        is SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER
                        else 6
                        if self.source_proof_mode
                        is SourceProofMode.ATOMIC_DETACH_MARKER
                        else (
                            5
                            if self.source_proof_mode
                            is SourceProofMode.PER_LEAF_MARKER
                            else 4
                        )
                    ),
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
                    "source_topology": self.settings.source_topology,
                    "source_connector_count": len(configured_sources),
                    "fence_source_lane": fence_source.lane,
                    "fence_slot_name": fence_source.slot_name,
                    "fence_publication_name": fence_source.publication_name,
                    "source_topic_prefixes": {
                        spec.lane: spec.topic_prefix for spec in configured_sources
                    },
                    "source_lane_evidence": source_lane_evidence,
                    "sink_connector_evidence": sink_connector_evidence,
                    "environment_generation_id": self.scenario_metadata.get(
                        "environment_generation_id"
                    ) or "legacy",
                    "scenario": self.scenario_metadata,
                    "ownership_outcome": "success",
                    "verification_outcome": "pending",
                    "fence_lsn": fence_lsn,
                    "fence_wakeup": dict(fence_wakeup_evidence),
                    "marker_fence": marker_fence_evidence,
                    "write_fence": dict(write_fence_evidence),
                    "hot_identity": None if hot_source is None else asdict(hot_source),
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
                if self._attempt_epoch is None and hot_gate_claimed and not ownership_granted:
                    try:
                        resolved_attempt = warm.execute(
                            """
                            SELECT a.attempt_epoch, a.state, p.state, p.attempt_epoch
                            FROM public.flip_attempts AS a
                            JOIN public.partition_tracker AS p
                              ON p.cell=a.cell AND p.timeslot=a.timeslot
                            WHERE a.attempt_id=%s AND a.cell=%s AND a.timeslot=%s
                            """,
                            (attempt_id, self.settings.cell, self.settings.timeslot),
                        ).fetchone()
                    except Exception as resolution_error:
                        error_payload = {
                            **(error_payload or {}),
                            "attempt_resolution_error": (
                                f"{type(resolution_error).__name__}: {resolution_error}"
                            ),
                        }
                    else:
                        if (
                            resolved_attempt is not None
                            and resolved_attempt[1] in ("locked", "drained", "recovering")
                            and resolved_attempt[2] in ("locked", "drained", "recovering")
                            and resolved_attempt[0] == resolved_attempt[3]
                        ):
                            self._attempt_epoch = int(resolved_attempt[0])
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
                            if hot_gate_mode:
                                gate = hot_write_gate_status(
                                    hot, self.settings.cell, self.settings.timeslot
                                )
                                if (
                                    gate.state != "parked"
                                    or gate.park_attempt_id != str(attempt_id)
                                    or gate.attempt_epoch not in (None, self._attempt_epoch)
                                ):
                                    raise RuntimeError(
                                        "recovery refused a mismatched hot transactional gate"
                                    )
                                reopened_epoch = reopen_hot_write_gate(
                                    hot,
                                    self.settings.cell,
                                    self.settings.timeslot,
                                    attempt_id,
                                    gate.attempt_epoch,
                                )
                                write_fence_evidence = {
                                    **write_fence_evidence,
                                    "reopened": True,
                                    "reopened_ownership_epoch": reopened_epoch,
                                }
                                if hot_coordinator_lock_acquired:
                                    if not self._release_session_lock(hot):
                                        raise RuntimeError(
                                            "recovery could not release the hot coordinator lock"
                                        )
                                    hot_coordinator_lock_acquired = False
                            else:
                                self._release_session_lock(hot)
                                self._remaining(recovery_deadline)
                                self._release_session_lock(warm)
                            self._remaining(recovery_deadline)
                            self.mark("trevert_end", recovered_tables=recovered)
                            outcome = "reverted"
                elif hot_gate_claimed and not ownership_granted:
                    tracker_state = warm.execute(
                        "SELECT state FROM public.partition_tracker WHERE cell=%s AND timeslot=%s",
                        (self.settings.cell, self.settings.timeslot),
                    ).fetchone()
                    if tracker_state == ("hot_primary",):
                        gate = hot_write_gate_status(
                            hot, self.settings.cell, self.settings.timeslot
                        )
                        if (
                            gate.state == "parked"
                            and gate.park_attempt_id == str(attempt_id)
                            and gate.attempt_epoch is None
                        ):
                            reopened_epoch = reopen_hot_write_gate(
                                hot,
                                self.settings.cell,
                                self.settings.timeslot,
                                attempt_id,
                                None,
                            )
                            write_fence_evidence = {
                                **write_fence_evidence,
                                "reopened": True,
                                "reopened_ownership_epoch": reopened_epoch,
                            }
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
            "schema_version": (
                7
                if self.source_proof_mode
                is SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER
                else 6
                if self.source_proof_mode
                is SourceProofMode.ATOMIC_DETACH_MARKER
                else (
                    5
                    if self.source_proof_mode is SourceProofMode.PER_LEAF_MARKER
                    else 4
                )
            ),
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
                "source_topology": self.settings.source_topology,
                "source_connector_count": len(configured_sources),
                "fence_source_lane": fence_source.lane,
                "fence_slot_name": fence_source.slot_name,
                "fence_publication_name": fence_source.publication_name,
                "source_topic_prefixes": {
                    spec.lane: spec.topic_prefix for spec in configured_sources
                },
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
            "cell": self.settings.cell,
            "timeslot": self.settings.timeslot,
            "environment_generation_id": self.scenario_metadata.get(
                "environment_generation_id"
            )
            or "legacy",
            "scenario": self.scenario_metadata,
            "workload_inserted_total": workload_inserted_total,
            "workload_by_timeslot": workload_by_timeslot,
            "source_lane_evidence": source_lane_evidence,
            "sink_connector_evidence": sink_connector_evidence,
            "admission": admission,
            "hot_identity": None if hot_source is None else asdict(hot_source),
            "fence_lsn": fence_lsn,
            "fence_wakeup": dict(self._fence_wakeup_evidence),
            "marker_fence": marker_fence_evidence,
            "write_fence": dict(write_fence_evidence),
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
