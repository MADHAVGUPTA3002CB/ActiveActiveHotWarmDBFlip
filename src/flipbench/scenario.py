from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .connect_api import ConnectClient
from .connector_configs import fence_source_spec, source_specs
from .core import (
    BenchmarkManifest,
    TimingError,
    TopicPartition,
    lag_admission_ready,
    overload_lag_vectors,
    production_admission_ready,
)
from .kafka_io import KafkaControl
from .overload import BackgroundBatchWriter
from .postgres_io import connect, guarded_insert_events, hot_identity, slot_status, wait_slot_lsn
from .settings import Settings
from .workload import MixedWorkload, WorkloadMix


@dataclass(frozen=True, slots=True)
class PreparedLag:
    run_id: uuid.UUID
    sink_events: int
    source_events: int
    source_lag_bytes: int


@dataclass(frozen=True, slots=True)
class RunningOverload:
    run_id: uuid.UUID
    writer: BackgroundBatchWriter
    inserted_at_admission: int
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def stop_and_join(self, timeout_seconds: float) -> int:
        return self.writer.stop_and_join(timeout_seconds)


@dataclass(frozen=True, slots=True)
class ProductionWorkload:
    run_id: uuid.UUID
    active_run_id: uuid.UUID
    writers: MixedWorkload
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def stop_retiring(self, timeout_seconds: float) -> int:
        return self.writers.stop_retiring(timeout_seconds)

    def stop_active(self, timeout_seconds: float) -> int:
        return self.writers.stop_active(timeout_seconds)


def _wait_sources_running(
    source: ConnectClient, settings: Settings, manifest: BenchmarkManifest
) -> None:
    for spec in source_specs(settings, manifest):
        source.wait_state(spec.connector_name, "RUNNING")


def prepare_paused_backlog(
    settings: Settings,
    manifest: BenchmarkManifest,
    sink_events_per_table: int,
    source_events_per_table: int,
    payload_bytes: int,
) -> PreparedLag:
    run_id = uuid.uuid4()
    source = ConnectClient(settings.source_connect_url)
    sink = ConnectClient(settings.sink_connect_url)
    fence_source = fence_source_spec(settings, manifest)
    _wait_sources_running(source, settings, manifest)
    sink_connector = manifest.tables[0].sink_connector
    sink.set_paused(sink_connector, True)
    sink.wait_state(sink_connector, "PAUSED")

    sink_events = guarded_insert_events(
        settings.hot_dsn,
        settings.warm_dsn,
        manifest,
        run_id,
        sink_events_per_table,
        "retiring",
        payload_bytes,
    )
    with connect(settings.hot_dsn, autocommit=True) as hot:
        _, fence = hot_identity(hot, settings.cell, fence_source.slot_name)
        wait_slot_lsn(hot, settings.cell, fence_source.slot_name, fence, 120, 0.1)

    source.set_paused(fence_source.connector_name, True)
    source.wait_state(fence_source.connector_name, "PAUSED")
    source_events = guarded_insert_events(
        settings.hot_dsn,
        settings.warm_dsn,
        manifest,
        run_id,
        source_events_per_table,
        "retiring",
        payload_bytes,
    )
    with connect(settings.hot_dsn, autocommit=True) as hot:
        lag_bytes = slot_status(hot, settings.cell, fence_source.slot_name).lag_bytes
    if lag_bytes <= 0:
        raise RuntimeError("failed to establish non-zero source lag")
    return PreparedLag(run_id, sink_events, source_events, lag_bytes)


def prepare_running_overload(
    settings: Settings,
    manifest: BenchmarkManifest,
    batch_events_per_table: int,
    payload_bytes: int,
    min_source_lag_bytes: int,
    min_source_lag_records_per_partition: int,
    min_sink_lag_records_per_partition: int,
    stable_samples: int,
    max_admitted_rows_per_partition: int,
    max_batches: int,
    timeout_seconds: float,
    poll_seconds: float,
) -> RunningOverload:
    if timeout_seconds <= 0 or not 0.01 <= poll_seconds <= 5:
        raise ValueError("timeout_seconds must be positive and poll_seconds must be between 0.01 and 5")
    lag_admission_ready(0, 0, min_source_lag_bytes, min_sink_lag_records_per_partition)
    if (
        min_source_lag_records_per_partition <= 0
        or stable_samples <= 0
        or max_admitted_rows_per_partition < min_source_lag_records_per_partition
    ):
        raise ValueError("source record lag, stable_samples, and admission cap are inconsistent")

    source = ConnectClient(settings.source_connect_url)
    sink = ConnectClient(settings.sink_connect_url)
    fence_source = fence_source_spec(settings, manifest)
    _wait_sources_running(source, settings, manifest)
    sink.wait_state(manifest.tables[0].sink_connector, "RUNNING")

    partitions = tuple(TopicPartition(route.topic, route.partition) for route in manifest.tables)
    group_by_partition = {
        TopicPartition(route.topic, route.partition): route.sink_group for route in manifest.tables
    }
    kafka = KafkaControl(settings.kafka_bootstrap)
    baseline_end_raw = kafka.end_offsets(partitions)
    baseline_end = {partition.key: baseline_end_raw[partition] for partition in partitions}
    with connect(settings.hot_dsn, autocommit=True) as hot:
        baseline_slot = slot_status(hot, settings.cell, fence_source.slot_name)

    run_id = uuid.uuid4()
    writer = BackgroundBatchWriter.start(
        lambda: guarded_insert_events(
            settings.hot_dsn,
            settings.warm_dsn,
            manifest,
            run_id,
            batch_events_per_table,
            "retiring",
            payload_bytes,
        ),
        max_batches,
    )
    inserted = 0
    last_source_lag = 0
    last_source_records: Mapping[str, int] = {}
    last_sink_records: Mapping[str, int] = {}
    consecutive_ready = 0
    stable_window: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds
    admitted = False
    try:
        with connect(settings.hot_dsn, autocommit=True) as hot:
            while time.monotonic() < deadline:
                inserted = writer.total_inserted()
                last_source_lag = slot_status(
                    hot, settings.cell, fence_source.slot_name
                ).lag_bytes
                sample_started_ns = time.perf_counter_ns()
                end_offsets_raw = kafka.end_offsets(partitions)
                remaining = max(0.1, deadline - time.monotonic())
                committed_raw = kafka.committed_offsets(group_by_partition, min(2.0, remaining))
                if inserted % len(partitions) != 0:
                    raise RuntimeError("writer committed-row count is not divisible by the table count")
                committed_per_table = inserted // len(partitions)
                if committed_per_table > max_admitted_rows_per_partition:
                    raise RuntimeError(
                        "healthy-overload workload exceeded its pre-lock admission cap; "
                        f"rows_per_partition={committed_per_table}, cap={max_admitted_rows_per_partition}"
                    )
                current_end = {partition.key: end_offsets_raw[partition] for partition in partitions}
                current_committed = {
                    partition.key: committed_raw.get(partition, 0) for partition in partitions
                }
                committed_hot_rows = {partition.key: committed_per_table for partition in partitions}
                try:
                    last_source_records, last_sink_records = overload_lag_vectors(
                        baseline_end,
                        current_end,
                        current_committed,
                        committed_hot_rows,
                    )
                except TimingError:
                    consecutive_ready = 0
                    stable_window = []
                    if not writer.is_alive():
                        writer.stop_and_join(max(0.1, min(5.0, remaining)))
                    time.sleep(poll_seconds)
                    continue
                ready = lag_admission_ready(
                    last_source_lag,
                    min(last_sink_records.values()),
                    min_source_lag_bytes,
                    min_sink_lag_records_per_partition,
                ) and min(last_source_records.values()) >= min_source_lag_records_per_partition
                sample = {
                    "sample_started_ns": sample_started_ns,
                    "sample_finished_ns": time.perf_counter_ns(),
                    "source_lag_bytes": last_source_lag,
                    "source_lag_records_by_partition": dict(last_source_records),
                    "sink_lag_records_by_partition": dict(last_sink_records),
                    "current_end_offsets": current_end,
                    "current_committed_offsets": current_committed,
                    "committed_hot_rows_by_partition": committed_hot_rows,
                }
                if ready:
                    consecutive_ready += 1
                    stable_window.append(sample)
                else:
                    consecutive_ready = 0
                    stable_window = []
                if consecutive_ready >= stable_samples and writer.is_alive():
                    admitted = True
                    return RunningOverload(
                        run_id,
                        writer,
                        inserted,
                        {
                            "mode": "healthy-overload",
                            "batch_events_per_table": batch_events_per_table,
                            "payload_bytes": payload_bytes,
                            "max_batches": max_batches,
                            "min_source_lag_bytes": min_source_lag_bytes,
                            "min_source_lag_records_per_partition": min_source_lag_records_per_partition,
                            "min_sink_lag_records_per_partition": min_sink_lag_records_per_partition,
                            "t1_min_sink_lag_records_per_partition": 1,
                            "t1_recheck_timeout_seconds": 10.0,
                            "required_stable_samples": stable_samples,
                            "max_admitted_rows_per_partition": max_admitted_rows_per_partition,
                            "trigger_source_lag_bytes": last_source_lag,
                            "trigger_source_lag_records_by_partition": dict(last_source_records),
                            "trigger_sink_lag_records_by_partition": dict(last_sink_records),
                            "baseline_end_offsets": baseline_end,
                            "baseline_slot": {
                                "confirmed_lsn": baseline_slot.confirmed_lsn,
                                "restart_lsn": baseline_slot.restart_lsn,
                                "lag_bytes": baseline_slot.lag_bytes,
                            },
                            "stable_window": stable_window,
                            "inserted_at_admission": inserted,
                        },
                    )
                if not writer.is_alive():
                    inserted = writer.stop_and_join(max(0.1, min(5.0, remaining)))
                    raise RuntimeError(
                        "overload writer exhausted before admission; "
                        f"source_lag_bytes={last_source_lag}, source_lag_records={dict(last_source_records)}, "
                        f"sink_lag_records={dict(last_sink_records)}, inserted={inserted}"
                    )
                time.sleep(poll_seconds)
        raise TimeoutError(
            "healthy-overload admission timed out; "
            f"source_lag_bytes={last_source_lag}, source_lag_records={dict(last_source_records)}, "
            f"sink_lag_records={dict(last_sink_records)}, inserted={inserted}"
        )
    finally:
        kafka.close()
        if not admitted:
            writer.stop_and_join(10.0)


def prepare_production_workload(
    settings: Settings,
    manifest: BenchmarkManifest,
    active_events_per_table: int,
    retiring_events_per_table: int,
    payload_bytes: int,
    active_pause_ms: float,
    retiring_pause_ms: float,
    max_source_lag_bytes: int,
    max_sink_lag_records_per_partition: int,
    stable_samples: int,
    max_batches: int,
    timeout_seconds: float,
    poll_seconds: float,
) -> ProductionWorkload:
    mix = WorkloadMix(active_events_per_table, retiring_events_per_table)
    if retiring_events_per_table == 0:
        raise ValueError("the first production-shaped profile requires a small positive retiring workload")
    if (
        timeout_seconds <= 0
        or not 0.001 <= poll_seconds <= 5
        or active_pause_ms < 0
        or retiring_pause_ms < 0
        or max_source_lag_bytes < 0
        or max_sink_lag_records_per_partition < 0
        or stable_samples <= 0
    ):
        raise ValueError("production workload thresholds and pacing values are inconsistent")

    source = ConnectClient(settings.source_connect_url)
    sink = ConnectClient(settings.sink_connect_url)
    fence_source = fence_source_spec(settings, manifest)
    _wait_sources_running(source, settings, manifest)
    sink.wait_state(manifest.tables[0].sink_connector, "RUNNING")

    run_id = uuid.uuid4()
    active_run_id = uuid.uuid4()

    def paced_insert(target_run_id: uuid.UUID, events: int, timeslot: str, pause_ms: float) -> int:
        inserted = guarded_insert_events(
            settings.hot_dsn,
            settings.warm_dsn,
            manifest,
            target_run_id,
            events,
            timeslot,
            payload_bytes,
        )
        if pause_ms:
            time.sleep(pause_ms / 1000)
        return inserted

    writers = MixedWorkload.start(
        lambda: paced_insert(active_run_id, mix.active_events_per_table, "active", active_pause_ms),
        lambda: paced_insert(run_id, mix.retiring_events_per_table, "retiring", retiring_pause_ms),
        max_batches,
    )
    partitions = tuple(TopicPartition(route.topic, route.partition) for route in manifest.tables)
    group_by_partition = {partition: manifest.tables[0].sink_group for partition in partitions}
    kafka = KafkaControl(settings.kafka_bootstrap)
    stable_window: list[dict[str, Any]] = []
    consecutive_ready = 0
    admitted = False
    deadline = time.monotonic() + timeout_seconds
    last_lag_bytes = 0
    last_sink_lag: dict[str, int] = {}
    try:
        with connect(settings.hot_dsn, autocommit=True) as hot:
            while time.monotonic() < deadline:
                if not writers.active_is_alive() or not writers.retiring_is_alive():
                    raise RuntimeError("production-shaped writer stopped before admission")
                end_offsets = kafka.end_offsets(partitions)
                committed = kafka.committed_offsets(
                    group_by_partition,
                    min(2.0, max(0.1, deadline - time.monotonic())),
                )
                last_lag_bytes = slot_status(
                    hot, settings.cell, fence_source.slot_name
                ).lag_bytes
                last_sink_lag = {
                    partition.key: max(0, end_offsets[partition] - committed.get(partition, 0))
                    for partition in partitions
                }
                active_total = writers.active_total()
                retiring_total = writers.retiring_total()
                ready = (
                    active_total > retiring_total > 0
                    and production_admission_ready(
                        last_lag_bytes,
                        last_sink_lag,
                        max_source_lag_bytes,
                        max_sink_lag_records_per_partition,
                    )
                )
                sample = {
                    "sample_ns": time.perf_counter_ns(),
                    "source_lag_bytes": last_lag_bytes,
                    "sink_lag_records_by_partition": last_sink_lag,
                    "active_committed_rows": active_total,
                    "retiring_committed_rows": retiring_total,
                }
                if ready:
                    consecutive_ready += 1
                    stable_window.append(sample)
                else:
                    consecutive_ready = 0
                    stable_window = []
                if consecutive_ready >= stable_samples:
                    admitted = True
                    return ProductionWorkload(
                        run_id,
                        active_run_id,
                        writers,
                        {
                            "mode": "production-shaped",
                            "active_events_per_table": active_events_per_table,
                            "retiring_events_per_table": retiring_events_per_table,
                            "active_pause_ms": active_pause_ms,
                            "retiring_pause_ms": retiring_pause_ms,
                            "max_source_lag_bytes": max_source_lag_bytes,
                            "max_sink_lag_records_per_partition": max_sink_lag_records_per_partition,
                            "required_stable_samples": stable_samples,
                            "stable_window": stable_window,
                            "active_run_id": str(active_run_id),
                        },
                    )
                time.sleep(poll_seconds)
        raise TimeoutError(
            "production-shaped admission timed out; "
            f"source_lag_bytes={last_lag_bytes}, sink_lag={last_sink_lag}, "
            f"active={writers.active_total()}, retiring={writers.retiring_total()}"
        )
    finally:
        kafka.close()
        if not admitted:
            try:
                writers.stop_retiring(10.0)
            finally:
                writers.stop_active(10.0)
