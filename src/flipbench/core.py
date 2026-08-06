from __future__ import annotations

import json
import math
import re
import uuid
from dataclasses import asdict, dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping, Sequence


class FenceWakeupMode(StrEnum):
    PASSIVE = "passive"
    IMMEDIATE_HEARTBEAT = "immediate_heartbeat"


class SourceProofMode(StrEnum):
    SLOT_LSN = "slot_lsn_v1"
    PER_LEAF_MARKER = "per_leaf_marker_v1"
    ATOMIC_DETACH_MARKER = "atomic_detach_marker_v1"
    PARALLEL_ATOMIC_DETACH_MARKER = "parallel_atomic_detach_marker_v1"


class WriteFenceMode(StrEnum):
    WARM_TRACKER_ADVISORY = "warm_tracker_advisory_v1"
    HOT_TRANSACTIONAL = "hot_transactional_v1"
    OPTIMISTIC_DETACH = "optimistic_detach_v1"


class OptimisticAdmissionCheckMode(StrEnum):
    STATE_AND_EPOCH = "state_and_epoch_v1"
    STATE_ONLY = "state_only_v1"


def state_only_batch_admission_supported(
    source_topology: str,
    fence_wakeup_mode: FenceWakeupMode | str,
    write_fence_mode: WriteFenceMode | str,
    source_proof_mode: SourceProofMode | str,
) -> bool:
    """Return whether a variant contract permits state-only foreground admission."""
    wakeup = str(fence_wakeup_mode)
    fence = str(write_fence_mode)
    proof = str(source_proof_mode)
    if (
        fence != WriteFenceMode.OPTIMISTIC_DETACH.value
        or wakeup != FenceWakeupMode.PASSIVE.value
    ):
        return False
    if proof == SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER.value:
        return source_topology in ("isolated", "shared")
    return (
        source_topology == "shared"
        and proof == SourceProofMode.SLOT_LSN.value
    )


class FlipbenchError(ValueError):
    """Base class for fail-closed validation errors."""


class LsnError(FlipbenchError):
    pass


class OffsetError(FlipbenchError):
    pass


class ManifestError(FlipbenchError):
    pass


class StateError(FlipbenchError):
    pass


class TimingError(FlipbenchError):
    pass


_LSN_PATTERN = re.compile(r"^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$")
_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_MAX_U64 = (1 << 64) - 1
_MAX_KAFKA_OFFSET = (1 << 63) - 1


def parse_lsn(value: str) -> int:
    if not isinstance(value, str) or not _LSN_PATTERN.fullmatch(value):
        raise LsnError(f"invalid PostgreSQL LSN: {value!r}")
    high_text, low_text = value.split("/", maxsplit=1)
    result = (int(high_text, 16) << 32) | int(low_text, 16)
    if result > _MAX_U64:
        raise LsnError(f"PostgreSQL LSN exceeds uint64: {value!r}")
    return result


def format_lsn(value: int) -> str:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _MAX_U64:
        raise LsnError(f"invalid uint64 LSN value: {value!r}")
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


@dataclass(frozen=True, slots=True)
class HotSourceIdentity:
    cell: str
    system_identifier: str
    database: str
    slot: str


def source_fence_satisfied(
    expected: HotSourceIdentity,
    observed: HotSourceIdentity,
    confirmed_lsn: str,
    fence_lsn: str,
) -> bool:
    if expected != observed:
        raise LsnError(f"source identity mismatch: expected={expected!r}, observed={observed!r}")
    return parse_lsn(confirmed_lsn) >= parse_lsn(fence_lsn)


@dataclass(frozen=True, slots=True, order=True)
class TopicPartition:
    topic: str
    partition: int

    def __post_init__(self) -> None:
        if not self.topic or not isinstance(self.topic, str):
            raise OffsetError("topic must be a non-empty string")
        if not isinstance(self.partition, int) or isinstance(self.partition, bool) or self.partition < 0:
            raise OffsetError("partition must be a non-negative integer")

    @property
    def key(self) -> str:
        return f"{self.topic}:{self.partition}"


@dataclass(frozen=True, slots=True)
class OffsetVector:
    cell: str
    timeslot: str
    attempt_epoch: int
    values: Mapping[TopicPartition, int]

    def __post_init__(self) -> None:
        if not self.cell or not self.timeslot:
            raise OffsetError("cell and timeslot are required")
        if not isinstance(self.attempt_epoch, int) or isinstance(self.attempt_epoch, bool) or self.attempt_epoch <= 0:
            raise OffsetError("attempt_epoch must be a positive integer")
        copied = dict(self.values)
        for partition, offset in copied.items():
            if not isinstance(partition, TopicPartition):
                raise OffsetError("offset keys must be TopicPartition values")
            if not isinstance(offset, int) or isinstance(offset, bool) or not 0 <= offset <= _MAX_KAFKA_OFFSET:
                raise OffsetError(f"invalid next offset for {partition.key}: {offset!r}")
        object.__setattr__(self, "values", MappingProxyType(copied))


@dataclass(frozen=True, slots=True)
class OffsetGateResult:
    ready: bool
    missing: tuple[TopicPartition, ...]
    behind: Mapping[TopicPartition, int]


def offset_gate(
    manifest: Sequence[TopicPartition],
    target: OffsetVector,
    current: OffsetVector,
) -> OffsetGateResult:
    required = tuple(manifest)
    if not required or len(set(required)) != len(required):
        raise OffsetError("offset manifest must be non-empty and unique")
    if (target.cell, target.timeslot, target.attempt_epoch) != (
        current.cell,
        current.timeslot,
        current.attempt_epoch,
    ):
        raise OffsetError("target and current offset vectors belong to different attempts")
    target_missing = tuple(partition for partition in required if partition not in target.values)
    if target_missing:
        raise OffsetError(f"persisted target vector is missing: {[item.key for item in target_missing]}")
    missing = tuple(
        partition
        for partition in required
        if partition not in current.values and target.values[partition] > 0
    )
    behind = {
        partition: target.values[partition] - current.values[partition]
        for partition in required
        if partition in current.values and current.values[partition] < target.values[partition]
    }
    return OffsetGateResult(not missing and not behind, missing, MappingProxyType(behind))


@dataclass(frozen=True, slots=True)
class TableRoute:
    parent: str
    leaf: str
    topic: str
    sink_connector: str
    sink_group: str
    partition: int = 0


@dataclass(frozen=True, slots=True)
class BenchmarkManifest:
    cell: str
    timeslot: str
    topic_prefix: str
    tables: tuple[TableRoute, ...]


@dataclass(frozen=True, slots=True)
class LeafFenceMarker:
    partition: TopicPartition
    parent: str
    leaf: str
    cell: str
    timeslot: str
    marker_id: uuid.UUID
    attempt_id: uuid.UUID
    attempt_epoch: int

    def __post_init__(self) -> None:
        if any(
            _IDENTIFIER_PATTERN.fullmatch(value) is None
            for value in (self.parent, self.leaf, self.cell, self.timeslot)
        ):
            raise ManifestError("leaf fence marker contains an unsafe table identifier")
        if not isinstance(self.marker_id, uuid.UUID) or not isinstance(
            self.attempt_id, uuid.UUID
        ):
            raise ManifestError("leaf fence marker IDs must be UUIDs")
        if (
            not isinstance(self.attempt_epoch, int)
            or isinstance(self.attempt_epoch, bool)
            or self.attempt_epoch <= 0
        ):
            raise ManifestError("leaf fence marker attempt_epoch must be positive")


def build_manifest(table_count: int, cell: str, timeslot: str) -> BenchmarkManifest:
    if table_count not in (5, 10, 15, 20):
        raise ManifestError("table_count must be one of 5, 10, 15, or 20")
    if not _IDENTIFIER_PATTERN.fullmatch(cell) or not _IDENTIFIER_PATTERN.fullmatch(timeslot):
        raise ManifestError("cell and timeslot must be safe PostgreSQL identifiers")
    prefix = f"cards.{cell}"
    routes = tuple(
        TableRoute(
            parent=f"bench_table_{index:02d}",
            leaf=f"bench_table_{index:02d}_p_{timeslot}",
            topic=f"{prefix}.public.bench_table_{index:02d}_p_{timeslot}",
            sink_connector="flipbench-sink",
            sink_group="connect-flipbench-sink",
        )
        for index in range(1, table_count + 1)
    )
    manifest = BenchmarkManifest(cell, timeslot, prefix, routes)
    validate_manifest(manifest)
    return manifest


def build_leaf_fence_markers(
    manifest: BenchmarkManifest,
    attempt_id: uuid.UUID,
    attempt_epoch: int,
) -> tuple[LeafFenceMarker, ...]:
    validate_manifest(manifest)
    if not isinstance(attempt_id, uuid.UUID):
        raise ManifestError("leaf fence attempt_id must be a UUID")
    if (
        not isinstance(attempt_epoch, int)
        or isinstance(attempt_epoch, bool)
        or attempt_epoch <= 0
    ):
        raise ManifestError("leaf fence attempt_epoch must be positive")
    return tuple(
        LeafFenceMarker(
            partition=TopicPartition(route.topic, route.partition),
            parent=route.parent,
            leaf=route.leaf,
            cell=manifest.cell,
            timeslot=manifest.timeslot,
            marker_id=uuid.uuid5(attempt_id, f"flipbench-leaf-fence-v1:{route.leaf}"),
            attempt_id=attempt_id,
            attempt_epoch=attempt_epoch,
        )
        for route in manifest.tables
    )


def validate_manifest(manifest: BenchmarkManifest) -> None:
    if not _IDENTIFIER_PATTERN.fullmatch(manifest.cell) or not _IDENTIFIER_PATTERN.fullmatch(manifest.timeslot):
        raise ManifestError("manifest cell and timeslot must be safe identifiers")
    if manifest.topic_prefix != f"cards.{manifest.cell}":
        raise ManifestError("manifest topic prefix must be derived from its cell")
    if len(manifest.tables) not in (5, 10, 15, 20):
        raise ManifestError("manifest table count must be one of 5, 10, 15, or 20")
    values = {
        "parents": [route.parent for route in manifest.tables],
        "leaves": [route.leaf for route in manifest.tables],
        "topics": [route.topic for route in manifest.tables],
    }
    for label, items in values.items():
        if len(items) != len(set(items)):
            raise ManifestError(f"duplicate {label} in manifest")
    for route in manifest.tables:
        if not _IDENTIFIER_PATTERN.fullmatch(route.parent) or not _IDENTIFIER_PATTERN.fullmatch(route.leaf):
            raise ManifestError(f"unsafe table route: {route!r}")
        if route.partition != 0:
            raise ManifestError("the first prototype requires exactly Kafka partition 0")
        if not route.leaf.startswith(f"{route.parent}_p_{manifest.timeslot}"):
            raise ManifestError(f"leaf does not match parent/timeslot: {route.leaf}")
        if route.topic != f"{manifest.topic_prefix}.public.{route.leaf}":
            raise ManifestError(f"topic does not exactly match leaf identity: {route.topic}")
        if not route.sink_connector or not route.sink_group:
            raise ManifestError("sink connector and group names must be non-empty")
        if route.sink_connector != "flipbench-sink" or route.sink_group != "connect-flipbench-sink":
            raise ManifestError("all routes must use the shared sink connector and consumer group")


def canonical_manifest_json(manifest: BenchmarkManifest) -> str:
    payload = asdict(manifest)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class AttemptState(StrEnum):
    HOT_PRIMARY = "hot_primary"
    LOCKED = "locked"
    DRAINED = "drained"
    WARM_PRIMARY = "warm_primary"
    RECOVERING = "recovering"


@dataclass(frozen=True, slots=True)
class GateEvidence:
    all_tables_detached: bool
    source_fence_reached: bool
    target_offsets_frozen: bool
    sink_offsets_reached: bool

    @property
    def complete(self) -> bool:
        return all(
            (
                self.all_tables_detached,
                self.source_fence_reached,
                self.target_offsets_frozen,
                self.sink_offsets_reached,
            )
        )


def transition(
    current: AttemptState,
    requested: AttemptState,
    evidence: GateEvidence | None = None,
) -> AttemptState:
    allowed = {
        AttemptState.HOT_PRIMARY: {AttemptState.LOCKED},
        AttemptState.LOCKED: {AttemptState.DRAINED, AttemptState.RECOVERING},
        AttemptState.DRAINED: {AttemptState.WARM_PRIMARY, AttemptState.RECOVERING},
        AttemptState.WARM_PRIMARY: set(),
        AttemptState.RECOVERING: set(),
    }
    if requested not in allowed[current]:
        raise StateError(f"forbidden transition: {current.value} -> {requested.value}")
    if requested == AttemptState.DRAINED and (evidence is None or not evidence.complete):
        raise StateError("drained requires detach, source, frozen target, and sink proofs")
    return requested


@dataclass(frozen=True, slots=True)
class QueueLatencyEstimate:
    source_catchup_seconds: float
    warm_catchup_seconds: float
    pre_fence_seconds: float
    source_ready_seconds: float
    total_seconds: float


def _finite_non_negative(name: str, value: float) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise TimingError(f"{name} must be finite and non-negative")
    return result


def estimate_queue_latency(
    source_backlog: float,
    sink_backlog: float,
    source_capacity: float,
    warm_capacity: float,
    live_rate: float,
    tracker_seconds: float,
    detach_seconds: Sequence[float],
    fence_seconds: float,
    source_visibility_seconds: float,
    capture_seconds: float,
    sink_visibility_seconds: float,
    grant_seconds: float,
) -> QueueLatencyEstimate:
    values = {
        name: _finite_non_negative(name, value)
        for name, value in (
            ("source_backlog", source_backlog),
            ("sink_backlog", sink_backlog),
            ("source_capacity", source_capacity),
            ("warm_capacity", warm_capacity),
            ("live_rate", live_rate),
            ("tracker_seconds", tracker_seconds),
            ("fence_seconds", fence_seconds),
            ("source_visibility_seconds", source_visibility_seconds),
            ("capture_seconds", capture_seconds),
            ("sink_visibility_seconds", sink_visibility_seconds),
            ("grant_seconds", grant_seconds),
        )
    }
    detach = tuple(_finite_non_negative("detach_seconds", value) for value in detach_seconds)
    source_headroom = values["source_capacity"] - values["live_rate"]
    warm_headroom = values["warm_capacity"] - values["live_rate"]
    if source_headroom <= 0 or warm_headroom <= 0:
        raise TimingError("source and warm net catch-up headroom must both be positive")
    source_catchup = values["source_backlog"] / source_headroom
    warm_catchup = (values["source_backlog"] + values["sink_backlog"]) / warm_headroom
    pre_fence = values["tracker_seconds"] + sum(detach) + values["fence_seconds"]
    source_ready = max(pre_fence, source_catchup) + values["source_visibility_seconds"]
    total = (
        max(source_ready, warm_catchup)
        + values["capture_seconds"]
        + values["sink_visibility_seconds"]
        + values["grant_seconds"]
    )
    return QueueLatencyEstimate(source_catchup, warm_catchup, pre_fence, source_ready, total)


def derive_stage_durations(timestamps_ns: Mapping[str, int]) -> Mapping[str, int]:
    ordered = ("t1", "t2", "t5", "t7", "t8", "t11", "t13")
    if any(name not in timestamps_ns for name in ordered):
        raise TimingError(f"missing stage timestamp; required={ordered}")
    values = tuple(timestamps_ns[name] for name in ordered)
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values):
        raise TimingError("stage timestamps must be non-negative monotonic nanoseconds")
    if any(right < left for left, right in zip(values, values[1:])):
        raise TimingError("monotonic stage timestamps moved backwards")
    result = {
        "tracker_lock_ns": timestamps_ns["t2"] - timestamps_ns["t1"],
        "source_proof_ns": timestamps_ns["t7"] - timestamps_ns["t5"],
        "capture_e_ns": timestamps_ns["t8"] - timestamps_ns["t7"],
        "sink_proof_ns": timestamps_ns["t11"] - timestamps_ns["t8"],
        "grant_ns": timestamps_ns["t13"] - timestamps_ns["t11"],
        "writer_park_ns": timestamps_ns["t13"] - timestamps_ns["t2"],
        "whole_attempt_ns": timestamps_ns["t13"] - timestamps_ns["t1"],
    }
    if "t6w" in timestamps_ns:
        wakeup_bounds = (
            timestamps_ns.get("t6"),
            timestamps_ns["t6w"],
            timestamps_ns["t7"],
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in wakeup_bounds
        ) or not wakeup_bounds[0] <= wakeup_bounds[1] <= wakeup_bounds[2]:
            raise TimingError("fence wake-up timestamps must satisfy t6 <= t6w <= t7")
        result["fence_wakeup_ns"] = wakeup_bounds[1] - wakeup_bounds[0]
        result["slot_wait_after_wakeup_ns"] = wakeup_bounds[2] - wakeup_bounds[1]
    if "t2h" in timestamps_ns:
        hot_fence_closed = timestamps_ns["t2h"]
        if (
            not isinstance(hot_fence_closed, int)
            or isinstance(hot_fence_closed, bool)
            or not timestamps_ns["t2"] <= hot_fence_closed <= timestamps_ns["t13"]
        ):
            raise TimingError("hot fence timestamp must satisfy t2 <= t2h <= t13")
        result["hot_fence_park_ns"] = hot_fence_closed - timestamps_ns["t2"]
        tracker_locked = timestamps_ns.get("t2w")
        if (
            not isinstance(tracker_locked, int)
            or isinstance(tracker_locked, bool)
            or not hot_fence_closed <= tracker_locked <= timestamps_ns["t13"]
        ):
            raise TimingError("hot fence tracker timestamp must satisfy t2h <= t2w <= t13")
        result["tracker_lock_ns"] = tracker_locked - hot_fence_closed
        result["admission_to_park_ns"] = timestamps_ns["t2"] - timestamps_ns["t1"]
        admission_stopped = timestamps_ns.get("t2f")
        if admission_stopped is not None:
            in_flight_resolved = timestamps_ns.get("t2q")
            if (
                not isinstance(admission_stopped, int)
                or isinstance(admission_stopped, bool)
                or not tracker_locked <= admission_stopped <= timestamps_ns["t5"]
                or not isinstance(in_flight_resolved, int)
                or isinstance(in_flight_resolved, bool)
                or not admission_stopped <= in_flight_resolved <= timestamps_ns["t5"]
            ):
                raise TimingError(
                    "optimistic detach timestamps must satisfy t2w <= t2f <= t2q <= t5"
                )
            result["admission_fence_ns"] = admission_stopped - tracker_locked
            result["in_flight_resolution_ns"] = in_flight_resolved - admission_stopped
    if "tverify" in timestamps_ns:
        verify = timestamps_ns["tverify"]
        if not isinstance(verify, int) or isinstance(verify, bool) or verify < timestamps_ns["t13"]:
            raise TimingError("post-grant verification must be a non-negative timestamp at or after t13")
        result["validation_ns"] = verify - timestamps_ns["t13"]
        result["whole_lifecycle_ns"] = verify - timestamps_ns["t1"]
    return MappingProxyType(result)


def derive_revert_durations(timestamps_ns: Mapping[str, int]) -> Mapping[str, int]:
    required = ("t2", "trevert_start", "trevert_end")
    if any(name not in timestamps_ns for name in required):
        raise TimingError(f"missing revert timestamp; required={required}")
    locked, revert_start, revert_end = (timestamps_ns[name] for name in required)
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in (locked, revert_start, revert_end)
    ) or not locked <= revert_start <= revert_end:
        raise TimingError("revert timestamps must be non-negative and monotonic")
    return MappingProxyType(
        {
            "forward_until_failure_ns": revert_start - locked,
            "revert_ns": revert_end - revert_start,
            "writer_park_ns": revert_end - locked,
        }
    )


def lag_admission_ready(
    source_lag_bytes: int,
    sink_lag_records: int,
    min_source_lag_bytes: int,
    min_sink_lag_records: int,
) -> bool:
    values = {
        "source_lag_bytes": source_lag_bytes,
        "sink_lag_records": sink_lag_records,
        "min_source_lag_bytes": min_source_lag_bytes,
        "min_sink_lag_records": min_sink_lag_records,
    }
    if any(not isinstance(value, int) or isinstance(value, bool) for value in values.values()):
        raise TimingError("lag admission values must be integers")
    if source_lag_bytes < 0 or sink_lag_records < 0:
        raise TimingError("observed lag cannot be negative")
    if min_source_lag_bytes <= 0 or min_sink_lag_records <= 0:
        raise TimingError("lag admission minimums must be positive")
    return source_lag_bytes >= min_source_lag_bytes and sink_lag_records >= min_sink_lag_records


def production_admission_ready(
    source_lag_bytes: int,
    sink_lag_records_by_partition: Mapping[str, int],
    max_source_lag_bytes: int,
    max_sink_lag_records_per_partition: int,
) -> bool:
    values = (source_lag_bytes, max_source_lag_bytes, max_sink_lag_records_per_partition)
    if any(not isinstance(value, int) or isinstance(value, bool) for value in values):
        raise TimingError("production lag bounds must be integers")
    if source_lag_bytes < 0 or max_source_lag_bytes < 0 or max_sink_lag_records_per_partition < 0:
        raise TimingError("production lag values must be non-negative")
    if not sink_lag_records_by_partition:
        raise TimingError("production sink lag vector must be non-empty")
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in sink_lag_records_by_partition.values()
    ):
        raise TimingError("production sink lag values must be non-negative integers")
    return (
        source_lag_bytes <= max_source_lag_bytes
        and max(sink_lag_records_by_partition.values()) <= max_sink_lag_records_per_partition
    )


def overload_lag_vectors(
    baseline_end_offsets: Mapping[str, int],
    current_end_offsets: Mapping[str, int],
    current_committed_offsets: Mapping[str, int],
    committed_hot_rows: Mapping[str, int],
) -> tuple[Mapping[str, int], Mapping[str, int]]:
    keys = set(baseline_end_offsets)
    if not keys or set(current_end_offsets) != keys or set(current_committed_offsets) != keys or set(committed_hot_rows) != keys:
        raise TimingError("overload lag vectors must have the same non-empty keyset")
    for label, vector in (
        ("baseline_end_offsets", baseline_end_offsets),
        ("current_end_offsets", current_end_offsets),
        ("current_committed_offsets", current_committed_offsets),
        ("committed_hot_rows", committed_hot_rows),
    ):
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in vector.values()):
            raise TimingError(f"{label} values must be non-negative integers")
    source: dict[str, int] = {}
    sink: dict[str, int] = {}
    for key in sorted(keys):
        produced = current_end_offsets[key] - baseline_end_offsets[key]
        source_backlog = committed_hot_rows[key] - produced
        sink_backlog = current_end_offsets[key] - current_committed_offsets[key]
        if produced < 0 or source_backlog < 0 or sink_backlog < 0:
            raise TimingError(f"overload offsets regressed or exceeded committed hot rows for {key}")
        source[key] = source_backlog
        sink[key] = sink_backlog
    return MappingProxyType(source), MappingProxyType(sink)
