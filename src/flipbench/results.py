from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Mapping


class ResultValidationError(ValueError):
    pass


def validate_result(payload: Mapping[str, Any]) -> None:
    required = {
        "schema_version",
        "run_id",
        "attempt_id",
        "outcome",
        "profile",
        "table_count",
        "stages_ns",
        "detach_ns_by_table",
        "target_next_offsets",
        "final_committed_next_offsets",
        "error",
    }
    missing = required.difference(payload)
    if missing:
        raise ResultValidationError(f"result is missing fields: {sorted(missing)}")
    try:
        uuid.UUID(str(payload["run_id"]))
        uuid.UUID(str(payload["attempt_id"]))
    except (ValueError, AttributeError) as error:
        raise ResultValidationError("run_id and attempt_id must be UUIDs") from error
    table_count = payload["table_count"]
    if table_count not in (5, 10, 15, 20):
        raise ResultValidationError("table_count must be 5, 10, 15, or 20")
    outcome = payload["outcome"]
    if outcome not in ("success", "failed", "reverted"):
        raise ResultValidationError("outcome must be success, failed, or reverted")
    if outcome in ("failed", "reverted"):
        if not isinstance(payload["error"], Mapping):
            raise ResultValidationError("failed and reverted results require an error object")
        return
    if payload["schema_version"] != 1:
        raise ResultValidationError("successful result schema_version must be 1")
    if payload["profile"] not in (
        "local-rf1-paused-backlog",
        "local-rf1-running",
        "local-rf1-healthy-overload",
        "local-rf1-production-shaped",
        "local-rf3-single-host-production-shaped",
    ):
        raise ResultValidationError("successful result has an unknown benchmark profile")
    attempt_epoch = payload.get("attempt_epoch")
    if not isinstance(attempt_epoch, int) or isinstance(attempt_epoch, bool) or attempt_epoch <= 0:
        raise ResultValidationError("successful result requires a positive attempt_epoch")
    if payload["error"] is not None:
        raise ResultValidationError("successful results cannot contain an error")
    stages = payload["stages_ns"]
    required_stages = {
        "t0", "t1", "t2", "t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "tverify"
    }
    requires_quiescence_stage = payload["profile"] in (
        "local-rf1-healthy-overload",
        "local-rf1-production-shaped",
        "local-rf3-single-host-production-shaped",
    )
    if requires_quiescence_stage:
        required_stages.add("t2q")
    required_stages.update({f"t3_{index}" for index in range(1, table_count + 1)})
    required_stages.update({f"t4_{index}" for index in range(1, table_count + 1)})
    if not required_stages.issubset(stages):
        raise ResultValidationError(f"successful result is missing stages: {sorted(required_stages.difference(stages))}")
    ordered_stages = ["t0", "t1", "t2"]
    if requires_quiescence_stage:
        ordered_stages.append("t2q")
    for index in range(1, table_count + 1):
        ordered_stages.extend((f"t3_{index}", f"t4_{index}"))
    ordered_stages.extend(("t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "tverify"))
    ordered_values = [stages[name] for name in ordered_stages]
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in ordered_values):
        raise ResultValidationError("successful result stage timestamps must be non-negative integers")
    if any(right < left for left, right in zip(ordered_values, ordered_values[1:])):
        raise ResultValidationError("successful result stage timestamps are not monotonic")
    if payload.get("ownership_outcome") != "success":
        raise ResultValidationError("successful result requires a successful ownership outcome")
    verification_outcome = payload.get("verification_outcome")
    if verification_outcome not in ("passed", "failed"):
        raise ResultValidationError("successful result requires a terminal verification outcome")
    expected_gc_eligible = verification_outcome == "passed"
    if payload.get("gc_eligible") is not expected_gc_eligible:
        raise ResultValidationError("GC eligibility must exactly match post-grant verification")
    if len(payload["detach_ns_by_table"]) != table_count:
        raise ResultValidationError("successful result must contain one detach duration per table")
    if len(payload["target_next_offsets"]) != table_count:
        raise ResultValidationError("successful result must contain the complete target offset vector")
    if len(payload["final_committed_next_offsets"]) != table_count:
        raise ResultValidationError("successful result must contain the complete committed offset vector")
    target = payload["target_next_offsets"]
    committed = payload["final_committed_next_offsets"]
    if set(target) != set(committed):
        raise ResultValidationError("target and committed offset vectors must have identical keys")
    max_offset = (1 << 63) - 1
    if any(
        not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= max_offset
        for value in (*target.values(), *committed.values())
    ):
        raise ResultValidationError("Kafka offsets must be non-negative signed 64-bit integers")
    if any(committed[key] < target[key] for key in target):
        raise ResultValidationError("successful result contains a committed offset behind its target")
    if payload["profile"] == "local-rf1-healthy-overload":
        scenario = payload.get("scenario")
        admission = payload.get("admission")
        if not isinstance(scenario, Mapping) or scenario.get("mode") != "healthy-overload":
            raise ResultValidationError("healthy-overload result requires scenario metadata")
        if not isinstance(admission, Mapping) or admission.get("connector_state") != "RUNNING":
            raise ResultValidationError("healthy-overload admission requires RUNNING connectors")
        min_source_bytes = scenario.get("min_source_lag_bytes")
        min_source_records = scenario.get("min_source_lag_records_per_partition")
        min_sink_records = scenario.get("min_sink_lag_records_per_partition")
        required_samples = scenario.get("required_stable_samples")
        max_admitted_rows = scenario.get("max_admitted_rows_per_partition")
        stable_window = scenario.get("stable_window")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in (
                min_source_bytes,
                min_source_records,
                min_sink_records,
                required_samples,
                max_admitted_rows,
            )
        ):
            raise ResultValidationError("healthy-overload thresholds must be positive integers")
        if not isinstance(stable_window, list) or len(stable_window) < required_samples:
            raise ResultValidationError("healthy-overload result lacks its stable admission window")
        source_lag_bytes = admission.get("source_lag_bytes")
        source_lag = admission.get("source_lag_records_by_partition")
        sink_lag = admission.get("sink_lag_by_partition")
        if not isinstance(source_lag_bytes, int) or source_lag_bytes < min_source_bytes:
            raise ResultValidationError("healthy-overload source lag is below its admission floor")
        if not isinstance(source_lag, Mapping) or set(source_lag) != set(target):
            raise ResultValidationError("healthy-overload source lag vector does not match the manifest")
        if any(not isinstance(value, int) or value < min_source_records for value in source_lag.values()):
            raise ResultValidationError("healthy-overload source record lag is below its per-partition floor")
        if not isinstance(sink_lag, Mapping) or set(sink_lag) != set(target):
            raise ResultValidationError("healthy-overload sink lag vector does not match the manifest")
        t1_min_sink_records = scenario.get("t1_min_sink_lag_records_per_partition")
        if (
            not isinstance(t1_min_sink_records, int)
            or isinstance(t1_min_sink_records, bool)
            or t1_min_sink_records <= 0
        ):
            raise ResultValidationError("healthy-overload t1 sink floor must be a positive integer")
        if any(
            not isinstance(value, int) or value < t1_min_sink_records
            for value in sink_lag.values()
        ):
            raise ResultValidationError("healthy-overload sink lag is below its t1 per-partition floor")
        if admission.get("writer_active_at_t1") is not True:
            raise ResultValidationError("healthy-overload writer was not active at t1")
        writer_inserted_at_t1 = admission.get("writer_inserted_at_t1")
        if (
            not isinstance(writer_inserted_at_t1, int)
            or writer_inserted_at_t1 <= 0
            or writer_inserted_at_t1 > max_admitted_rows * table_count
        ):
            raise ResultValidationError("healthy-overload workload exceeded its pre-lock admission cap")
        for sample in stable_window:
            if not isinstance(sample, Mapping):
                raise ResultValidationError("healthy-overload stable samples must be objects")
            sample_source = sample.get("source_lag_records_by_partition")
            sample_sink = sample.get("sink_lag_records_by_partition")
            if not isinstance(sample_source, Mapping) or set(sample_source) != set(target):
                raise ResultValidationError("stable source lag vector does not match the manifest")
            if not isinstance(sample_sink, Mapping) or set(sample_sink) != set(target):
                raise ResultValidationError("stable sink lag vector does not match the manifest")
            if any(value < min_source_records for value in sample_source.values()):
                raise ResultValidationError("stable source lag is below its per-partition floor")
            if any(value < min_sink_records for value in sample_sink.values()):
                raise ResultValidationError("stable sink lag is below its per-partition floor")
        workload_inserted_total = payload.get("workload_inserted_total")
        if (
            not isinstance(workload_inserted_total, int)
            or isinstance(workload_inserted_total, bool)
            or workload_inserted_total <= 0
        ):
            raise ResultValidationError("healthy-overload result requires a positive committed workload")
    if payload["profile"] in ("local-rf1-production-shaped", "local-rf3-single-host-production-shaped"):
        scenario = payload.get("scenario")
        admission = payload.get("admission")
        workload = payload.get("workload_by_timeslot")
        if not isinstance(scenario, Mapping) or scenario.get("mode") != "production-shaped":
            raise ResultValidationError("production-shaped result requires its scenario metadata")
        max_source_lag = scenario.get("max_source_lag_bytes")
        max_sink_lag = scenario.get("max_sink_lag_records_per_partition")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (max_source_lag, max_sink_lag)
        ):
            raise ResultValidationError("production-shaped lag ceilings must be non-negative integers")
        if not isinstance(admission, Mapping) or admission.get("connector_state") != "RUNNING":
            raise ResultValidationError("production-shaped admission requires RUNNING connectors")
        source_lag = admission.get("source_lag_bytes")
        sink_lag = admission.get("sink_lag_by_partition")
        if not isinstance(source_lag, int) or isinstance(source_lag, bool) or not 0 <= source_lag <= max_source_lag:
            raise ResultValidationError("production-shaped source lag exceeds its admission ceiling")
        if not isinstance(sink_lag, Mapping) or set(sink_lag) != set(payload["target_next_offsets"]):
            raise ResultValidationError("production-shaped sink lag vector does not match the manifest")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= max_sink_lag
            for value in sink_lag.values()
        ):
            raise ResultValidationError("production-shaped sink lag exceeds its per-partition ceiling")
        if not isinstance(workload, Mapping) or set(workload) != {"t1", "t2q", "t13"}:
            raise ResultValidationError("production-shaped result requires t1/t2q/t13 workload evidence")
        snapshots = tuple(workload[name] for name in ("t1", "t2q", "t13"))
        if any(not isinstance(snapshot, Mapping) for snapshot in snapshots):
            raise ResultValidationError("production-shaped workload snapshots must be objects")
        active = tuple(snapshot.get("active") for snapshot in snapshots)
        retiring = tuple(snapshot.get("retiring") for snapshot in snapshots)
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in (*active, *retiring)):
            raise ResultValidationError("production-shaped workload counters must be non-negative integers")
        if active[1] < active[0] or active[2] <= active[1]:
            raise ResultValidationError("active workload must make positive progress after retiring quiescence")
        if retiring[2] != retiring[1]:
            raise ResultValidationError("retiring workload advanced after writer quiescence")
        if snapshots[2].get("writer_alive") is not True:
            raise ResultValidationError("active writer was not alive at warm ownership grant")
    if payload.get("hot_identity") is None or not payload.get("fence_lsn"):
        raise ResultValidationError("successful result requires hot identity and fence LSN")


def validate_ownership_checkpoint(payload: Mapping[str, Any]) -> None:
    required = {
        "schema_version",
        "artifact_type",
        "non_authoritative",
        "run_id",
        "attempt_id",
        "attempt_epoch",
        "ownership_outcome",
        "verification_outcome",
        "table_count",
        "cell",
        "timeslot",
        "environment_generation_id",
        "stages_ns",
        "durations_ns",
        "target_next_offsets",
        "final_committed_next_offsets",
    }
    missing = required.difference(payload)
    if missing:
        raise ResultValidationError(f"ownership checkpoint is missing fields: {sorted(missing)}")
    if payload["artifact_type"] != "ownership_grant" or payload["non_authoritative"] is not True:
        raise ResultValidationError("ownership checkpoint must be non-authoritative history")
    if payload["schema_version"] != 1:
        raise ResultValidationError("ownership checkpoint schema_version must be 1")
    if payload["ownership_outcome"] != "success" or payload["verification_outcome"] != "pending":
        raise ResultValidationError("ownership checkpoint requires successful grant and pending verification")
    try:
        uuid.UUID(str(payload["run_id"]))
        uuid.UUID(str(payload["attempt_id"]))
    except (ValueError, AttributeError) as error:
        raise ResultValidationError("ownership checkpoint IDs must be UUIDs") from error
    if payload["table_count"] not in (5, 10, 15, 20):
        raise ResultValidationError("ownership checkpoint table_count is unsupported")
    attempt_epoch = payload["attempt_epoch"]
    if not isinstance(attempt_epoch, int) or isinstance(attempt_epoch, bool) or attempt_epoch <= 0:
        raise ResultValidationError("ownership checkpoint requires a positive attempt_epoch")
    if not isinstance(payload["cell"], str) or not 1 <= len(payload["cell"]) <= 64:
        raise ResultValidationError("ownership checkpoint requires a bounded cell")
    if not isinstance(payload["timeslot"], str) or not 1 <= len(payload["timeslot"]) <= 64:
        raise ResultValidationError("ownership checkpoint requires a bounded timeslot")
    generation_id = payload["environment_generation_id"]
    if generation_id != "legacy":
        try:
            uuid.UUID(str(generation_id))
        except (ValueError, AttributeError) as error:
            raise ResultValidationError(
                "ownership checkpoint environment_generation_id must be legacy or a UUID"
            ) from error
    stages = payload["stages_ns"]
    if not isinstance(stages, Mapping):
        raise ResultValidationError("ownership checkpoint stages_ns must be an object")
    t13 = stages.get("t13")
    if not isinstance(t13, int) or isinstance(t13, bool) or t13 < 0:
        raise ResultValidationError("ownership checkpoint requires a non-negative t13")
    durations = payload["durations_ns"]
    if not isinstance(durations, Mapping) or any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in durations.values()
    ):
        raise ResultValidationError("ownership checkpoint durations must be non-negative integers")
    targets = payload["target_next_offsets"]
    currents = payload["final_committed_next_offsets"]
    if not isinstance(targets, Mapping) or not isinstance(currents, Mapping):
        raise ResultValidationError("ownership checkpoint offset vectors must be objects")
    if set(targets) != set(currents):
        raise ResultValidationError("ownership checkpoint offset vectors must have identical keys")
    if len(targets) != payload["table_count"]:
        raise ResultValidationError("ownership checkpoint requires one offset per retiring table")
    for key, target in targets.items():
        current = currents[key]
        if (
            not isinstance(key, str)
            or not key.endswith(":0")
            or not isinstance(target, int)
            or isinstance(target, bool)
            or target < 0
            or not isinstance(current, int)
            or isinstance(current, bool)
            or current < target
        ):
            raise ResultValidationError(
                "ownership checkpoint offsets must be non-negative partition-0 integers at or beyond target"
            )


def _write_document_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    if path.parent.is_symlink() or path.is_symlink():
        raise ResultValidationError("result path must not contain a symlinked run artifact")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.is_symlink():
        raise ResultValidationError("result path must not contain a symlinked run artifact")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    validate_result(payload)
    _write_document_atomic(path, payload)


def write_ownership_checkpoint_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    validate_ownership_checkpoint(payload)
    _write_document_atomic(path, payload)
