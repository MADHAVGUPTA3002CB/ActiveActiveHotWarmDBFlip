from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Mapping

from .results import validate_ownership_checkpoint, validate_result


def _non_negative_integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _non_negative_number(value: object) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return None
    return value


def summarize_result(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        run_id = str(uuid.UUID(str(payload["run_id"])))
    except (KeyError, ValueError, AttributeError) as error:
        raise ValueError("saved result requires a valid run_id") from error
    artifact_type = str(payload.get("artifact_type", "completed_run"))
    if artifact_type not in ("ownership_grant", "completed_run"):
        raise ValueError("unknown saved-result artifact type")
    table_count = payload.get("table_count")
    if table_count not in (5, 10, 15, 20):
        raise ValueError("saved result has an unsupported table count")
    durations = payload.get("durations_ns")
    admission = payload.get("admission")
    durations = durations if isinstance(durations, Mapping) else {}
    admission = admission if isinstance(admission, Mapping) else {}
    outcome = str(payload.get("outcome", payload.get("ownership_outcome", "unknown")))
    if outcome not in ("success", "failed", "reverted"):
        outcome = "failed"
    verification = str(payload.get("verification_outcome", "pending"))
    if verification not in ("pending", "passed", "failed", "not_run"):
        verification = "not_run"
    recorded_at = payload.get("finished_at_utc", payload.get("recorded_at_utc"))
    if not isinstance(recorded_at, str) or len(recorded_at) > 64:
        recorded_at = None
    profile = payload.get("profile")
    if not isinstance(profile, str) or len(profile) > 128:
        profile = "unknown"
    scenario = payload.get("scenario")
    scenario = scenario if isinstance(scenario, Mapping) else {}
    workload_mode = scenario.get("workload_mode")
    if workload_mode not in ("legacy_batch", "target_rate_v1"):
        workload_mode = None
    workload_settings = scenario.get("workload_settings")
    workload_settings = workload_settings if isinstance(workload_settings, Mapping) else {}
    write_fence_mode = scenario.get(
        "write_fence_mode", workload_settings.get("write_fence_mode")
    )
    if write_fence_mode not in (
        "warm_tracker_advisory_v1",
        "hot_transactional_v1",
        "optimistic_detach_v1",
    ):
        write_fence_mode = "legacy/unknown"
    transaction_shape = scenario.get("transaction_shape")
    tables_per_transaction = scenario.get("tables_per_api_transaction")
    optimistic_contract = scenario.get("optimistic_contract_version")
    if (
        write_fence_mode == "optimistic_detach_v1"
        and optimistic_contract == "state_only_batch_first_write_admission_v4"
        and transaction_shape == "api_batch_separate_commits_v1"
    ):
        pass
    elif (
        write_fence_mode == "optimistic_detach_v1"
        and optimistic_contract == "reserved_batch_first_write_admission_v3"
        and transaction_shape == "api_batch_separate_commits_v1"
    ):
        pass
    elif (
        write_fence_mode == "optimistic_detach_v1"
        and optimistic_contract == "batch_admission_separate_commits_v1"
    ):
        transaction_shape = "legacy_batch_admission_extra_transaction"
    elif (
        write_fence_mode == "optimistic_detach_v1"
        and optimistic_contract == "batch_first_write_admission_v2"
    ):
        transaction_shape = "legacy_unreserved_batch_scheduler"
    elif write_fence_mode == "optimistic_detach_v1" and tables_per_transaction == table_count:
        transaction_shape = "legacy_all_tables_api"
    elif write_fence_mode == "optimistic_detach_v1" and tables_per_transaction == 1:
        transaction_shape = "legacy_per_transaction_gate_api"
    elif transaction_shape in ("single_table_api", "api_batch_separate_commits_v1"):
        pass
    elif write_fence_mode == "hot_transactional_v1" and tables_per_transaction == 1:
        transaction_shape = "single_table_api"
    else:
        transaction_shape = "legacy/unknown"
    active_target = _non_negative_integer(workload_settings.get("active_target_tps"))
    retiring_target = _non_negative_integer(workload_settings.get("retiring_target_tps"))
    target_tps = (
        active_target + retiring_target
        if active_target is not None and retiring_target is not None
        else None
    )
    achieved_tps = None
    stable_window = scenario.get("stable_window")
    if isinstance(stable_window, list):
        for sample in reversed(stable_window):
            if not isinstance(sample, Mapping):
                continue
            transactions = sample.get("transactions")
            if isinstance(transactions, Mapping):
                achieved_tps = _non_negative_number(transactions.get("achieved_tps"))
                if achieved_tps is not None:
                    break
    topology = payload.get("topology")
    topology = topology if isinstance(topology, Mapping) else {}
    source_topology = (
        payload.get("source_topology", topology.get("source_topology"))
        if payload.get("schema_version") in (2, 3, 4, 5, 6, 7)
        else None
    )
    if source_topology not in ("shared", "isolated"):
        source_topology = "legacy/unknown"
    generation_id = payload.get(
        "environment_generation_id", scenario.get("environment_generation_id")
    )
    if not isinstance(generation_id, str) or len(generation_id) > 64:
        generation_id = None
    attempt_id = payload.get("attempt_id")
    try:
        attempt_id = str(uuid.UUID(str(attempt_id)))
    except (ValueError, AttributeError):
        attempt_id = None
    identity = payload.get("hot_identity")
    identity = identity if isinstance(identity, Mapping) else {}
    fence_wakeup = payload.get("fence_wakeup")
    fence_wakeup = fence_wakeup if isinstance(fence_wakeup, Mapping) else {}
    marker_fence = payload.get("marker_fence")
    marker_fence = marker_fence if isinstance(marker_fence, Mapping) else {}
    atomic_timings = marker_fence.get("atomic_transaction_ns_by_leaf")
    atomic_detach_marker_ns = (
        sum(atomic_timings.values())
        if isinstance(atomic_timings, Mapping)
        and atomic_timings
        and all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in atomic_timings.values()
        )
        else None
    )
    parallel_wall_duration = marker_fence.get("parallel_wall_duration_ns")
    parallel_detach_wall_ns = (
        parallel_wall_duration
        if isinstance(parallel_wall_duration, int)
        and not isinstance(parallel_wall_duration, bool)
        and parallel_wall_duration >= 0
        else None
    )
    fence_wakeup_mode = fence_wakeup.get("mode")
    if fence_wakeup_mode not in ("passive", "immediate_heartbeat"):
        fence_wakeup_mode = "legacy/unknown"
    fence_wakeup_applied = fence_wakeup.get("applied")
    if not isinstance(fence_wakeup_applied, bool):
        fence_wakeup_applied = None
    cell = payload.get("cell", identity.get("cell"))
    timeslot = payload.get("timeslot")
    source_proof_mode = scenario.get("source_proof_mode")
    if source_proof_mode not in (
        "slot_lsn_v1",
        "per_leaf_marker_v1",
        "atomic_detach_marker_v1",
        "parallel_atomic_detach_marker_v1",
    ):
        source_proof_mode = "slot_lsn_v1" if payload.get("schema_version") in (1, 2, 3, 4) else "legacy/unknown"
    optimistic_admission_check_mode = scenario.get(
        "optimistic_admission_check_mode"
    )
    if optimistic_admission_check_mode not in (
        "state_and_epoch_v1",
        "state_only_v1",
    ):
        optimistic_admission_check_mode = "legacy/unknown"
    return {
        "artifact_type": artifact_type,
        "run_id": run_id,
        "attempt_id": attempt_id,
        "recorded_at_utc": recorded_at,
        "outcome": outcome,
        "verification_outcome": verification,
        "table_count": table_count,
        "profile": profile,
        "environment_generation_id": generation_id,
        "source_topology": source_topology,
        "fence_wakeup_mode": fence_wakeup_mode,
        "fence_wakeup_applied": fence_wakeup_applied,
        "source_proof_mode": source_proof_mode,
        "cell": cell if isinstance(cell, str) and len(cell) <= 64 else None,
        "timeslot": timeslot if isinstance(timeslot, str) and len(timeslot) <= 64 else None,
        "historical_saved_run": True,
        "workload_mode": workload_mode,
        "write_fence_mode": write_fence_mode,
        "optimistic_admission_check_mode": optimistic_admission_check_mode,
        "transaction_shape": transaction_shape,
        "operations_per_api_batch": _non_negative_integer(
            scenario.get("operations_per_api_batch")
        ),
        "ownership_reads_per_api_batch": _non_negative_integer(
            scenario.get("ownership_reads_per_api_batch")
        ),
        "ownership_epoch_checks_per_api_batch": _non_negative_integer(
            scenario.get("ownership_epoch_checks_per_api_batch")
        ),
        "postgres_transactions_per_api_batch": _non_negative_integer(
            scenario.get("postgres_transactions_per_api_batch")
        ),
        "target_tps": target_tps,
        "achieved_tps": achieved_tps,
        "tracker_lock_ns": _non_negative_integer(durations.get("tracker_lock_ns")),
        "hot_fence_park_ns": _non_negative_integer(
            durations.get("hot_fence_park_ns")
        ),
        "admission_fence_ns": _non_negative_integer(
            durations.get("admission_fence_ns")
        ),
        "in_flight_resolution_ns": _non_negative_integer(
            durations.get("in_flight_resolution_ns")
        ),
        "source_proof_ns": _non_negative_integer(durations.get("source_proof_ns")),
        "atomic_detach_marker_ns": atomic_detach_marker_ns,
        "parallel_detach_wall_ns": parallel_detach_wall_ns,
        "fence_wakeup_ns": _non_negative_integer(durations.get("fence_wakeup_ns")),
        "slot_wait_after_wakeup_ns": _non_negative_integer(
            durations.get("slot_wait_after_wakeup_ns")
        ),
        "capture_e_ns": _non_negative_integer(durations.get("capture_e_ns")),
        "sink_proof_ns": _non_negative_integer(durations.get("sink_proof_ns")),
        "grant_ns": _non_negative_integer(durations.get("grant_ns")),
        "forward_until_failure_ns": _non_negative_integer(
            durations.get("forward_until_failure_ns")
        ),
        "revert_ns": _non_negative_integer(durations.get("revert_ns")),
        "writer_park_ns": _non_negative_integer(durations.get("writer_park_ns")),
        "whole_attempt_ns": _non_negative_integer(
            durations.get("whole_attempt_ns", durations.get("forward_until_failure_ns"))
        ),
        "whole_lifecycle_ns": _non_negative_integer(durations.get("whole_lifecycle_ns")),
        "validation_ns": _non_negative_integer(durations.get("validation_ns")),
        "source_lag_bytes": _non_negative_integer(admission.get("source_lag_bytes")),
        "sink_lag_records": _non_negative_integer(admission.get("sink_lag_records")),
    }


def load_saved_runs(
    results_dir: Path,
    limit: int = 50,
    max_file_bytes: int = 10 * 1024 * 1024,
) -> list[dict[str, Any]]:
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
        raise ValueError("saved-run limit must be between 1 and 500")
    if not results_dir.exists():
        return []
    candidates: list[tuple[int, Path]] = []
    for run_dir in results_dir.iterdir():
        if not run_dir.is_dir() or run_dir.is_symlink():
            continue
        artifacts = tuple(
            artifact
            for artifact in (run_dir / "run.json", run_dir / "ownership-grant.json")
            if artifact.is_file() and not artifact.is_symlink()
        )
        if not artifacts:
            continue
        try:
            stat = max((artifact.stat() for artifact in artifacts), key=lambda item: item.st_mtime_ns)
        except OSError:
            continue
        candidates.append((stat.st_mtime_ns, run_dir))
    summaries: list[dict[str, Any]] = []
    for _, run_dir in sorted(candidates, reverse=True)[:limit]:
        for artifact in (run_dir / "run.json", run_dir / "ownership-grant.json"):
            try:
                if artifact.is_symlink() or not artifact.is_file() or artifact.stat().st_size > max_file_bytes:
                    continue
                payload = json.loads(artifact.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    continue
                if artifact.name == "run.json":
                    validate_result(payload)
                else:
                    validate_ownership_checkpoint(payload)
                summaries.append(summarize_result(payload))
                break
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                continue
    return summaries
