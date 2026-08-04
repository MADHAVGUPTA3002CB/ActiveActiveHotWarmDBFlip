#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from flipbench.matrix import (  # noqa: E402
    MatrixCase,
    MatrixObservation,
    build_full_matrix,
    summarize_observations,
)
from flipbench.results import validate_result  # noqa: E402
from run_tps_matrix import (  # noqa: E402
    API,
    MatrixAbort,
    TERMINAL_FLIP_STATES,
    api_state,
    request_json,
    restart_environment,
    stop_workload_if_possible,
)


QUEUE_LIMIT = 5_000
LOW_ADMISSION_PERCENT = 5
WARMUP_SECONDS = 25
MEASUREMENT_SECONDS = 20
FINAL_HEALTHY_SAMPLES = 5


def require_contract(
    label: str,
    observed: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> None:
    mismatches = {
        key: {"expected": value, "observed": observed.get(key)}
        for key, value in expected.items()
        if observed.get(key) != value
    }
    if mismatches:
        raise RuntimeError(f"{label} configuration drift: {mismatches}")


def prepare_output_dir(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"matrix output directory already exists: {path}")
    path.mkdir(parents=True)


def as_matrix_abort(error: BaseException, case_id: str) -> MatrixAbort:
    if isinstance(error, MatrixAbort):
        return error
    return MatrixAbort(
        f"case {case_id} ended ambiguously; refusing the next destructive reset: "
        f"{type(error).__name__}: {error}"
    )


def configure_case(case: MatrixCase) -> None:
    workload_payload = {
        "mode": "target_rate_v1",
        "write_fence_mode": case.write_fence_mode,
        "active_target_tps": case.active_target_tps,
        "retiring_target_tps": case.retiring_target_tps,
        "active_rows_per_transaction": 1,
        "retiring_rows_per_transaction": 1,
        "active_workers": 32,
        "retiring_workers": 8,
        "max_queue_size": QUEUE_LIMIT,
        "rate_window_seconds": 5,
        "min_achievement_percent": LOW_ADMISSION_PERCENT,
        "payload_bytes": 256,
    }
    observed_workload = request_json(
        API,
        "/workload",
        method="PATCH",
        payload=workload_payload,
    )
    if not isinstance(observed_workload, Mapping):
        raise RuntimeError("workload PATCH returned no settings")
    require_contract("workload", observed_workload, workload_payload)
    threshold_payload = {
        "max_source_lag_bytes": 512 * 1024 * 1024,
        "max_sink_lag_records_per_partition": 250_000,
        "stable_samples": 6,
        "poll_ms": 50,
        "park_budget_ms": 120_000,
        "revert_reserve_ms": 20_000,
    }
    observed_thresholds = request_json(
        API,
        "/thresholds",
        method="PATCH",
        payload=threshold_payload,
    )
    if not isinstance(observed_thresholds, Mapping):
        raise RuntimeError("threshold PATCH returned no settings")
    require_contract("threshold", observed_thresholds, threshold_payload)


def _observation(state: Mapping[str, Any]) -> tuple[MatrixObservation, dict[str, Any]]:
    latest = state.get("latest")
    if not isinstance(latest, Mapping):
        raise RuntimeError("control API has no current workload sample")
    transactions = latest.get("transactions")
    if not isinstance(transactions, Mapping):
        raise RuntimeError("control API has no transaction metrics")
    active = transactions.get("active")
    retiring = transactions.get("retiring")
    if not isinstance(active, Mapping) or not isinstance(retiring, Mapping):
        raise RuntimeError("control API transaction lanes are incomplete")
    scheduled = int(active.get("scheduled_transactions") or 0) + int(
        retiring.get("scheduled_transactions") or 0
    )
    rejected = int(active.get("rejected_transactions") or 0) + int(
        retiring.get("rejected_transactions") or 0
    )
    queue = int(active.get("queue_depth") or 0) + int(retiring.get("queue_depth") or 0)
    observation = MatrixObservation(
        achieved_tps=float(transactions.get("achieved_tps") or 0),
        active_tps=float(active.get("committed_tps") or 0),
        retiring_tps=float(retiring.get("committed_tps") or 0),
        scheduled_transactions=scheduled,
        rejected_transactions=rejected,
        queue_depth=queue,
    )
    raw = {
        "at": latest.get("at"),
        "admission_ready": latest.get("admission_ready"),
        "healthy_samples": latest.get("healthy_samples"),
        "source_lag_bytes": latest.get("source_lag_bytes"),
        "source_lag_bytes_by_lane": latest.get("source_lag_bytes_by_lane"),
        "max_sink_lag_records": latest.get("max_sink_lag_records"),
        "achieved_tps": observation.achieved_tps,
        "active_tps": observation.active_tps,
        "retiring_tps": observation.retiring_tps,
        "scheduled_transactions": scheduled,
        "rejected_transactions": rejected,
        "queue_depth": queue,
        "active_p95_ms": active.get("latency_p95_ms"),
        "retiring_p95_ms": retiring.get("latency_p95_ms"),
        "rate_valid": transactions.get("rate_valid"),
        "metrics_error": state.get("metrics_error"),
        "connector_states": state.get("connectors"),
    }
    return observation, raw


def collect_preflip_window(case: MatrixCase) -> tuple[list[dict[str, Any]], Mapping[str, Any]]:
    print(f"    warmup: {WARMUP_SECONDS}s", flush=True)
    deadline = time.monotonic() + WARMUP_SECONDS
    while time.monotonic() < deadline:
        state = api_state()
        workload = state.get("workload")
        if not isinstance(workload, Mapping) or not workload.get("running"):
            raise RuntimeError(f"workload stopped during warmup: {state.get('metrics_error')}")
        time.sleep(1)

    print(f"    measurement: {MEASUREMENT_SECONDS}s", flush=True)
    raw_window: list[dict[str, Any]] = []
    observations: list[MatrixObservation] = []
    for second in range(MEASUREMENT_SECONDS):
        state = api_state()
        observation, raw = _observation(state)
        if raw["metrics_error"] is not None:
            raise RuntimeError(f"metrics error during measurement: {raw['metrics_error']}")
        observations.append(observation)
        raw_window.append(raw)
        if second % 5 == 0 or second == MEASUREMENT_SECONDS - 1:
            print(
                f"    sample {second + 1:02d}: achieved={observation.achieved_tps:,.1f} "
                f"queue={observation.queue_depth:,} rejected={observation.rejected_transactions:,}",
                flush=True,
            )
        time.sleep(1)
    final = raw_window[-FINAL_HEALTHY_SAMPLES:]
    if len(final) != FINAL_HEALTHY_SAMPLES or any(
        item.get("admission_ready") is not True
        or item.get("rate_valid") is not True
        or item.get("metrics_error") is not None
        for item in final
    ):
        raise RuntimeError("the final low-floor health samples did not admit a diagnostic flip")
    summary = summarize_observations(
        observations,
        target_tps=case.target_tps,
        queue_limit=QUEUE_LIMIT * 2,
    )
    return raw_window, summary


def _verify_and_copy_result(
    case: MatrixCase,
    generation_id: str,
    run_id: str,
    experiment_dir: Path,
) -> tuple[str, str]:
    source = Path("results") / run_id / "run.json"
    deadline = time.monotonic() + 30
    while not source.exists() and time.monotonic() < deadline:
        time.sleep(0.25)
    if not source.exists():
        raise MatrixAbort(f"saved run artifact is missing for {run_id}")
    payload = json.loads(source.read_text(encoding="utf-8"))
    validate_result(payload)
    scenario = payload.get("scenario")
    scenario = scenario if isinstance(scenario, Mapping) else {}
    workload = scenario.get("workload_settings")
    workload = workload if isinstance(workload, Mapping) else {}
    topology = payload.get("topology")
    topology = topology if isinstance(topology, Mapping) else {}
    expected_identity = {
        "run_id": run_id,
        "generation": generation_id,
        "topology": case.source_topology,
        "wakeup": case.fence_wakeup_mode,
        "fence": case.write_fence_mode,
        "active_tps": case.active_target_tps,
        "retiring_tps": case.retiring_target_tps,
    }
    observed_identity = {
        "run_id": payload.get("run_id"),
        "generation": payload.get("environment_generation_id"),
        "topology": topology.get("source_topology"),
        "wakeup": scenario.get("fence_wakeup_mode"),
        "fence": scenario.get("write_fence_mode"),
        "active_tps": workload.get("active_target_tps"),
        "retiring_tps": workload.get("retiring_target_tps"),
    }
    require_contract("saved run identity", observed_identity, expected_identity)
    require_contract(
        "saved workload",
        workload,
        {
            "mode": "target_rate_v1",
            "write_fence_mode": case.write_fence_mode,
            "active_target_tps": case.active_target_tps,
            "retiring_target_tps": case.retiring_target_tps,
            "active_rows_per_transaction": 1,
            "retiring_rows_per_transaction": 1,
            "active_workers": 32,
            "retiring_workers": 8,
            "max_queue_size": QUEUE_LIMIT,
            "rate_window_seconds": 5,
            "min_achievement_percent": LOW_ADMISSION_PERCENT,
            "payload_bytes": 256,
        },
    )
    require_contract(
        "saved flip thresholds",
        scenario,
        {
            "max_source_lag_bytes": 512 * 1024 * 1024,
            "max_sink_lag_records_per_partition": 250_000,
            "required_stable_samples": 6,
            "park_budget_ms": 120_000,
            "revert_reserve_ms": 20_000,
            "forward_budget_ms": 100_000,
        },
    )
    destination = experiment_dir / "raw" / case.case_id / "run.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    shutil.copyfile(source, temporary)
    temporary.replace(destination)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    return str(destination.relative_to(experiment_dir)), digest


def run_case(case: MatrixCase, experiment_dir: Path, generations: set[str]) -> dict[str, Any]:
    if shutil.disk_usage(Path.cwd()).free < 25 * 1024**3:
        raise MatrixAbort("less than 25 GiB free disk remains before a fresh case")
    print(
        f"\n[{case.ordinal:02d}/17 {case.variant} @ {case.target_tps:,} TPS] "
        f"resetting {case.source_topology} RF3",
        flush=True,
    )
    started_at = datetime.now(timezone.utc).isoformat()
    generation_id = restart_environment(case.source_topology)
    if generation_id in generations:
        raise MatrixAbort(f"environment generation was reused: {generation_id}")
    generations.add(generation_id)
    configure_case(case)
    request_json(API, "/workload/start", method="POST", payload={})
    raw_window, capacity = collect_preflip_window(case)
    print(
        f"    capacity={capacity['capacity_label']} median={capacity['median_achieved_tps']:,.1f} "
        f"({capacity['achievement_percent']:.1f}%), starting flip",
        flush=True,
    )
    request_json(
        API,
        "/flip/start",
        method="POST",
        payload={"fence_wakeup_mode": case.fence_wakeup_mode},
    )
    deadline = time.monotonic() + 180
    terminal: Mapping[str, Any] | None = None
    while time.monotonic() < deadline:
        state = api_state()
        flip = state.get("flip")
        status = flip.get("status") if isinstance(flip, Mapping) else None
        if status in TERMINAL_FLIP_STATES:
            terminal = flip
            break
        time.sleep(1)
    if terminal is None:
        raise MatrixAbort("flip did not reach a terminal state within 180 seconds")
    stop_workload_if_possible()
    run_id = str(terminal.get("run_id"))
    raw_path, raw_sha256 = _verify_and_copy_result(
        case, generation_id, run_id, experiment_dir
    )
    durations = terminal.get("durations_ns")
    durations = dict(durations) if isinstance(durations, Mapping) else {}
    print(
        f"    flip={terminal.get('status')} writer_park="
        f"{float(durations.get('writer_park_ns') or 0) / 1_000_000:,.1f}ms",
        flush=True,
    )
    return {
        "case_id": case.case_id,
        "ordinal": case.ordinal,
        "variant": case.variant,
        "target_tps": case.target_tps,
        "active_target_tps": case.active_target_tps,
        "retiring_target_tps": case.retiring_target_tps,
        "source_topology": case.source_topology,
        "fence_wakeup_mode": case.fence_wakeup_mode,
        "write_fence_mode": case.write_fence_mode,
        "environment_generation_id": generation_id,
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "capacity": dict(capacity),
        "preflip_observations": raw_window,
        "flip_status": terminal.get("status"),
        "run_id": run_id,
        "outcome": terminal.get("outcome"),
        "verification_outcome": terminal.get("verification_outcome"),
        "durations_ns": durations,
        "error": terminal.get("error"),
        "raw_result_path": raw_path,
        "raw_result_sha256": raw_sha256,
    }


def save_matrix(path: Path, started_at: str, cases: list[dict[str, Any]]) -> None:
    payload = {
        "schema_version": 1,
        "experiment": "full-a-b-bplus-d-tps-matrix",
        "started_at_utc": started_at,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": {"platform": platform.platform(), "single_host": True},
        "constants": {
            "table_count": 5,
            "kafka_replication_factor": 3,
            "rows_per_transaction": 1,
            "active_retiring_split": "90:10",
            "active_workers": 32,
            "retiring_workers": 8,
            "queue_limit_per_lane": QUEUE_LIMIT,
            "payload_bytes": 256,
            "rate_window_seconds": 5,
            "low_flip_admission_percent_per_lane": LOW_ADMISSION_PERCENT,
            "warmup_seconds": WARMUP_SECONDS,
            "measurement_seconds": MEASUREMENT_SECONDS,
            "max_source_lag_bytes": 512 * 1024 * 1024,
            "max_sink_lag_records_per_partition": 250_000,
            "park_budget_ms": 120_000,
            "revert_reserve_ms": 20_000,
        },
        "cases": cases,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm-reset", required=True)
    parser.add_argument("--output-dir", type=Path)
    arguments = parser.parse_args()
    if arguments.confirm_reset != "RESET":
        parser.error("--confirm-reset must be RESET exactly")
    started_at = datetime.now(timezone.utc).isoformat()
    output_dir = arguments.output_dir or Path("results") / (
        f"full-tps-matrix-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    )
    try:
        prepare_output_dir(output_dir)
    except FileExistsError as error:
        parser.error(str(error))
    matrix_path = output_dir / "matrix.json"
    cases: list[dict[str, Any]] = []
    generations: set[str] = set()
    save_matrix(matrix_path, started_at, cases)
    try:
        for case in build_full_matrix():
            try:
                result = run_case(case, output_dir, generations)
            except Exception as error:
                stop_workload_if_possible()
                result = {
                    "case_id": case.case_id,
                    "ordinal": case.ordinal,
                    "variant": case.variant,
                    "target_tps": case.target_tps,
                    "flip_status": "harness_error",
                    "error": f"{type(error).__name__}: {error}"[:4000],
                    "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                print(f"    ERROR: {result['error']}", flush=True)
                cases.append(result)
                save_matrix(matrix_path, started_at, cases)
                raise as_matrix_abort(error, case.case_id) from error
            cases.append(result)
            save_matrix(matrix_path, started_at, cases)
    finally:
        stop_workload_if_possible()
        save_matrix(matrix_path, started_at, cases)
    print(f"\nFull matrix saved to {matrix_path}", flush=True)


if __name__ == "__main__":
    main()
