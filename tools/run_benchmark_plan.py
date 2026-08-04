#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from flipbench.benchmark_plan import (  # noqa: E402
    BenchmarkCase,
    BenchmarkPlan,
    BenchmarkPlanError,
    build_benchmark_cases,
    load_benchmark_plan,
    render_benchmark_report,
)
from flipbench.matrix import MatrixObservation, summarize_observations  # noqa: E402
from flipbench.results import validate_result  # noqa: E402
from run_tps_matrix import (  # noqa: E402
    API,
    SUPERVISOR,
    MatrixAbort,
    TERMINAL_FLIP_STATES,
    api_state,
    request_json,
    stop_workload_if_possible,
)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _require_contract(
    label: str,
    observed: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> None:
    mismatches = {
        key: {"expected": expected_value, "observed": observed.get(key)}
        for key, expected_value in expected.items()
        if observed.get(key) != expected_value
    }
    if mismatches:
        raise RuntimeError(f"{label} configuration drift: {mismatches}")


def _restart_environment(plan: BenchmarkPlan, case: BenchmarkCase) -> str:
    started = request_json(
        SUPERVISOR,
        "/environment/restart",
        method="POST",
        payload={
            "table_count": plan.table_count,
            "source_topology": case.source_topology,
            "confirmation": "RESET",
        },
    )
    if not isinstance(started, Mapping) or not isinstance(started.get("job_id"), str):
        raise MatrixAbort("restart supervisor returned no job identity")
    job_id = started["job_id"]
    deadline = time.monotonic() + plan.timing.restart_timeout_seconds
    last_phase: object = None
    while time.monotonic() < deadline:
        current = request_json(SUPERVISOR, "/state")
        if not isinstance(current, Mapping):
            raise MatrixAbort("restart supervisor state is malformed")
        if current.get("job_id") != job_id:
            raise MatrixAbort("restart supervisor job identity changed")
        if current.get("phase") != last_phase:
            print(f"    reset: {current.get('phase')}", flush=True)
            last_phase = current.get("phase")
        if current.get("status") == "completed":
            generation = current.get("environment_generation_id")
            if not isinstance(generation, str) or not generation:
                raise MatrixAbort("completed restart has no generation identity")
            return generation
        if current.get("status") == "failed":
            raise MatrixAbort(f"environment restart failed: {current.get('error')}")
        time.sleep(2)
    raise MatrixAbort("environment restart exceeded its configured timeout")


def _workload_payload(plan: BenchmarkPlan, case: BenchmarkCase) -> dict[str, Any]:
    return {
        "mode": "target_rate_v1",
        "write_fence_mode": case.write_fence_mode,
        "active_target_tps": case.active_target_tps,
        "retiring_target_tps": case.retiring_target_tps,
        "active_rows_per_transaction": plan.workload.rows_per_transaction,
        "retiring_rows_per_transaction": plan.workload.rows_per_transaction,
        "active_workers": plan.workload.active_workers,
        "retiring_workers": plan.workload.retiring_workers,
        "max_queue_size": plan.workload.queue_limit_per_lane,
        "rate_window_seconds": plan.workload.rate_window_seconds,
        "min_achievement_percent": plan.workload.minimum_achievement_percent,
        "payload_bytes": plan.workload.payload_bytes,
    }


def _threshold_payload(plan: BenchmarkPlan) -> dict[str, Any]:
    return {
        "max_source_lag_bytes": plan.admission.max_source_lag_bytes,
        "max_sink_lag_records_per_partition": (
            plan.admission.max_sink_lag_records_per_partition
        ),
        "stable_samples": plan.admission.stable_samples,
        "poll_ms": plan.admission.poll_ms,
        "park_budget_ms": plan.admission.park_budget_ms,
        "revert_reserve_ms": plan.admission.revert_reserve_ms,
    }


def _flip_payload(case: BenchmarkCase) -> dict[str, str]:
    return {
        "fence_wakeup_mode": case.fence_wakeup_mode,
        "source_proof_mode": case.source_proof_mode,
    }


def configure_case(plan: BenchmarkPlan, case: BenchmarkCase) -> None:
    workload = _workload_payload(plan, case)
    observed_workload = request_json(
        API, "/workload", method="PATCH", payload=workload
    )
    if not isinstance(observed_workload, Mapping):
        raise RuntimeError("workload PATCH returned no settings")
    _require_contract("workload", observed_workload, workload)
    thresholds = _threshold_payload(plan)
    observed_thresholds = request_json(
        API, "/thresholds", method="PATCH", payload=thresholds
    )
    if not isinstance(observed_thresholds, Mapping):
        raise RuntimeError("threshold PATCH returned no settings")
    _require_contract("threshold", observed_thresholds, thresholds)


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
    queue_depth = int(active.get("queue_depth") or 0) + int(
        retiring.get("queue_depth") or 0
    )
    observation = MatrixObservation(
        achieved_tps=float(transactions.get("achieved_tps") or 0),
        active_tps=float(active.get("committed_tps") or 0),
        retiring_tps=float(retiring.get("committed_tps") or 0),
        scheduled_transactions=scheduled,
        rejected_transactions=rejected,
        queue_depth=queue_depth,
    )
    return observation, {
        "at": latest.get("at"),
        "admission_ready": latest.get("admission_ready"),
        "healthy_samples": latest.get("healthy_samples"),
        "rate_valid": transactions.get("rate_valid"),
        "source_lag_bytes": latest.get("source_lag_bytes"),
        "source_lag_bytes_by_lane": latest.get("source_lag_bytes_by_lane"),
        "max_sink_lag_records": latest.get("max_sink_lag_records"),
        "achieved_tps": observation.achieved_tps,
        "active_tps": observation.active_tps,
        "retiring_tps": observation.retiring_tps,
        "scheduled_transactions": scheduled,
        "rejected_transactions": rejected,
        "queue_depth": queue_depth,
        "active_p95_ms": active.get("latency_p95_ms"),
        "retiring_p95_ms": retiring.get("latency_p95_ms"),
        "metrics_error": state.get("metrics_error"),
        "connector_states": state.get("connectors"),
    }


def collect_preflip_window(
    plan: BenchmarkPlan, case: BenchmarkCase
) -> tuple[list[dict[str, Any]], Mapping[str, Any]]:
    print(f"    warmup: {plan.timing.warmup_seconds}s", flush=True)
    warmup_deadline = time.monotonic() + plan.timing.warmup_seconds
    while time.monotonic() < warmup_deadline:
        state = api_state()
        workload = state.get("workload")
        if not isinstance(workload, Mapping) or workload.get("running") is not True:
            raise RuntimeError(f"workload stopped during warmup: {state.get('metrics_error')}")
        time.sleep(min(plan.timing.sample_interval_seconds, max(0.01, warmup_deadline - time.monotonic())))

    sample_count = (
        plan.timing.measurement_seconds // plan.timing.sample_interval_seconds
    )
    print(
        f"    measurement: {sample_count} samples every "
        f"{plan.timing.sample_interval_seconds}s",
        flush=True,
    )
    raw_window: list[dict[str, Any]] = []
    observations: list[MatrixObservation] = []
    for index in range(sample_count):
        time.sleep(plan.timing.sample_interval_seconds)
        state = api_state()
        observation, raw = _observation(state)
        if raw["metrics_error"] is not None:
            raise RuntimeError(f"metrics error during measurement: {raw['metrics_error']}")
        observations.append(observation)
        raw_window.append(raw)
        print(
            f"    sample {index + 1:02d}/{sample_count:02d}: "
            f"achieved={observation.achieved_tps:,.1f} "
            f"queue={observation.queue_depth:,}",
            flush=True,
        )
    final = raw_window[-plan.timing.final_healthy_samples :]
    if any(
        item.get("admission_ready") is not True
        or item.get("rate_valid") is not True
        or item.get("metrics_error") is not None
        for item in final
    ):
        raise RuntimeError("final configured health samples did not admit the flip")
    capacity = summarize_observations(
        observations,
        target_tps=case.target_tps,
        queue_limit=plan.workload.queue_limit_per_lane * 2,
    )
    return raw_window, capacity


def _copy_verified_result(
    plan: BenchmarkPlan,
    case: BenchmarkCase,
    generation_id: str,
    run_id: str,
    output_dir: Path,
) -> tuple[str, str, Mapping[str, Any]]:
    source = Path("results") / run_id / "run.json"
    deadline = time.monotonic() + 30
    while not source.is_file() and time.monotonic() < deadline:
        time.sleep(0.25)
    if not source.is_file() or source.is_symlink():
        raise MatrixAbort(f"saved result is missing or unsafe for run {run_id}")
    if source.stat().st_size > 10 * 1024 * 1024:
        raise MatrixAbort(f"saved result is unexpectedly large for run {run_id}")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise MatrixAbort("saved result is not an object")
    validate_result(payload)
    scenario = payload.get("scenario")
    scenario = scenario if isinstance(scenario, Mapping) else {}
    workload = scenario.get("workload_settings")
    workload = workload if isinstance(workload, Mapping) else {}
    topology = payload.get("topology")
    topology = topology if isinstance(topology, Mapping) else {}
    _require_contract(
        "saved result identity",
        {
            "run_id": payload.get("run_id"),
            "generation": payload.get("environment_generation_id"),
            "topology": topology.get("source_topology"),
            "wakeup": scenario.get("fence_wakeup_mode"),
            "fence": scenario.get("write_fence_mode"),
            "proof": scenario.get("source_proof_mode"),
            "tables_per_api_transaction": scenario.get("tables_per_api_transaction"),
            "operations_per_api_batch": scenario.get("operations_per_api_batch"),
            "ownership_reads_per_api_batch": scenario.get(
                "ownership_reads_per_api_batch"
            ),
            "postgres_transactions_per_api_batch": scenario.get(
                "postgres_transactions_per_api_batch"
            ),
            "api_batch_scheduling": scenario.get("api_batch_scheduling"),
        },
        {
            "run_id": run_id,
            "generation": generation_id,
            "topology": case.source_topology,
            "wakeup": case.fence_wakeup_mode,
            "fence": case.write_fence_mode,
            "proof": case.source_proof_mode,
            "tables_per_api_transaction": case.tables_per_api_transaction,
            "operations_per_api_batch": case.operations_per_api_batch,
            "ownership_reads_per_api_batch": case.ownership_reads_per_api_batch,
            "postgres_transactions_per_api_batch": case.operations_per_api_batch,
            "api_batch_scheduling": "single_worker_reserved_v1",
        },
    )
    _require_contract("saved workload", workload, _workload_payload(plan, case))
    thresholds = _threshold_payload(plan)
    _require_contract(
        "saved thresholds",
        scenario,
        {
            "max_source_lag_bytes": thresholds["max_source_lag_bytes"],
            "max_sink_lag_records_per_partition": thresholds[
                "max_sink_lag_records_per_partition"
            ],
            "required_stable_samples": thresholds["stable_samples"],
            "park_budget_ms": thresholds["park_budget_ms"],
            "revert_reserve_ms": thresholds["revert_reserve_ms"],
            "forward_budget_ms": thresholds["park_budget_ms"]
            - thresholds["revert_reserve_ms"],
        },
    )
    destination = output_dir / "raw" / case.case_id / "run.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(".run.json.tmp")
    shutil.copyfile(source, temporary)
    temporary.replace(destination)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    return str(destination.relative_to(output_dir)), digest, payload


def run_case(
    plan: BenchmarkPlan,
    case: BenchmarkCase,
    output_dir: Path,
    generations: set[str],
) -> dict[str, Any]:
    required_disk = plan.safety.minimum_free_disk_gib * 1024**3
    if shutil.disk_usage(Path.cwd()).free < required_disk:
        raise MatrixAbort(
            f"less than {plan.safety.minimum_free_disk_gib} GiB free disk remains"
        )
    print(
        f"\n[{case.ordinal:03d}/{plan.case_count:03d}] r{case.repetition} "
        f"{case.variant} @ {case.target_tps:,} TPS "
        f"({case.transaction_shape})",
        flush=True,
    )
    started_at = datetime.now(timezone.utc).isoformat()
    generation = _restart_environment(plan, case)
    if generation in generations:
        raise MatrixAbort(f"environment generation was reused: {generation}")
    generations.add(generation)
    configure_case(plan, case)
    request_json(API, "/workload/start", method="POST", payload={})
    raw_window, capacity = collect_preflip_window(plan, case)
    request_json(
        API,
        "/flip/start",
        method="POST",
        payload=_flip_payload(case),
    )
    deadline = time.monotonic() + plan.timing.flip_timeout_seconds
    terminal: Mapping[str, Any] | None = None
    while time.monotonic() < deadline:
        state = api_state()
        flip = state.get("flip")
        if isinstance(flip, Mapping) and flip.get("status") in TERMINAL_FLIP_STATES:
            terminal = flip
            break
        time.sleep(1)
    if terminal is None:
        raise MatrixAbort("flip did not reach a terminal state within its timeout")
    stop_workload_if_possible()
    run_id = terminal.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise MatrixAbort("terminal flip has no run identity")
    raw_path, raw_sha256, saved = _copy_verified_result(
        plan, case, generation, run_id, output_dir
    )
    durations = saved.get("durations_ns")
    durations = dict(durations) if isinstance(durations, Mapping) else {}
    result = {
        "case_id": case.case_id,
        "ordinal": case.ordinal,
        "repetition": case.repetition,
        "variant": case.variant,
        "target_tps": case.target_tps,
        "active_target_tps": case.active_target_tps,
        "retiring_target_tps": case.retiring_target_tps,
        "source_topology": case.source_topology,
        "fence_wakeup_mode": case.fence_wakeup_mode,
        "write_fence_mode": case.write_fence_mode,
        "source_proof_mode": case.source_proof_mode,
        "transaction_shape": case.transaction_shape,
        "tables_per_api_transaction": case.tables_per_api_transaction,
        "operations_per_api_batch": case.operations_per_api_batch,
        "ownership_reads_per_api_batch": case.ownership_reads_per_api_batch,
        "postgres_transactions_per_api_batch": case.operations_per_api_batch,
        "estimated_physical_rows_per_api_transaction": (
            plan.workload.rows_per_transaction * case.tables_per_api_transaction
        ),
        "environment_generation_id": generation,
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "capacity": dict(capacity),
        "preflip_observations": raw_window,
        "flip_status": terminal.get("status"),
        "run_id": run_id,
        "outcome": saved.get("outcome"),
        "verification_outcome": saved.get("verification_outcome"),
        "durations_ns": durations,
        "error": saved.get("error"),
        "raw_result_path": raw_path,
        "raw_result_sha256": raw_sha256,
    }
    print(
        f"    flip={result['flip_status']} capacity={capacity['capacity_label']} "
        f"writer_park={float(durations.get('writer_park_ns') or 0) / 1_000_000:.1f}ms",
        flush=True,
    )
    return result


def _save_outputs(
    output_dir: Path,
    plan: BenchmarkPlan,
    started_at: str,
    cases: list[dict[str, Any]],
) -> None:
    manifest = {
        "schema_version": 1,
        "experiment": "configuration-driven-hot-warm-benchmark",
        "plan_sha256": plan.sha256,
        "plan": plan.to_dict(),
        "started_at_utc": started_at,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "single_host": True,
        },
        "cases": cases,
    }
    _atomic_json(output_dir / "matrix.json", manifest)
    _atomic_text(
        output_dir / "report.md",
        render_benchmark_report(plan, cases, plan.sha256),
    )


def _print_plan(plan: BenchmarkPlan) -> None:
    print(
        json.dumps(
            {
                "name": plan.name,
                "plan_sha256": plan.sha256,
                "case_count": plan.case_count,
                "cases": [
                    {
                        "case_id": case.case_id,
                        "repetition": case.repetition,
                        "variant": case.variant,
                        "target_tps": case.target_tps,
                        "source_topology": case.source_topology,
                        "write_fence_mode": case.write_fence_mode,
                        "source_proof_mode": case.source_proof_mode,
                        "transaction_shape": case.transaction_shape,
                        "tables_per_api_transaction": case.tables_per_api_transaction,
                        "operations_per_api_batch": case.operations_per_api_batch,
                        "ownership_reads_per_api_batch": case.ownership_reads_per_api_batch,
                        "postgres_transactions_per_api_batch": case.operations_per_api_batch,
                    }
                    for case in build_benchmark_cases(plan)
                ],
            },
            indent=2,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a validated hot-to-warm benchmark plan"
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--confirm-reset")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args()
    try:
        plan = load_benchmark_plan(arguments.plan)
    except BenchmarkPlanError as error:
        parser.error(str(error))
    if arguments.dry_run:
        _print_plan(plan)
        return
    if arguments.confirm_reset != "RESET":
        parser.error("--confirm-reset must be RESET exactly")
    output_dir = arguments.output_dir or Path("results") / (
        f"benchmark-{plan.name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    )
    if output_dir.exists():
        parser.error(f"output directory already exists: {output_dir}")
    output_dir.mkdir(parents=True)
    started_at = datetime.now(timezone.utc).isoformat()
    completed: list[dict[str, Any]] = []
    generations: set[str] = set()
    _save_outputs(output_dir, plan, started_at, completed)
    try:
        for case in build_benchmark_cases(plan):
            try:
                result = run_case(plan, case, output_dir, generations)
            except Exception as error:
                stop_workload_if_possible()
                failed = {
                    "case_id": case.case_id,
                    "ordinal": case.ordinal,
                    "repetition": case.repetition,
                    "variant": case.variant,
                    "target_tps": case.target_tps,
                    "transaction_shape": case.transaction_shape,
                    "tables_per_api_transaction": case.tables_per_api_transaction,
                    "operations_per_api_batch": case.operations_per_api_batch,
                    "ownership_reads_per_api_batch": case.ownership_reads_per_api_batch,
                    "postgres_transactions_per_api_batch": case.operations_per_api_batch,
                    "flip_status": "harness_error",
                    "error": f"{type(error).__name__}: {error}"[:4000],
                    "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                completed.append(failed)
                _save_outputs(output_dir, plan, started_at, completed)
                raise MatrixAbort(
                    f"case {case.case_id} ended ambiguously; later resets are blocked"
                ) from error
            completed.append(result)
            _save_outputs(output_dir, plan, started_at, completed)
            correctness_failed = (
                result["flip_status"] == "verification_failed"
                or result["verification_outcome"] == "failed"
            )
            if result["flip_status"] == "failed" or result["outcome"] == "failed":
                raise MatrixAbort(
                    f"case {case.case_id} ended without verified safe recovery"
                )
            if correctness_failed and plan.safety.stop_on_correctness_failure:
                raise MatrixAbort(
                    f"case {case.case_id} failed correctness verification"
                )
    finally:
        stop_workload_if_possible()
        _save_outputs(output_dir, plan, started_at, completed)
    print(f"\nBenchmark results: {output_dir / 'report.md'}", flush=True)


if __name__ == "__main__":
    main()
