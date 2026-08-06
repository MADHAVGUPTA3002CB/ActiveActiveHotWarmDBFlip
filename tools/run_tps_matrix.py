#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


API = "http://127.0.0.1:8090/api"
SUPERVISOR = "http://127.0.0.1:8091/api"
ORIGIN = "http://localhost:3000"
CASES = (
    ("A", "shared", "passive", 1_000),
    ("B", "isolated", "passive", 1_000),
    ("B+", "isolated", "immediate_heartbeat", 1_000),
    ("B", "isolated", "passive", 5_000),
    ("B+", "isolated", "immediate_heartbeat", 5_000),
    ("A", "shared", "passive", 5_000),
    ("B+", "isolated", "immediate_heartbeat", 15_000),
    ("A", "shared", "passive", 15_000),
    ("B", "isolated", "passive", 15_000),
)
TERMINAL_FLIP_STATES = {"succeeded", "reverted", "failed", "verification_failed"}


class MatrixAbort(RuntimeError):
    """A condition that makes later destructive resets unsafe."""


def request_json(
    base: str,
    path: str,
    *,
    method: str = "GET",
    payload: Mapping[str, Any] | None = None,
) -> Any:
    encoded = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{base}{path}",
        data=encoded,
        method=method,
        headers={
            "Accept": "application/json",
            "Origin": ORIGIN,
            **({"Content-Type": "application/json"} if encoded is not None else {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = json.loads(response.read())
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"{method} {path} returned HTTP {error.code}: {detail}") from error
    if not isinstance(body, Mapping) or body.get("ok") is not True:
        raise RuntimeError(f"{method} {path} returned an invalid response")
    return body.get("data")


def api_state() -> Mapping[str, Any]:
    state = request_json(API, "/state")
    if not isinstance(state, Mapping):
        raise RuntimeError("control API state is not an object")
    return state


def restart_environment(topology: str) -> str:
    state = request_json(
        SUPERVISOR,
        "/environment/restart",
        method="POST",
        payload={
            "table_count": 5,
            "source_topology": topology,
            "confirmation": "RESET",
        },
    )
    job_id = str(state["job_id"])
    deadline = time.monotonic() + 900
    last_phase = None
    while time.monotonic() < deadline:
        current = request_json(SUPERVISOR, "/state")
        phase = current.get("phase")
        if phase != last_phase:
            print(f"    reset: {phase}", flush=True)
            last_phase = phase
        if current.get("job_id") != job_id:
            raise MatrixAbort("supervisor restart job changed unexpectedly")
        if current.get("status") == "completed":
            return str(current["environment_generation_id"])
        if current.get("status") == "failed":
            raise MatrixAbort(f"environment restart failed: {current.get('error')}")
        time.sleep(2)
    raise MatrixAbort("environment restart exceeded 15 minutes")


def configure_workload(total_tps: int, variant: str) -> tuple[int, int]:
    active_tps = total_tps * 9 // 10
    retiring_tps = total_tps - active_tps
    request_json(
        API,
        "/workload",
        method="PATCH",
        payload={
            "mode": "target_rate_v1",
            "active_target_tps": active_tps,
            "retiring_target_tps": retiring_tps,
            "active_rows_per_transaction": 1,
            "retiring_rows_per_transaction": 1,
            "active_workers": 32,
            "retiring_workers": 8,
            "max_queue_size": 30_000,
            "rate_window_seconds": 5,
            "min_achievement_percent": 80,
            "payload_bytes": 256,
            "write_fence_mode": (
                "optimistic_detach_v1"
                if variant == "A"
                else "warm_tracker_advisory_v1"
            ),
            "optimistic_admission_check_mode": (
                "state_only_v1"
                if variant == "A"
                else "state_and_epoch_v1"
            ),
        },
    )
    request_json(
        API,
        "/thresholds",
        method="PATCH",
        payload={
            "max_source_lag_bytes": 64 * 1024 * 1024,
            "max_sink_lag_records_per_partition": 100,
            "stable_samples": 6,
            "poll_ms": 50,
            "park_budget_ms": 60_000,
            "revert_reserve_ms": 10_000,
        },
    )
    return active_tps, retiring_tps


def compact_sample(sample: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(sample, Mapping):
        return None
    transactions = sample.get("transactions")
    if not isinstance(transactions, Mapping):
        return None
    active = transactions.get("active")
    retiring = transactions.get("retiring")
    active = active if isinstance(active, Mapping) else {}
    retiring = retiring if isinstance(retiring, Mapping) else {}
    return {
        "at": sample.get("at"),
        "target_tps": transactions.get("target_tps"),
        "achieved_tps": transactions.get("achieved_tps"),
        "achievement_percent": transactions.get("achievement_percent"),
        "active_tps": sample.get("active_transactions_per_second"),
        "retiring_tps": sample.get("retiring_transactions_per_second"),
        "active_p95_ms": active.get("latency_p95_ms"),
        "retiring_p95_ms": retiring.get("latency_p95_ms"),
        "active_queue": active.get("queue_depth"),
        "retiring_queue": retiring.get("queue_depth"),
        "active_rejected": active.get("rejected_transactions"),
        "retiring_rejected": retiring.get("rejected_transactions"),
        "active_scheduled": active.get("scheduled_transactions"),
        "retiring_scheduled": retiring.get("scheduled_transactions"),
        "source_lag_bytes": sample.get("source_lag_bytes"),
        "source_lag_bytes_by_lane": sample.get("source_lag_bytes_by_lane"),
        "max_sink_lag_records": sample.get("max_sink_lag_records"),
        "sink_lag_records": sample.get("sink_lag_records"),
    }


def stop_workload_if_possible() -> None:
    try:
        state = api_state()
        if state.get("workload", {}).get("running") and state.get("flip", {}).get("status") != "running":
            request_json(API, "/workload/stop", method="POST", payload={})
    except Exception as error:
        print(f"    cleanup warning: {type(error).__name__}: {error}", flush=True)


def run_case(variant: str, topology: str, wakeup: str, total_tps: int) -> dict[str, Any]:
    if shutil.disk_usage(Path.cwd()).free < 25 * 1024**3:
        raise MatrixAbort("less than 25 GiB free disk remains; stopping the matrix")
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"\n[{variant} @ {total_tps:,} TPS] resetting {topology} topology", flush=True)
    generation_id = restart_environment(topology)
    active_tps, retiring_tps = configure_workload(total_tps, variant)
    request_json(API, "/workload/start", method="POST", payload={})
    print(
        f"    writes started: active={active_tps:,}, retiring={retiring_tps:,}, rows/tx=1",
        flush=True,
    )

    warmup_deadline = time.monotonic() + 20
    admission_deadline = time.monotonic() + (180 if total_tps == 15_000 else 90)
    peak: dict[str, Any] | None = None
    admitted: dict[str, Any] | None = None
    healthy_streak: list[dict[str, Any]] = []
    baseline_scheduled: int | None = None
    baseline_rejected: int | None = None
    last_progress = 0.0
    while time.monotonic() < admission_deadline:
        state = api_state()
        latest = state.get("latest")
        sample = compact_sample(latest if isinstance(latest, Mapping) else None)
        if sample is not None and (
            peak is None or float(sample.get("achieved_tps") or 0) > float(peak.get("achieved_tps") or 0)
        ):
            peak = sample
        transactions = latest.get("transactions") if isinstance(latest, Mapping) else None
        rate_valid = isinstance(transactions, Mapping) and transactions.get("rate_valid") is True
        rejected_total = 0 if sample is None else int(sample.get("active_rejected") or 0) + int(
            sample.get("retiring_rejected") or 0
        )
        scheduled_total = 0 if sample is None else int(sample.get("active_scheduled") or 0) + int(
            sample.get("retiring_scheduled") or 0
        )
        if time.monotonic() >= warmup_deadline and baseline_scheduled is None:
            baseline_scheduled = scheduled_total
            baseline_rejected = rejected_total
        rejected = rejected_total - (baseline_rejected or 0)
        scheduled = scheduled_total - (baseline_scheduled or 0)
        rejection_ratio = rejected / max(1, scheduled)
        healthy = (
            time.monotonic() >= warmup_deadline
            and isinstance(latest, Mapping)
            and latest.get("admission_ready") is True
            and rate_valid
            and rejection_ratio <= 0.05
            and sample is not None
        )
        healthy_streak = [*healthy_streak, sample][-10:] if healthy else []
        if len(healthy_streak) == 10:
            queue_totals = [
                int(item.get("active_queue") or 0) + int(item.get("retiring_queue") or 0)
                for item in healthy_streak
            ]
            continuously_growing = all(
                current > previous
                for previous, current in zip(queue_totals, queue_totals[1:])
            )
            if not continuously_growing:
                admitted = sample
                break
            healthy_streak = healthy_streak[-1:]
        now = time.monotonic()
        if now - last_progress >= 10:
            print(
                "    warmup: "
                f"achieved={None if sample is None else sample.get('achieved_tps')} TPS, "
                f"queue={None if sample is None else int(sample.get('active_queue') or 0) + int(sample.get('retiring_queue') or 0)}, "
                f"rejected={rejection_ratio:.2%}, healthy={len(healthy_streak)}/10",
                flush=True,
            )
            last_progress = now
        workload = state.get("workload", {})
        if isinstance(workload, Mapping) and not workload.get("running"):
            raise RuntimeError(f"workload stopped before admission: {state.get('metrics_error')}")
        time.sleep(1)

    base = {
        "variant": variant,
        "source_topology": topology,
        "fence_wakeup_mode": wakeup,
        "target_tps": total_tps,
        "active_target_tps": active_tps,
        "retiring_target_tps": retiring_tps,
        "active_rows_per_transaction": 1,
        "retiring_rows_per_transaction": 1,
        "environment_generation_id": generation_id,
        "started_at_utc": started_at,
        "peak_preflip": peak,
        "admission_sample": admitted,
    }
    if admitted is None:
        stop_workload_if_possible()
        print(
            f"    admission timeout; peak achieved={None if peak is None else peak.get('achieved_tps')} TPS",
            flush=True,
        )
        return {
            **base,
            "status": "admission_timeout",
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    print(
        f"    admitted at {admitted.get('achieved_tps')} TPS; starting {variant} flip",
        flush=True,
    )
    request_json(
        API,
        "/flip/start",
        method="POST",
        payload={
            "fence_wakeup_mode": wakeup,
            "source_proof_mode": "slot_lsn_v1",
        },
    )
    flip_deadline = time.monotonic() + 180
    final_state: Mapping[str, Any] | None = None
    while time.monotonic() < flip_deadline:
        state = api_state()
        flip = state.get("flip", {})
        status = flip.get("status") if isinstance(flip, Mapping) else None
        if status in TERMINAL_FLIP_STATES:
            final_state = state
            break
        time.sleep(1)
    if final_state is None:
        raise MatrixAbort(
            "flip did not reach a terminal state within 180 seconds; manual recovery is required"
        )
    stop_workload_if_possible()
    flip = final_state["flip"]
    final_sample = compact_sample(final_state.get("latest"))
    print(
        f"    flip {flip.get('status')}; writer park={round(float(flip.get('durations_ns', {}).get('writer_park_ns', 0)) / 1_000_000, 1)} ms",
        flush=True,
    )
    return {
        **base,
        "status": flip.get("status"),
        "run_id": flip.get("run_id"),
        "outcome": flip.get("outcome"),
        "verification_outcome": flip.get("verification_outcome"),
        "durations_ns": flip.get("durations_ns"),
        "final_sample": final_sample,
        "error": flip.get("error"),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def save_report(path: Path, started_at: str, cases: list[dict[str, Any]]) -> None:
    payload = {
        "schema_version": 1,
        "experiment": "target-tps-a-b-bplus-matrix",
        "started_at_utc": started_at,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "single_host": True,
        },
        "constants": {
            "table_count": 5,
            "active_retiring_split": "90:10",
            "rows_per_transaction": 1,
            "payload_bytes": 256,
            "active_workers": 32,
            "retiring_workers": 8,
            "rate_window_seconds": 5,
            "minimum_achievement_percent": 80,
            "minimum_warmup_seconds": 20,
            "required_healthy_one_second_observations": 10,
            "max_rejected_ratio": 0.05,
            "admission_timeout_seconds": {"1000": 90, "5000": 90, "15000": 180},
            "max_source_lag_bytes": 64 * 1024 * 1024,
            "max_sink_lag_records_per_partition": 100,
            "park_budget_ms": 60_000,
            "revert_reserve_ms": 10_000,
        },
        "cases": cases,
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--confirm-reset",
        required=True,
        help="must be RESET to authorize deletion of scoped local benchmark volumes",
    )
    arguments = parser.parse_args()
    if arguments.confirm_reset != "RESET":
        parser.error("--confirm-reset must be RESET exactly")
    started_at = datetime.now(timezone.utc).isoformat()
    output = Path("results") / f"tps-matrix-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    cases: list[dict[str, Any]] = []
    save_report(output, started_at, cases)
    try:
        for variant, topology, wakeup, total_tps in CASES:
            try:
                result = run_case(variant, topology, wakeup, total_tps)
            except Exception as error:
                stop_workload_if_possible()
                result = {
                    "variant": variant,
                    "source_topology": topology,
                    "fence_wakeup_mode": wakeup,
                    "target_tps": total_tps,
                    "status": "harness_error",
                    "error": f"{type(error).__name__}: {error}"[:2000],
                    "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                print(f"    ERROR: {result['error']}", flush=True)
                if isinstance(error, MatrixAbort):
                    cases.append(result)
                    save_report(output, started_at, cases)
                    raise
            cases.append(result)
            save_report(output, started_at, cases)
    finally:
        stop_workload_if_possible()
        save_report(output, started_at, cases)
    print(f"\nMatrix saved to {output}", flush=True)


if __name__ == "__main__":
    main()
