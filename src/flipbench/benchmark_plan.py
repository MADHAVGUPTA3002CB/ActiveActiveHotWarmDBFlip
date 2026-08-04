from __future__ import annotations

import hashlib
import json
import random
import re
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .matrix import variant_dimensions


class BenchmarkPlanError(ValueError):
    pass


_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")
_TOP_LEVEL_FIELDS = {
    "schema_version",
    "name",
    "description",
    "variants",
    "target_tps",
    "repetitions",
    "random_seed",
    "table_count",
    "active_percent",
    "workload",
    "timing",
    "admission",
    "safety",
}


def _exact_fields(label: str, value: object, expected: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BenchmarkPlanError(f"{label} must be an object")
    if set(value) != expected:
        raise BenchmarkPlanError(
            f"{label} fields must be exactly {sorted(expected)}"
        )
    return value


def _integer(label: str, value: object, minimum: int, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not minimum <= value <= maximum
    ):
        raise BenchmarkPlanError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return value


@dataclass(frozen=True, slots=True)
class BenchmarkWorkload:
    rows_per_transaction: int
    payload_bytes: int
    active_workers: int
    retiring_workers: int
    queue_limit_per_lane: int
    rate_window_seconds: int
    minimum_achievement_percent: int


@dataclass(frozen=True, slots=True)
class BenchmarkTiming:
    warmup_seconds: int
    measurement_seconds: int
    sample_interval_seconds: int
    final_healthy_samples: int
    flip_timeout_seconds: int
    restart_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class BenchmarkAdmission:
    max_source_lag_bytes: int
    max_sink_lag_records_per_partition: int
    stable_samples: int
    poll_ms: int
    park_budget_ms: int
    revert_reserve_ms: int


@dataclass(frozen=True, slots=True)
class BenchmarkSafety:
    minimum_free_disk_gib: int
    stop_on_correctness_failure: bool


@dataclass(frozen=True, slots=True)
class BenchmarkPlan:
    schema_version: int
    name: str
    description: str
    variants: tuple[str, ...]
    target_tps: tuple[int, ...]
    repetitions: int
    random_seed: int
    table_count: int
    active_percent: int
    workload: BenchmarkWorkload
    timing: BenchmarkTiming
    admission: BenchmarkAdmission
    safety: BenchmarkSafety

    @property
    def case_count(self) -> int:
        return len(self.variants) * len(self.target_tps) * self.repetitions

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["variants"] = list(self.variants)
        value["target_tps"] = list(self.target_tps)
        return value

    @property
    def sha256(self) -> str:
        canonical = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True, slots=True)
class BenchmarkCase:
    ordinal: int
    repetition: int
    variant: str
    target_tps: int
    active_target_tps: int
    retiring_target_tps: int
    source_topology: str
    fence_wakeup_mode: str
    write_fence_mode: str
    source_proof_mode: str
    transaction_shape: str
    tables_per_api_transaction: int
    operations_per_api_batch: int
    ownership_reads_per_api_batch: int

    @property
    def case_id(self) -> str:
        normalized = self.variant.lower().replace("+", "plus")
        return (
            f"{self.ordinal:03d}-r{self.repetition:02d}-"
            f"{self.target_tps:06d}tps-{normalized}"
        )


def load_benchmark_plan(path: Path) -> BenchmarkPlan:
    if path.is_symlink() or not path.is_file():
        raise BenchmarkPlanError(f"benchmark plan is not a regular file: {path}")
    if path.stat().st_size > 65_536:
        raise BenchmarkPlanError("benchmark plan exceeds 65536 bytes")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise BenchmarkPlanError(f"could not read benchmark plan: {error}") from error
    root = _exact_fields("benchmark plan", payload, _TOP_LEVEL_FIELDS)
    if root["schema_version"] != 1:
        raise BenchmarkPlanError("schema_version must be 1")
    name = root["name"]
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise BenchmarkPlanError("name must be a lowercase 3..64 character slug")
    description = root["description"]
    if not isinstance(description, str) or not 1 <= len(description) <= 500:
        raise BenchmarkPlanError("description must contain 1..500 characters")

    raw_variants = root["variants"]
    if not isinstance(raw_variants, list) or not 1 <= len(raw_variants) <= 5:
        raise BenchmarkPlanError("variants must contain 1..5 values")
    if any(not isinstance(value, str) for value in raw_variants):
        raise BenchmarkPlanError("every variant must be a string")
    variants = tuple(raw_variants)
    if len(set(variants)) != len(variants):
        raise BenchmarkPlanError("variants must be unique")
    for variant in variants:
        try:
            variant_dimensions(variant)
        except ValueError as error:
            raise BenchmarkPlanError(f"unknown benchmark variant: {variant}") from error

    raw_targets = root["target_tps"]
    if not isinstance(raw_targets, list) or not 1 <= len(raw_targets) <= 20:
        raise BenchmarkPlanError("target_tps must contain 1..20 values")
    targets = tuple(
        _integer("target_tps value", value, 2, 100_000) for value in raw_targets
    )
    if len(set(targets)) != len(targets):
        raise BenchmarkPlanError("target_tps values must be unique")
    if tuple(sorted(targets)) != targets:
        raise BenchmarkPlanError("target_tps values must be ascending")

    repetitions = _integer("repetitions", root["repetitions"], 1, 30)
    random_seed = _integer("random_seed", root["random_seed"], 0, 2**31 - 1)
    table_count = _integer("table_count", root["table_count"], 5, 20)
    if table_count not in (5, 10, 15, 20):
        raise BenchmarkPlanError("table_count must be 5, 10, 15, or 20")
    active_percent = _integer("active_percent", root["active_percent"], 1, 99)
    if any(target * active_percent // 100 < 1 for target in targets):
        raise BenchmarkPlanError("active_percent leaves an empty active lane")

    workload_raw = _exact_fields(
        "workload",
        root["workload"],
        {
            "rows_per_transaction",
            "payload_bytes",
            "active_workers",
            "retiring_workers",
            "queue_limit_per_lane",
            "rate_window_seconds",
            "minimum_achievement_percent",
        },
    )
    workload = BenchmarkWorkload(
        _integer("rows_per_transaction", workload_raw["rows_per_transaction"], 1, 100_000),
        _integer("payload_bytes", workload_raw["payload_bytes"], 16, 65_536),
        _integer("active_workers", workload_raw["active_workers"], 1, 63),
        _integer("retiring_workers", workload_raw["retiring_workers"], 1, 63),
        _integer("queue_limit_per_lane", workload_raw["queue_limit_per_lane"], 1, 100_000),
        _integer("rate_window_seconds", workload_raw["rate_window_seconds"], 1, 60),
        _integer(
            "minimum_achievement_percent",
            workload_raw["minimum_achievement_percent"],
            1,
            100,
        ),
    )
    if workload.active_workers + workload.retiring_workers > 64:
        raise BenchmarkPlanError("active and retiring workers must total at most 64")

    timing_raw = _exact_fields(
        "timing",
        root["timing"],
        {
            "warmup_seconds",
            "measurement_seconds",
            "sample_interval_seconds",
            "final_healthy_samples",
            "flip_timeout_seconds",
            "restart_timeout_seconds",
        },
    )
    timing = BenchmarkTiming(
        _integer("warmup_seconds", timing_raw["warmup_seconds"], 1, 3600),
        _integer("measurement_seconds", timing_raw["measurement_seconds"], 1, 3600),
        _integer("sample_interval_seconds", timing_raw["sample_interval_seconds"], 1, 60),
        _integer("final_healthy_samples", timing_raw["final_healthy_samples"], 1, 100),
        _integer("flip_timeout_seconds", timing_raw["flip_timeout_seconds"], 10, 3600),
        _integer("restart_timeout_seconds", timing_raw["restart_timeout_seconds"], 60, 3600),
    )
    available_samples = timing.measurement_seconds // timing.sample_interval_seconds
    if timing.final_healthy_samples > available_samples:
        raise BenchmarkPlanError(
            "final healthy samples exceed the measurement window"
        )

    admission_raw = _exact_fields(
        "admission",
        root["admission"],
        {
            "max_source_lag_bytes",
            "max_sink_lag_records_per_partition",
            "stable_samples",
            "poll_ms",
            "park_budget_ms",
            "revert_reserve_ms",
        },
    )
    admission = BenchmarkAdmission(
        _integer("max_source_lag_bytes", admission_raw["max_source_lag_bytes"], 0, 2**63 - 1),
        _integer(
            "max_sink_lag_records_per_partition",
            admission_raw["max_sink_lag_records_per_partition"],
            0,
            2**31 - 1,
        ),
        _integer("stable_samples", admission_raw["stable_samples"], 1, 100),
        _integer("poll_ms", admission_raw["poll_ms"], 1, 5000),
        _integer("park_budget_ms", admission_raw["park_budget_ms"], 100, 3_600_000),
        _integer("revert_reserve_ms", admission_raw["revert_reserve_ms"], 1, 3_599_999),
    )
    if admission.revert_reserve_ms >= admission.park_budget_ms:
        raise BenchmarkPlanError("revert reserve must be smaller than park budget")

    safety_raw = _exact_fields(
        "safety",
        root["safety"],
        {"minimum_free_disk_gib", "stop_on_correctness_failure"},
    )
    stop_on_failure = safety_raw["stop_on_correctness_failure"]
    if not isinstance(stop_on_failure, bool):
        raise BenchmarkPlanError("stop_on_correctness_failure must be boolean")
    safety = BenchmarkSafety(
        _integer("minimum_free_disk_gib", safety_raw["minimum_free_disk_gib"], 1, 500),
        stop_on_failure,
    )
    plan = BenchmarkPlan(
        1,
        name,
        description,
        variants,
        targets,
        repetitions,
        random_seed,
        table_count,
        active_percent,
        workload,
        timing,
        admission,
        safety,
    )
    if plan.case_count > 500:
        raise BenchmarkPlanError("benchmark plan may contain at most 500 cases")
    return plan


def build_benchmark_cases(plan: BenchmarkPlan) -> tuple[BenchmarkCase, ...]:
    rng = random.Random(plan.random_seed)
    cases: list[BenchmarkCase] = []
    for repetition in range(1, plan.repetitions + 1):
        for target in plan.target_tps:
            ordered_variants = list(plan.variants)
            rng.shuffle(ordered_variants)
            for variant in ordered_variants:
                topology, wakeup, fence, proof = variant_dimensions(variant)
                active = target * plan.active_percent // 100
                shape = "api_batch_separate_commits_v1"
                cases.append(
                    BenchmarkCase(
                        len(cases) + 1,
                        repetition,
                        variant,
                        target,
                        active,
                        target - active,
                        topology,
                        wakeup,
                        fence,
                        proof,
                        shape,
                        1,
                        plan.table_count,
                        1 if variant in ("E", "F", "G", "H") else plan.table_count,
                    )
                )
    return tuple(cases)


def _milliseconds(value: object) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "—"
    return f"{value / 1_000_000:.1f} ms"


def render_benchmark_report(
    plan: BenchmarkPlan,
    observations: Sequence[Mapping[str, Any]],
    plan_sha256: str,
) -> str:
    shapes = {
        str(item.get("transaction_shape")) for item in observations
    } or {"api_batch_separate_commits_v1" for _variant in plan.variants}
    lines = [
        f"# Benchmark report: {plan.name}",
        "",
        plan.description,
        "",
        f"Plan SHA-256: `{plan_sha256}`",
        "",
        f"Cases completed: **{len(observations)} / {plan.case_count}**",
        "",
    ]
    if len(shapes) > 1:
        lines.extend(
            (
                "> Workload warning: `single_table_api` and `all_tables_api` results are not directly comparable as equal business operations. Compare each shape separately or normalize by physical rows/commits.",
                "",
            )
        )
    lines.extend(
        (
            "## Individual runs",
            "",
            "| Case | Variant | Shape | Ownership reads / API batch | Target TPS | Median achieved | Capacity | Flip | Writer park | Source proof | Verification |",
            "|---|---|---|---:|---:|---:|---|---|---:|---:|---|",
        )
    )
    for item in observations:
        capacity = item.get("capacity")
        capacity = capacity if isinstance(capacity, Mapping) else {}
        durations = item.get("durations_ns")
        durations = durations if isinstance(durations, Mapping) else {}
        lines.append(
            "| {case} | {variant} | `{shape}` | {reads} | {target:,} | {achieved:,.1f} | {label} | {flip} | {park} | {source} | {verify} |".format(
                case=item.get("case_id", "unknown"),
                variant=item.get("variant", "?"),
                shape=item.get("transaction_shape", "unknown"),
                reads=item.get("ownership_reads_per_api_batch", "—"),
                target=int(item.get("target_tps") or 0),
                achieved=float(capacity.get("median_achieved_tps") or 0),
                label=capacity.get("capacity_label", "unknown"),
                flip=item.get("flip_status", "unknown"),
                park=_milliseconds(durations.get("writer_park_ns")),
                source=_milliseconds(durations.get("source_proof_ns")),
                verify=item.get("verification_outcome", "unknown"),
            )
        )

    lines.extend(("", "## Aggregated comparison", ""))
    lines.extend(
        (
            "| Variant | Shape | Target TPS | Runs | Median achieved TPS | Median writer park | Passed |",
            "|---|---|---:|---:|---:|---:|---:|",
        )
    )
    groups: dict[tuple[str, str, int], list[Mapping[str, Any]]] = {}
    for item in observations:
        key = (
            str(item.get("variant")),
            str(item.get("transaction_shape")),
            int(item.get("target_tps") or 0),
        )
        groups.setdefault(key, []).append(item)
    for (variant, shape, target), items in sorted(groups.items()):
        achieved = [
            float(item.get("capacity", {}).get("median_achieved_tps") or 0)
            for item in items
            if isinstance(item.get("capacity"), Mapping)
        ]
        parks = [
            float(item.get("durations_ns", {}).get("writer_park_ns"))
            for item in items
            if isinstance(item.get("durations_ns"), Mapping)
            and isinstance(item.get("durations_ns", {}).get("writer_park_ns"), (int, float))
        ]
        passed = sum(
            item.get("flip_status") == "succeeded"
            and item.get("verification_outcome") == "passed"
            for item in items
        )
        median_park = "—" if not parks else _milliseconds(statistics.median(parks))
        median_achieved = 0.0 if not achieved else statistics.median(achieved)
        lines.append(
            f"| {variant} | `{shape}` | {target:,} | {len(items)} | {median_achieved:,.1f} | {median_park} | {passed}/{len(items)} |"
        )
    lines.extend(
        (
            "",
            "Capacity labels describe whether the offered load was sustained. Flip latency from an overloaded case is diagnostic evidence, not a healthy-load SLO.",
            "",
        )
    )
    return "\n".join(lines)
