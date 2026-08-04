from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class MatrixCase:
    ordinal: int
    variant: str
    target_tps: int
    source_topology: str
    fence_wakeup_mode: str
    write_fence_mode: str
    source_proof_mode: str

    @property
    def active_target_tps(self) -> int:
        return self.target_tps * 9 // 10

    @property
    def retiring_target_tps(self) -> int:
        return self.target_tps - self.active_target_tps

    @property
    def case_id(self) -> str:
        normalized = self.variant.lower().replace("+", "plus")
        return f"{self.ordinal:02d}-{self.target_tps:05d}tps-{normalized}"


@dataclass(frozen=True, slots=True)
class MatrixObservation:
    achieved_tps: float
    active_tps: float
    retiring_tps: float
    scheduled_transactions: int
    rejected_transactions: int
    queue_depth: int


_VARIANTS = MappingProxyType(
    {
        "A": ("shared", "passive", "warm_tracker_advisory_v1", "slot_lsn_v1"),
        "B": ("isolated", "passive", "warm_tracker_advisory_v1", "slot_lsn_v1"),
        "B+": ("isolated", "immediate_heartbeat", "warm_tracker_advisory_v1", "slot_lsn_v1"),
        "D": ("isolated", "immediate_heartbeat", "hot_transactional_v1", "slot_lsn_v1"),
        "E": ("isolated", "immediate_heartbeat", "optimistic_detach_v1", "slot_lsn_v1"),
        "F": ("isolated", "passive", "optimistic_detach_v1", "per_leaf_marker_v1"),
        "G": ("isolated", "passive", "optimistic_detach_v1", "atomic_detach_marker_v1"),
        "H": ("isolated", "passive", "optimistic_detach_v1", "parallel_atomic_detach_marker_v1"),
    }
)


def variant_dimensions(variant: str) -> tuple[str, str, str, str]:
    try:
        return _VARIANTS[variant]
    except KeyError as error:
        raise ValueError(f"unknown benchmark variant: {variant!r}") from error


def build_full_matrix() -> tuple[MatrixCase, ...]:
    ordered_cells = (
        ("D", 1_000),
        ("A", 2_000), ("B", 2_000), ("B+", 2_000), ("D", 2_000),
        ("B", 3_000), ("D", 3_000), ("A", 3_000), ("B+", 3_000),
        ("B+", 5_000), ("A", 5_000), ("D", 5_000), ("B", 5_000),
        ("D", 15_000), ("B+", 15_000), ("B", 15_000), ("A", 15_000),
    )
    return tuple(
        MatrixCase(index, variant, target, *variant_dimensions(variant))
        for index, (variant, target) in enumerate(ordered_cells, start=1)
    )


def classify_capacity(
    achievement_percent: float,
    rejection_percent: float,
    p95_queue_depth: float,
    queue_limit: int,
) -> str:
    if achievement_percent < 20 or rejection_percent > 50:
        return "severely_overloaded"
    if (
        achievement_percent < 50
        or rejection_percent > 10
        or p95_queue_depth >= queue_limit * 0.9
    ):
        return "overloaded"
    if achievement_percent < 90 or rejection_percent >= 1:
        return "constrained"
    return "sustainable"


def _percentile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def summarize_observations(
    observations: Sequence[MatrixObservation],
    *,
    target_tps: int,
    queue_limit: int,
) -> Mapping[str, float | int | str]:
    if not observations:
        raise ValueError("at least one matrix observation is required")
    achieved = [item.achieved_tps for item in observations]
    active = [item.active_tps for item in observations]
    retiring = [item.retiring_tps for item in observations]
    queues = [float(item.queue_depth) for item in observations]
    scheduled_delta = max(
        0,
        observations[-1].scheduled_transactions
        - observations[0].scheduled_transactions,
    )
    rejected_delta = max(
        0,
        observations[-1].rejected_transactions
        - observations[0].rejected_transactions,
    )
    rejection_percent = 100 * rejected_delta / max(1, scheduled_delta)
    median_achieved = float(statistics.median(achieved))
    achievement_percent = 100 * median_achieved / target_tps
    p95_queue = _percentile(queues, 0.95)
    return MappingProxyType(
        {
            "peak_observed_tps": max(achieved),
            "median_achieved_tps": median_achieved,
            "p95_achieved_tps": _percentile(achieved, 0.95),
            "median_active_tps": float(statistics.median(active)),
            "median_retiring_tps": float(statistics.median(retiring)),
            "achievement_percent": achievement_percent,
            "rejection_percent": rejection_percent,
            "p95_queue_depth": p95_queue,
            "capacity_label": classify_capacity(
                achievement_percent,
                rejection_percent,
                p95_queue,
                queue_limit,
            ),
        }
    )
