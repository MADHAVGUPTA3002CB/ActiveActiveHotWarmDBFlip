from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .overload import BackgroundBatchWriter


@dataclass(frozen=True, slots=True)
class WorkloadMix:
    active_events_per_table: int
    retiring_events_per_table: int

    def __post_init__(self) -> None:
        values = (self.active_events_per_table, self.retiring_events_per_table)
        if any(not isinstance(value, int) or isinstance(value, bool) for value in values):
            raise ValueError("workload events-per-table values must be integers")
        if self.active_events_per_table <= 0 or self.retiring_events_per_table < 0:
            raise ValueError("active load must be positive and retiring load cannot be negative")
        if self.retiring_events_per_table > self.active_events_per_table:
            raise ValueError("production-shaped workload must be active-heavy and retiring-light")


@dataclass(frozen=True, slots=True)
class MixedWorkload:
    active: BackgroundBatchWriter
    retiring: BackgroundBatchWriter

    @classmethod
    def start(
        cls,
        active_batch: Callable[[], int],
        retiring_batch: Callable[[], int],
        max_batches: int,
    ) -> MixedWorkload:
        active = BackgroundBatchWriter.start(active_batch, max_batches)
        try:
            retiring = BackgroundBatchWriter.start(retiring_batch, max_batches)
        except BaseException:
            active.stop_and_join(5.0)
            raise
        return cls(active, retiring)

    def stop_retiring(self, timeout_seconds: float) -> int:
        return self.retiring.stop_and_join(timeout_seconds)

    def stop_active(self, timeout_seconds: float) -> int:
        return self.active.stop_and_join(timeout_seconds)

    def active_is_alive(self) -> bool:
        return self.active.is_alive()

    def retiring_is_alive(self) -> bool:
        return self.retiring.is_alive()

    def active_total(self) -> int:
        return self.active.total_inserted()

    def retiring_total(self) -> int:
        return self.retiring.total_inserted()
