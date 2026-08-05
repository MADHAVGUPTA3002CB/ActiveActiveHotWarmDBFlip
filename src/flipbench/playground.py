from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread
from typing import Callable, Mapping

from .core import (
    FenceWakeupMode,
    OptimisticAdmissionCheckMode,
    SourceProofMode,
    WriteFenceMode,
)
from .traffic import TrafficLane, TrafficSnapshot, TrafficTarget, TrafficWorker


@dataclass(frozen=True, slots=True)
class FlipStartRequest:
    fence_wakeup_mode: FenceWakeupMode = FenceWakeupMode.PASSIVE
    source_proof_mode: SourceProofMode = SourceProofMode.SLOT_LSN

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> FlipStartRequest:
        if set(payload) - {"fence_wakeup_mode", "source_proof_mode"}:
            raise ValueError("flip body contains an unknown option")
        value = payload.get("fence_wakeup_mode", FenceWakeupMode.PASSIVE.value)
        proof = payload.get("source_proof_mode", SourceProofMode.SLOT_LSN.value)
        if not isinstance(value, str):
            raise ValueError("fence_wakeup_mode must be a string")
        if not isinstance(proof, str):
            raise ValueError("source_proof_mode must be a string")
        try:
            return cls(FenceWakeupMode(value), SourceProofMode(proof))
        except ValueError as error:
            raise ValueError(
                "flip modes must be supported fence wake-up and source-proof values"
            ) from error


def _bounded_integer(name: str, value: int, minimum: int, maximum: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")


@dataclass(frozen=True, slots=True)
class WorkloadSettings:
    active_rows_per_partition: int = 100
    retiring_rows_per_partition: int = 10
    active_pause_ms: int = 5
    retiring_pause_ms: int = 10
    payload_bytes: int = 256
    mode: str = "legacy_batch"
    active_target_tps: int = 13_636
    retiring_target_tps: int = 1_364
    active_rows_per_transaction: int = 1
    retiring_rows_per_transaction: int = 1
    active_update_percent: int = 0
    retiring_update_percent: int = 0
    update_seed_rows_per_table: int = 1_000
    active_workers: int = 32
    retiring_workers: int = 8
    max_queue_size: int = 30_000
    rate_window_seconds: int = 5
    min_achievement_percent: int = 80
    write_fence_mode: str = WriteFenceMode.WARM_TRACKER_ADVISORY.value
    optimistic_admission_check_mode: str = (
        OptimisticAdmissionCheckMode.STATE_AND_EPOCH.value
    )

    def __post_init__(self) -> None:
        _bounded_integer("active_rows_per_partition", self.active_rows_per_partition, 1, 100_000)
        _bounded_integer("retiring_rows_per_partition", self.retiring_rows_per_partition, 0, 100_000)
        _bounded_integer("active_pause_ms", self.active_pause_ms, 0, 60_000)
        _bounded_integer("retiring_pause_ms", self.retiring_pause_ms, 0, 60_000)
        _bounded_integer("payload_bytes", self.payload_bytes, 16, 65_536)
        if self.mode not in ("legacy_batch", "target_rate_v1"):
            raise ValueError("mode must be legacy_batch or target_rate_v1")
        try:
            guard_mode = WriteFenceMode(self.write_fence_mode)
        except ValueError as error:
            raise ValueError(
                "write_fence_mode must be warm_tracker_advisory_v1, "
                "hot_transactional_v1, or optimistic_detach_v1"
            ) from error
        if guard_mode in (
            WriteFenceMode.HOT_TRANSACTIONAL,
            WriteFenceMode.OPTIMISTIC_DETACH,
        ) and self.mode != "target_rate_v1":
            raise ValueError(
                f"{guard_mode.value} requires target_rate_v1 workload mode"
            )
        try:
            admission_check_mode = OptimisticAdmissionCheckMode(
                self.optimistic_admission_check_mode
            )
        except ValueError as error:
            raise ValueError(
                "optimistic_admission_check_mode must be state_and_epoch_v1 or state_only_v1"
            ) from error
        if (
            admission_check_mode is OptimisticAdmissionCheckMode.STATE_ONLY
            and guard_mode is not WriteFenceMode.OPTIMISTIC_DETACH
        ):
            raise ValueError(
                "state_only_v1 admission requires optimistic_detach_v1"
            )
        _bounded_integer("active_target_tps", self.active_target_tps, 1, 1_000_000)
        _bounded_integer("retiring_target_tps", self.retiring_target_tps, 1, 1_000_000)
        _bounded_integer("active_update_percent", self.active_update_percent, 0, 100)
        _bounded_integer("retiring_update_percent", self.retiring_update_percent, 0, 100)
        _bounded_integer(
            "update_seed_rows_per_table",
            self.update_seed_rows_per_table,
            1,
            100_000,
        )
        has_updates = self.active_update_percent > 0 or self.retiring_update_percent > 0
        if has_updates and self.mode != "target_rate_v1":
            raise ValueError("UPDATE traffic requires target-rate workload mode")
        if (
            self.active_update_percent > 0
            and self.update_seed_rows_per_table < self.active_rows_per_transaction
        ) or (
            self.retiring_update_percent > 0
            and self.update_seed_rows_per_table < self.retiring_rows_per_transaction
        ):
            raise ValueError(
                "update seed rows per table must cover rows per UPDATE transaction"
            )
        self.active_target()
        self.retiring_target()
        _bounded_integer("min_achievement_percent", self.min_achievement_percent, 1, 100)
        if self.mode == "target_rate_v1":
            if self.total_target_tps > 100_000:
                raise ValueError("total target TPS must not exceed 100000 on the local profile")
            if self.active_workers + self.retiring_workers > 64:
                raise ValueError("total workers must not exceed 64 on the local profile")
            if self.max_queue_size > 100_000:
                raise ValueError("queue size must not exceed 100000 per lane on the local profile")
            in_flight_rows = (
                self.active_workers * self.active_rows_per_transaction
                + self.retiring_workers * self.retiring_rows_per_transaction
            )
            if in_flight_rows > 250_000:
                raise ValueError(
                    "aggregate in-flight rows must not exceed 250000 on the local profile"
                )
            in_flight_payload = self.payload_bytes * (
                in_flight_rows
            )
            if in_flight_payload > 256 * 1024 * 1024:
                raise ValueError(
                    "aggregate in-flight payload must not exceed 256 MiB on the local profile"
                )

    @property
    def total_target_tps(self) -> int:
        return self.active_target_tps + self.retiring_target_tps

    def active_target(self) -> TrafficTarget:
        return TrafficTarget(
            target_tps=self.active_target_tps,
            rows_per_transaction=self.active_rows_per_transaction,
            worker_count=self.active_workers,
            max_queue_size=self.max_queue_size,
            rate_window_seconds=self.rate_window_seconds,
            update_percent=self.active_update_percent,
        )

    def retiring_target(self) -> TrafficTarget:
        return TrafficTarget(
            target_tps=self.retiring_target_tps,
            rows_per_transaction=self.retiring_rows_per_transaction,
            worker_count=self.retiring_workers,
            max_queue_size=self.max_queue_size,
            rate_window_seconds=self.rate_window_seconds,
            update_percent=self.retiring_update_percent,
        )

    def to_dict(self) -> dict[str, int | str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AdmissionThresholds:
    max_source_lag_bytes: int = 8 * 1024 * 1024
    max_sink_lag_records_per_partition: int = 10
    stable_samples: int = 3
    park_budget_ms: int = 5000
    revert_reserve_ms: int = 500
    poll_ms: int = 50

    def __post_init__(self) -> None:
        _bounded_integer("max_source_lag_bytes", self.max_source_lag_bytes, 0, 2**63 - 1)
        _bounded_integer(
            "max_sink_lag_records_per_partition",
            self.max_sink_lag_records_per_partition,
            0,
            2**31 - 1,
        )
        _bounded_integer("stable_samples", self.stable_samples, 1, 100)
        _bounded_integer("park_budget_ms", self.park_budget_ms, 100, 600_000)
        _bounded_integer("revert_reserve_ms", self.revert_reserve_ms, 1, 599_999)
        _bounded_integer("poll_ms", self.poll_ms, 10, 5000)
        if self.revert_reserve_ms >= self.park_budget_ms:
            raise ValueError("revert_reserve_ms must be smaller than park_budget_ms")

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


class AdmissionWindow:
    def __init__(self, thresholds: AdmissionThresholds) -> None:
        self._thresholds = thresholds
        self._healthy_samples = 0

    @property
    def healthy_samples(self) -> int:
        return self._healthy_samples

    @property
    def ready(self) -> bool:
        return self._healthy_samples >= self._thresholds.stable_samples

    def reset(self) -> None:
        self._healthy_samples = 0

    def observe(self, source_lag_bytes: int, sink_lag_by_partition: Mapping[str, int]) -> bool:
        if not sink_lag_by_partition:
            raise ValueError("sink lag vector must not be empty")
        _bounded_integer("source_lag_bytes", source_lag_bytes, 0, 2**63 - 1)
        for value in sink_lag_by_partition.values():
            _bounded_integer("sink_lag", value, 0, 2**63 - 1)
        healthy = (
            source_lag_bytes <= self._thresholds.max_source_lag_bytes
            and max(sink_lag_by_partition.values())
            <= self._thresholds.max_sink_lag_records_per_partition
        )
        self._healthy_samples = self._healthy_samples + 1 if healthy else 0
        return self.ready


def validate_local_batch_budget(
    settings: WorkloadSettings,
    table_count: int,
    max_batch_bytes: int = 16 * 1024 * 1024,
) -> None:
    _bounded_integer("table_count", table_count, 1, 1000)
    _bounded_integer("max_batch_bytes", max_batch_bytes, 1, 2**63 - 1)
    if settings.mode == "target_rate_v1":
        largest_bytes = max(
            settings.active_rows_per_transaction,
            settings.retiring_rows_per_transaction,
        ) * settings.payload_bytes
    else:
        largest_rows = max(settings.active_rows_per_partition, settings.retiring_rows_per_partition)
        largest_bytes = largest_rows * settings.payload_bytes * table_count
    if largest_bytes > max_batch_bytes:
        raise ValueError(
            "largest batch exceeds the 16 MiB local safety budget; reduce rows or payload size"
        )


def workload_progress_valid(
    settings: WorkloadSettings,
    active_rows: int,
    retiring_rows: int,
) -> bool:
    if settings.mode == "target_rate_v1":
        return active_rows > 0 and retiring_rows > 0
    return active_rows > retiring_rows > 0


class _Counter:
    def __init__(self) -> None:
        self._lock = Lock()
        self._value = 0

    def add(self, value: int) -> None:
        with self._lock:
            self._value += value

    def read(self) -> int:
        with self._lock:
            return self._value


@dataclass(frozen=True, slots=True)
class LiveBatchWriter:
    _thread: Thread
    _stop: Event
    _counter: _Counter
    _errors: SimpleQueue[BaseException]

    @classmethod
    def start(
        cls,
        write_batch: Callable[[int], int],
        live_config: Callable[[], tuple[int, float]],
    ) -> LiveBatchWriter:
        if not callable(write_batch) or not callable(live_config):
            raise TypeError("write_batch and live_config must be callable")
        stop = Event()
        counter = _Counter()
        errors: SimpleQueue[BaseException] = SimpleQueue()

        def run() -> None:
            try:
                while not stop.is_set():
                    rows, pause_seconds = live_config()
                    _bounded_integer("rows_per_partition", rows, 0, 100_000)
                    if rows == 0:
                        stop.wait(max(0.01, pause_seconds))
                        continue
                    inserted = write_batch(rows)
                    if not isinstance(inserted, int) or isinstance(inserted, bool) or inserted <= 0:
                        raise ValueError("write_batch must return a positive integer")
                    counter.add(inserted)
                    stop.wait(pause_seconds)
            except RuntimeError as error:
                if not str(error).startswith("hot writer parked:"):
                    errors.put(error)
            except BaseException as error:
                errors.put(error)

        thread = Thread(target=run, name="flipbench-playground-writer", daemon=True)
        writer = cls(thread, stop, counter, errors)
        thread.start()
        return writer

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def total_inserted(self) -> int:
        return self._counter.read()

    def stop_and_join(self, timeout_seconds: float) -> int:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._stop.set()
        self._thread.join(timeout_seconds)
        if self._thread.is_alive():
            raise TimeoutError("live batch writer did not stop")
        try:
            error = self._errors.get_nowait()
        except Empty:
            return self.total_inserted()
        raise RuntimeError(str(error)) from error


class LiveWorkload:
    def __init__(self, settings: WorkloadSettings) -> None:
        self._lock = Lock()
        self._settings = settings
        self._active: LiveBatchWriter | TrafficLane | None = None
        self._retiring: LiveBatchWriter | TrafficLane | None = None

    def settings(self) -> WorkloadSettings:
        with self._lock:
            return self._settings

    def update(self, settings: WorkloadSettings) -> WorkloadSettings:
        with self._lock:
            previous = self._settings
            self._settings = settings
            active = self._active
            retiring = self._retiring
        if settings.mode == "target_rate_v1" and settings != previous:
            for lane in (active, retiring):
                reset = getattr(lane, "reset_measurement", None)
                if callable(reset):
                    reset()
        return settings

    def start(
        self,
        active_batch: Callable[[int], int],
        retiring_batch: Callable[[int], int],
    ) -> None:
        if self.running():
            raise RuntimeError("workload is already running")
        active = LiveBatchWriter.start(
            active_batch,
            lambda: (
                self.settings().active_rows_per_partition,
                self.settings().active_pause_ms / 1000,
            ),
        )
        try:
            retiring = LiveBatchWriter.start(
                retiring_batch,
                lambda: (
                    self.settings().retiring_rows_per_partition,
                    self.settings().retiring_pause_ms / 1000,
                ),
            )
        except BaseException:
            active.stop_and_join(5)
            raise
        self._active = active
        self._retiring = retiring

    def start_target_rate(
        self,
        active_worker_factory: Callable[[], TrafficWorker],
        retiring_worker_factory: Callable[[], TrafficWorker],
        table_count: int,
        operations_per_api_batch: int = 1,
    ) -> None:
        if self.running():
            raise RuntimeError("workload is already running")
        if self.settings().mode != "target_rate_v1":
            raise RuntimeError("target-rate start requires target_rate_v1 settings")
        active = TrafficLane.start(
            active_worker_factory,
            lambda: self.settings().active_target(),
            table_count,
            operations_per_api_batch,
        )
        try:
            retiring = TrafficLane.start(
                retiring_worker_factory,
                lambda: self.settings().retiring_target(),
                table_count,
                operations_per_api_batch,
            )
        except BaseException:
            active.stop_and_join(5)
            raise
        self._active = active
        self._retiring = retiring

    def running(self) -> bool:
        return any(
            writer is not None and writer.is_alive()
            for writer in (self._active, self._retiring)
        )

    def active_is_alive(self) -> bool:
        return self._active is not None and self._active.is_alive()

    def retiring_is_alive(self) -> bool:
        return self._retiring is not None and self._retiring.is_alive()

    def active_total(self) -> int:
        return 0 if self._active is None else self._active.total_inserted()

    def retiring_total(self) -> int:
        return 0 if self._retiring is None else self._retiring.total_inserted()

    def traffic_snapshots(self) -> dict[str, TrafficSnapshot] | None:
        if not isinstance(self._active, TrafficLane) or not isinstance(self._retiring, TrafficLane):
            return None
        return {
            "active": self._active.snapshot(),
            "retiring": self._retiring.snapshot(),
        }

    def stop_retiring(self, timeout_seconds: float) -> int:
        if self._retiring is None:
            return 0
        stopped = self._retiring.stop_and_join(timeout_seconds)
        return stopped.committed_rows if isinstance(stopped, TrafficSnapshot) else stopped

    def stop_retiring_admission(self, timeout_seconds: float) -> int:
        if not isinstance(self._retiring, TrafficLane):
            raise RuntimeError(
                "optimistic detach requires a target-rate retiring traffic lane"
            )
        snapshot = self._retiring.stop_admission(timeout_seconds)
        return snapshot.committed_rows

    def finish_retiring_in_flight(self, timeout_seconds: float) -> int:
        if not isinstance(self._retiring, TrafficLane):
            raise RuntimeError(
                "optimistic detach requires a target-rate retiring traffic lane"
            )
        snapshot = self._retiring.finish_in_flight(timeout_seconds)
        return snapshot.committed_rows

    def stop_all(self, timeout_seconds: float = 5) -> None:
        errors: list[BaseException] = []
        for writer in (self._retiring, self._active):
            if writer is not None:
                try:
                    writer.stop_and_join(timeout_seconds)
                except BaseException as error:
                    errors.append(error)
        if errors:
            raise RuntimeError("; ".join(str(error) for error in errors))
