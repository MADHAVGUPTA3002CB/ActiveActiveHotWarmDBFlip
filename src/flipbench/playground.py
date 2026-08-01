from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread
from typing import Callable, Mapping


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

    def __post_init__(self) -> None:
        _bounded_integer("active_rows_per_partition", self.active_rows_per_partition, 1, 100_000)
        _bounded_integer("retiring_rows_per_partition", self.retiring_rows_per_partition, 0, 100_000)
        _bounded_integer("active_pause_ms", self.active_pause_ms, 0, 60_000)
        _bounded_integer("retiring_pause_ms", self.retiring_pause_ms, 0, 60_000)
        _bounded_integer("payload_bytes", self.payload_bytes, 16, 65_536)

    def to_dict(self) -> dict[str, int]:
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
    largest_rows = max(settings.active_rows_per_partition, settings.retiring_rows_per_partition)
    if largest_rows * settings.payload_bytes * table_count > max_batch_bytes:
        raise ValueError(
            "largest batch exceeds the 16 MiB local safety budget; reduce rows or payload size"
        )


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
        self._active: LiveBatchWriter | None = None
        self._retiring: LiveBatchWriter | None = None

    def settings(self) -> WorkloadSettings:
        with self._lock:
            return self._settings

    def update(self, settings: WorkloadSettings) -> WorkloadSettings:
        with self._lock:
            self._settings = settings
            return self._settings

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

    def running(self) -> bool:
        return self._active is not None and self._active.is_alive()

    def active_is_alive(self) -> bool:
        return self._active is not None and self._active.is_alive()

    def retiring_is_alive(self) -> bool:
        return self._retiring is not None and self._retiring.is_alive()

    def active_total(self) -> int:
        return 0 if self._active is None else self._active.total_inserted()

    def retiring_total(self) -> int:
        return 0 if self._retiring is None else self._retiring.total_inserted()

    def stop_retiring(self, timeout_seconds: float) -> int:
        if self._retiring is None:
            return 0
        return self._retiring.stop_and_join(timeout_seconds)

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
