from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from typing import Callable, Literal, Protocol

from .connect_api import redact_error_detail


def _positive_integer(name: str, value: int, maximum: int) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > maximum
    ):
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")


@dataclass(frozen=True, slots=True)
class TrafficTarget:
    target_tps: int
    rows_per_transaction: int
    worker_count: int
    max_queue_size: int
    rate_window_seconds: int = 5
    update_percent: int = 0

    def __post_init__(self) -> None:
        _positive_integer("target_tps", self.target_tps, 1_000_000)
        _positive_integer("rows_per_transaction", self.rows_per_transaction, 100_000)
        _positive_integer("worker_count", self.worker_count, 256)
        _positive_integer("max_queue_size", self.max_queue_size, 1_000_000)
        _positive_integer("rate_window_seconds", self.rate_window_seconds, 60)
        if (
            not isinstance(self.update_percent, int)
            or isinstance(self.update_percent, bool)
            or not 0 <= self.update_percent <= 100
        ):
            raise ValueError("update_percent must be an integer between 0 and 100")


class TrafficWorker(Protocol):
    def write(self, table_index: int, rows: int) -> int: ...

    def update(self, table_index: int, rows: int, target_index: int) -> int: ...

    def close(self) -> None: ...


class FatalTrafficWorkerError(RuntimeError):
    """The worker session is unsafe to reuse and the lane must stop."""


class CommittedTrafficWorkerError(FatalTrafficWorkerError):
    """The database commit succeeded but mandatory worker cleanup failed."""

    def __init__(self, committed_rows: int, message: str) -> None:
        _positive_integer("committed_rows", committed_rows, 100_000)
        super().__init__(message)
        self.committed_rows = committed_rows


class PacedArrivals:
    """Pure token accumulator used by the open-loop scheduler."""

    def __init__(self, start_ns: int) -> None:
        if not isinstance(start_ns, int) or isinstance(start_ns, bool) or start_ns < 0:
            raise ValueError("start_ns must be a non-negative integer")
        self._last_ns = start_ns
        self._credit = 0.0

    def advance(self, now_ns: int, target_tps: int, max_burst: int) -> tuple[int, int]:
        if now_ns < self._last_ns:
            raise ValueError("monotonic time moved backwards")
        _positive_integer("target_tps", target_tps, 1_000_000)
        _positive_integer("max_burst", max_burst, 1_000_000)
        elapsed_ns = now_ns - self._last_ns
        self._last_ns = now_ns
        available = self._credit + elapsed_ns * target_tps / 1_000_000_000
        due = math.floor(available)
        self._credit = available - due
        emitted = min(due, max_burst)
        return emitted, due - emitted


@dataclass(frozen=True, slots=True)
class TrafficSnapshot:
    scheduled_transactions: int
    started_transactions: int
    committed_transactions: int
    committed_insert_transactions: int
    committed_update_transactions: int
    failed_transactions: int
    rejected_transactions: int
    committed_rows: int
    in_flight_transactions: int
    queue_depth: int
    committed_tps: float
    rows_per_second: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    last_error: str | None

    def to_dict(self) -> dict[str, int | float | str | None]:
        return {
            field: getattr(self, field)
            for field in self.__dataclass_fields__
        }


class _TrafficCounters:
    def __init__(self, start_ns: int) -> None:
        self._lock = Lock()
        self._start_ns = start_ns
        self._scheduled = 0
        self._started = 0
        self._committed = 0
        self._committed_inserts = 0
        self._committed_updates = 0
        self._failed = 0
        self._rejected = 0
        self._rows = 0
        self._in_flight = 0
        self._commits: deque[tuple[int, int]] = deque()
        self._latencies_ms: deque[tuple[int, float]] = deque(maxlen=100_000)
        self._last_error: str | None = None
        self._last_error_at_ns: int | None = None

    def offered(self, count: int) -> None:
        if count <= 0:
            return
        with self._lock:
            self._scheduled += count

    def rejected(self, count: int = 1) -> None:
        if count <= 0:
            return
        with self._lock:
            self._rejected += count

    def started(self) -> None:
        with self._lock:
            self._started += 1
            self._in_flight += 1

    def committed(
        self,
        at_ns: int,
        rows: int,
        latency_ms: float,
        operation: Literal["insert", "update"],
    ) -> None:
        with self._lock:
            self._committed += 1
            if operation == "insert":
                self._committed_inserts += 1
            else:
                self._committed_updates += 1
            self._rows += rows
            self._in_flight -= 1
            self._commits.append((at_ns, rows))
            self._latencies_ms.append((at_ns, latency_ms))

    def failed(self, error: BaseException) -> None:
        with self._lock:
            self._failed += 1
            self._in_flight -= 1
            self._last_error = redact_error_detail(
                f"{type(error).__name__}: {error}"
            )[:512]
            self._last_error_at_ns = time.monotonic_ns()

    def setup_failed(self, error: BaseException) -> None:
        with self._lock:
            self._failed += 1
            self._last_error = redact_error_detail(
                f"{type(error).__name__}: {error}"
            )[:512]
            self._last_error_at_ns = time.monotonic_ns()

    def committed_with_error(
        self,
        at_ns: int,
        rows: int,
        latency_ms: float,
        error: BaseException,
        operation: Literal["insert", "update"],
    ) -> None:
        with self._lock:
            self._committed += 1
            if operation == "insert":
                self._committed_inserts += 1
            else:
                self._committed_updates += 1
            self._rows += rows
            self._in_flight -= 1
            self._commits.append((at_ns, rows))
            self._latencies_ms.append((at_ns, latency_ms))
            self._last_error = redact_error_detail(
                f"{type(error).__name__}: {error}"
            )[:512]
            self._last_error_at_ns = at_ns

    def parked(self) -> None:
        with self._lock:
            self._rejected += 1
            self._in_flight -= 1

    def reset_measurement(self, start_ns: int) -> None:
        with self._lock:
            self._start_ns = start_ns
            self._commits.clear()
            self._latencies_ms.clear()
            self._last_error = None
            self._last_error_at_ns = None

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        index = min(len(values) - 1, math.ceil(percentile * len(values)) - 1)
        return values[index]

    def snapshot(
        self,
        now_ns: int,
        queue_depth: int,
        rate_window_seconds: int,
    ) -> TrafficSnapshot:
        cutoff = now_ns - rate_window_seconds * 1_000_000_000
        with self._lock:
            while self._commits and self._commits[0][0] < cutoff:
                self._commits.popleft()
            while self._latencies_ms and self._latencies_ms[0][0] < cutoff:
                self._latencies_ms.popleft()
            if self._last_error_at_ns is not None and self._last_error_at_ns < cutoff:
                self._last_error = None
                self._last_error_at_ns = None
            observed_seconds = max(
                0.001,
                min(rate_window_seconds, (now_ns - self._start_ns) / 1_000_000_000),
            )
            recent_rows = sum(rows for _, rows in self._commits)
            latencies = sorted(latency for _, latency in self._latencies_ms)
            return TrafficSnapshot(
                scheduled_transactions=self._scheduled,
                started_transactions=self._started,
                committed_transactions=self._committed,
                committed_insert_transactions=self._committed_inserts,
                committed_update_transactions=self._committed_updates,
                failed_transactions=self._failed,
                rejected_transactions=self._rejected,
                committed_rows=self._rows,
                in_flight_transactions=self._in_flight,
                queue_depth=queue_depth,
                committed_tps=round(len(self._commits) / observed_seconds, 1),
                rows_per_second=round(recent_rows / observed_seconds, 1),
                latency_p50_ms=round(self._percentile(latencies, 0.50), 3),
                latency_p95_ms=round(self._percentile(latencies, 0.95), 3),
                latency_p99_ms=round(self._percentile(latencies, 0.99), 3),
                last_error=self._last_error,
            )


@dataclass(frozen=True, slots=True)
class _TransactionIntent:
    table_index: int
    rows: int
    operation: Literal["insert", "update"]
    update_target_index: int | None


@dataclass(frozen=True, slots=True)
class _ApiBatchIntent:
    operations: tuple[_TransactionIntent, ...]


@dataclass(frozen=True, slots=True)
class TrafficLane:
    _scheduler: Thread
    _workers: tuple[Thread, ...]
    _scheduler_stop: Event
    _workers_stop: Event
    _queue: Queue[_ApiBatchIntent]
    _counters: _TrafficCounters
    _target: Callable[[], TrafficTarget]
    _batch_size: int

    @classmethod
    def start(
        cls,
        worker_factory: Callable[[], TrafficWorker],
        live_target: Callable[[], TrafficTarget],
        table_count: int,
        operations_per_api_batch: int = 1,
    ) -> TrafficLane:
        if not callable(worker_factory) or not callable(live_target):
            raise TypeError("worker_factory and live_target must be callable")
        _positive_integer("table_count", table_count, 1_000)
        _positive_integer("operations_per_api_batch", operations_per_api_batch, 1_000)
        initial = live_target()
        if not isinstance(initial, TrafficTarget):
            raise TypeError("live_target must return TrafficTarget")
        if operations_per_api_batch > initial.max_queue_size:
            raise ValueError("API batch must fit in the configured transaction queue")
        work_queue: Queue[_ApiBatchIntent] = Queue(
            maxsize=max(1, initial.max_queue_size // operations_per_api_batch)
        )
        scheduler_stop = Event()
        workers_stop = Event()
        start_ns = time.monotonic_ns()
        counters = _TrafficCounters(start_ns)

        def schedule() -> None:
            pacer = PacedArrivals(start_ns)
            sequence = 0
            update_positions = [0] * table_count
            pending: list[_TransactionIntent] = []
            try:
                while not scheduler_stop.is_set():
                    target = live_target()
                    max_burst = max(
                        1,
                        min(target.max_queue_size, math.ceil(target.target_tps / 100)),
                    )
                    emitted, missed = pacer.advance(
                        time.monotonic_ns(), target.target_tps, max_burst
                    )
                    counters.offered(emitted + missed)
                    counters.rejected(missed)
                    for _ in range(emitted):
                        table_index = sequence % table_count
                        is_update = (
                            ((sequence + 1) * target.update_percent) // 100
                            > (sequence * target.update_percent) // 100
                        )
                        update_target_index = None
                        if is_update:
                            update_target_index = update_positions[table_index]
                            update_positions[table_index] += 1
                        pending.append(
                            _TransactionIntent(
                                table_index,
                                target.rows_per_transaction,
                                "update" if is_update else "insert",
                                update_target_index,
                            )
                        )
                        sequence += 1
                        if len(pending) < operations_per_api_batch:
                            continue
                        batch = _ApiBatchIntent(tuple(pending))
                        pending = []
                        try:
                            work_queue.put_nowait(batch)
                        except Full:
                            counters.rejected(len(batch.operations))
                    scheduler_stop.wait(0.001)
            finally:
                counters.rejected(len(pending))

        def work() -> None:
            try:
                worker = worker_factory()
            except BaseException as error:
                counters.setup_failed(error)
                scheduler_stop.set()
                workers_stop.set()
                return
            try:
                while not workers_stop.is_set():
                    try:
                        batch = work_queue.get(timeout=0.02)
                    except Empty:
                        continue
                    if workers_stop.is_set():
                        counters.rejected(len(batch.operations))
                        work_queue.task_done()
                        break
                    stop_worker = False
                    for index, intent in enumerate(batch.operations):
                        counters.started()
                        transaction_start = time.monotonic_ns()
                        abort_batch = False
                        try:
                            if intent.operation == "update":
                                if intent.update_target_index is None:
                                    raise RuntimeError("update target index is missing")
                                affected = worker.update(
                                    intent.table_index,
                                    intent.rows,
                                    intent.update_target_index,
                                )
                            else:
                                affected = worker.write(intent.table_index, intent.rows)
                            if affected != intent.rows:
                                raise RuntimeError(
                                    f"transaction affected {affected} rows; expected {intent.rows}"
                                )
                        except CommittedTrafficWorkerError as error:
                            completed_ns = time.monotonic_ns()
                            counters.committed_with_error(
                                completed_ns,
                                error.committed_rows,
                                (completed_ns - transaction_start) / 1_000_000,
                                error,
                                intent.operation,
                            )
                            scheduler_stop.set()
                            workers_stop.set()
                            stop_worker = True
                            abort_batch = True
                        except FatalTrafficWorkerError as error:
                            counters.failed(error)
                            scheduler_stop.set()
                            workers_stop.set()
                            stop_worker = True
                            abort_batch = True
                        except RuntimeError as error:
                            if str(error).startswith("hot writer parked:"):
                                counters.parked()
                            else:
                                counters.failed(error)
                            abort_batch = True
                        except BaseException as error:
                            counters.failed(error)
                            abort_batch = True
                        else:
                            completed_ns = time.monotonic_ns()
                            counters.committed(
                                completed_ns,
                                affected,
                                (completed_ns - transaction_start) / 1_000_000,
                                intent.operation,
                            )
                        if abort_batch:
                            counters.rejected(len(batch.operations) - index - 1)
                            break
                    work_queue.task_done()
                    if stop_worker:
                        break
            finally:
                try:
                    worker.close()
                except BaseException:
                    pass

        workers = tuple(
            Thread(target=work, name=f"flipbench-traffic-worker-{index}", daemon=True)
            for index in range(initial.worker_count)
        )
        scheduler = Thread(target=schedule, name="flipbench-traffic-scheduler", daemon=True)
        lane = cls(
            scheduler,
            workers,
            scheduler_stop,
            workers_stop,
            work_queue,
            counters,
            live_target,
            operations_per_api_batch,
        )
        for worker in workers:
            worker.start()
        scheduler.start()
        return lane

    def is_alive(self) -> bool:
        return self._scheduler.is_alive() and all(worker.is_alive() for worker in self._workers)

    def snapshot(self) -> TrafficSnapshot:
        target = self._target()
        return self._counters.snapshot(
            time.monotonic_ns(),
            self._queue.qsize() * self._batch_size,
            target.rate_window_seconds,
        )

    def total_inserted(self) -> int:
        return self.snapshot().committed_rows

    def reset_measurement(self) -> None:
        self._counters.reset_measurement(time.monotonic_ns())

    def stop_and_join(self, timeout_seconds: float) -> TrafficSnapshot:
        deadline = time.monotonic() + timeout_seconds
        self.stop_admission(timeout_seconds)
        return self.finish_in_flight(max(0.001, deadline - time.monotonic()))

    def stop_admission(self, timeout_seconds: float) -> TrafficSnapshot:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        deadline = time.monotonic() + timeout_seconds
        self._scheduler_stop.set()
        self._scheduler.join(max(0.0, deadline - time.monotonic()))
        if self._scheduler.is_alive():
            raise TimeoutError("traffic scheduler did not stop")
        cancelled = 0
        while True:
            try:
                batch = self._queue.get_nowait()
            except Empty:
                break
            else:
                cancelled += len(batch.operations)
                self._queue.task_done()
        self._counters.rejected(cancelled)
        return self.snapshot()

    def finish_in_flight(self, timeout_seconds: float) -> TrafficSnapshot:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self._scheduler.is_alive():
            raise RuntimeError("traffic admission must be stopped before joining workers")
        deadline = time.monotonic() + timeout_seconds
        self._workers_stop.set()
        for worker in self._workers:
            worker.join(max(0.0, deadline - time.monotonic()))
        if any(worker.is_alive() for worker in self._workers):
            raise TimeoutError("traffic workers did not stop")
        return self.snapshot()
