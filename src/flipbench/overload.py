from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread
from typing import Callable


class _CommittedCounter:
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
class BackgroundBatchWriter:
    _thread: Thread
    _stop: Event
    _counter: _CommittedCounter
    _errors: SimpleQueue[BaseException]

    @classmethod
    def start(cls, write_batch: Callable[[], int], max_batches: int) -> BackgroundBatchWriter:
        if not callable(write_batch):
            raise TypeError("write_batch must be callable")
        if not isinstance(max_batches, int) or isinstance(max_batches, bool) or max_batches <= 0:
            raise ValueError("max_batches must be a positive integer")

        stop = Event()
        counter = _CommittedCounter()
        errors: SimpleQueue[BaseException] = SimpleQueue()

        def run() -> None:
            try:
                for _ in range(max_batches):
                    if stop.is_set():
                        return
                    batch_count = write_batch()
                    if not isinstance(batch_count, int) or isinstance(batch_count, bool) or batch_count <= 0:
                        raise ValueError("write_batch must return a positive integer")
                    counter.add(batch_count)
                if not stop.is_set():
                    errors.put(RuntimeError("overload writer exhausted maximum batches before ownership lock"))
            except RuntimeError as error:
                if str(error).startswith("hot writer parked:"):
                    return
                errors.put(error)
            except BaseException as error:
                errors.put(error)

        thread = Thread(target=run, name="flipbench-overload-writer", daemon=True)
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
            raise TimeoutError("background overload writer did not stop")
        try:
            error = self._errors.get_nowait()
        except Empty:
            return self.total_inserted()
        raise RuntimeError(str(error)) from error
