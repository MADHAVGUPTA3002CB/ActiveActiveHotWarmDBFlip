from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Generic, Hashable, Sequence, TypeVar


T = TypeVar("T", bound=Hashable)


@dataclass(frozen=True, slots=True)
class ParallelDetachSuccess(Generic[T]):
    item: T
    duration_ns: int


@dataclass(frozen=True, slots=True)
class ParallelDetachFailure(Generic[T]):
    item: T
    error: str


class ParallelDetachError(RuntimeError, Generic[T]):
    def __init__(
        self,
        succeeded: tuple[ParallelDetachSuccess[T], ...],
        failed: tuple[ParallelDetachFailure[T], ...],
    ) -> None:
        self.succeeded = succeeded
        self.failed = failed
        leaves = ", ".join(str(item.item) for item in failed)
        super().__init__(f"parallel detach failed for: {leaves}")


def _timed(item: T, worker: Callable[[T], None]) -> ParallelDetachSuccess[T]:
    started_ns = time.perf_counter_ns()
    worker(item)
    return ParallelDetachSuccess(item, time.perf_counter_ns() - started_ns)


def run_all_parallel(
    items: Sequence[T],
    worker: Callable[[T], None],
) -> tuple[ParallelDetachSuccess[T], ...]:
    """Run one independent transaction per item and await every terminal result."""
    immutable_items = tuple(items)
    if not immutable_items:
        raise ValueError("parallel detach requires at least one leaf")
    if len(set(immutable_items)) != len(immutable_items):
        raise ValueError("parallel detach leaves must be unique")

    succeeded: tuple[ParallelDetachSuccess[T], ...] = ()
    failed: tuple[ParallelDetachFailure[T], ...] = ()
    order = {item: index for index, item in enumerate(immutable_items)}
    with ThreadPoolExecutor(
        max_workers=len(immutable_items),
        thread_name_prefix="flipbench-detach",
    ) as executor:
        futures = {
            executor.submit(_timed, item, worker): item
            for item in immutable_items
        }
        for future in as_completed(futures):
            item = futures[future]
            try:
                result = future.result()
            except BaseException as error:
                failed = (
                    *failed,
                    ParallelDetachFailure(
                        item,
                        f"{type(error).__name__}: {error}",
                    ),
                )
            else:
                succeeded = (*succeeded, result)

    ordered_success = tuple(sorted(succeeded, key=lambda item: order[item.item]))
    ordered_failure = tuple(sorted(failed, key=lambda item: order[item.item]))
    if ordered_failure:
        raise ParallelDetachError(ordered_success, ordered_failure)
    return ordered_success
