from __future__ import annotations

import time
import unittest
from threading import Event, Thread

from flipbench.traffic import (
    CommittedTrafficWorkerError,
    PacedArrivals,
    TrafficLane,
    TrafficTarget,
)


class TrafficTargetTests(unittest.TestCase):
    def test_validates_immutable_target(self) -> None:
        target = TrafficTarget(
            target_tps=15_000,
            rows_per_transaction=1,
            worker_count=32,
            max_queue_size=30_000,
        )
        self.assertEqual(target.target_tps, 15_000)
        with self.assertRaisesRegex(ValueError, "target_tps"):
            TrafficTarget(0, 1, 1, 1)
        with self.assertRaisesRegex(ValueError, "rows_per_transaction"):
            TrafficTarget(1, 0, 1, 1)
        with self.assertRaisesRegex(ValueError, "worker_count"):
            TrafficTarget(1, 1, 0, 1)
        with self.assertRaisesRegex(ValueError, "max_queue_size"):
            TrafficTarget(1, 1, 1, 0)
        with self.assertRaisesRegex(ValueError, "update_percent"):
            TrafficTarget(1, 1, 1, 1, update_percent=-1)
        with self.assertRaisesRegex(ValueError, "update_percent"):
            TrafficTarget(1, 1, 1, 1, update_percent=101)

    def test_rejects_booleans_as_numeric_configuration(self) -> None:
        with self.assertRaises(ValueError):
            TrafficTarget(True, 1, 1, 1)  # type: ignore[arg-type]


class PacedArrivalsTests(unittest.TestCase):
    def test_emits_exact_rate_for_one_second(self) -> None:
        pacer = PacedArrivals(start_ns=0)
        emitted, missed = pacer.advance(
            now_ns=1_000_000_000,
            target_tps=1_000,
            max_burst=1_000,
        )
        self.assertEqual((emitted, missed), (1_000, 0))

    def test_caps_catch_up_burst_and_reports_missed_arrivals(self) -> None:
        pacer = PacedArrivals(start_ns=0)
        emitted, missed = pacer.advance(
            now_ns=1_000_000_000,
            target_tps=15_000,
            max_burst=150,
        )
        self.assertEqual(emitted, 150)
        self.assertEqual(missed, 14_850)

    def test_rate_change_only_applies_to_future_time(self) -> None:
        pacer = PacedArrivals(start_ns=0)
        self.assertEqual(pacer.advance(500_000_000, 100, 100), (50, 0))
        self.assertEqual(pacer.advance(1_000_000_000, 200, 200), (100, 0))


class _Worker:
    def __init__(
        self,
        committed: list[tuple[int, int]],
        block: Event | None = None,
        operations: list[tuple[str, int, int | None]] | None = None,
    ) -> None:
        self.committed = committed
        self.block = block
        self.operations = operations
        self.closed = False

    def write(self, table_index: int, rows: int) -> int:
        if self.block is not None:
            self.block.wait(1)
        self.committed.append((table_index, rows))
        if self.operations is not None:
            self.operations.append(("insert", table_index, None))
        return rows

    def update(self, table_index: int, rows: int, target_index: int) -> int:
        if self.block is not None:
            self.block.wait(1)
        self.committed.append((table_index, rows))
        if self.operations is not None:
            self.operations.append(("update", table_index, target_index))
        return rows

    def close(self) -> None:
        self.closed = True


class _FatalAfterCommitWorker(_Worker):
    def write(self, table_index: int, rows: int) -> int:
        raise CommittedTrafficWorkerError(rows, "lifecycle guard release failed")


class TrafficLaneTests(unittest.TestCase):
    def test_schedules_a_deterministic_fifty_fifty_insert_update_mix(self) -> None:
        committed: list[tuple[int, int]] = []
        operations: list[tuple[str, int, int | None]] = []
        target = TrafficTarget(200, 1, 1, 100, update_percent=50)
        lane = TrafficLane.start(
            lambda: _Worker(committed, operations=operations),
            lambda: target,
            table_count=1,
        )
        deadline = time.monotonic() + 1
        while len(operations) < 10 and time.monotonic() < deadline:
            time.sleep(0.002)
        snapshot = lane.stop_and_join(1)

        self.assertEqual(
            operations[:10],
            [
                ("insert", 0, None),
                ("update", 0, 0),
                ("insert", 0, None),
                ("update", 0, 1),
                ("insert", 0, None),
                ("update", 0, 2),
                ("insert", 0, None),
                ("update", 0, 3),
                ("insert", 0, None),
                ("update", 0, 4),
            ],
        )
        self.assertGreaterEqual(snapshot.committed_insert_transactions, 5)
        self.assertGreaterEqual(snapshot.committed_update_transactions, 5)

    def test_update_targets_rotate_independently_for_each_table(self) -> None:
        committed: list[tuple[int, int]] = []
        operations: list[tuple[str, int, int | None]] = []
        target = TrafficTarget(500, 1, 1, 100, update_percent=100)
        lane = TrafficLane.start(
            lambda: _Worker(committed, operations=operations),
            lambda: target,
            table_count=2,
        )
        deadline = time.monotonic() + 1
        while len(operations) < 6 and time.monotonic() < deadline:
            time.sleep(0.002)
        lane.stop_and_join(1)

        self.assertEqual(
            operations[:6],
            [
                ("update", 0, 0),
                ("update", 1, 0),
                ("update", 0, 1),
                ("update", 1, 1),
                ("update", 0, 2),
                ("update", 1, 2),
            ],
        )

    def test_default_target_remains_insert_only(self) -> None:
        committed: list[tuple[int, int]] = []
        operations: list[tuple[str, int, int | None]] = []
        target = TrafficTarget(200, 1, 1, 100)
        lane = TrafficLane.start(
            lambda: _Worker(committed, operations=operations),
            lambda: target,
            table_count=1,
        )
        deadline = time.monotonic() + 1
        while len(operations) < 5 and time.monotonic() < deadline:
            time.sleep(0.002)
        snapshot = lane.stop_and_join(1)

        self.assertTrue(all(kind == "insert" for kind, _, _ in operations))
        self.assertEqual(snapshot.committed_update_transactions, 0)

    def test_api_batch_must_fit_in_configured_queue(self) -> None:
        target = TrafficTarget(100, 1, 1, 4)
        with self.assertRaisesRegex(ValueError, "batch must fit"):
            TrafficLane.start(
                lambda: _Worker([]),
                lambda: target,
                table_count=5,
                operations_per_api_batch=5,
            )

    def test_admitted_api_batch_stays_on_one_worker_and_finishes_after_admission_stops(self) -> None:
        committed: list[tuple[int, int]] = []
        release = Event()
        target = TrafficTarget(1_000, 1, 1, 100)
        lane = TrafficLane.start(
            lambda: _Worker(committed, release),
            lambda: target,
            table_count=5,
            operations_per_api_batch=5,
        )
        deadline = time.monotonic() + 1
        while lane.snapshot().in_flight_transactions == 0 and time.monotonic() < deadline:
            time.sleep(0.002)
        lane.stop_admission(1)
        release.set()
        snapshot = lane.finish_in_flight(1)
        self.assertEqual(committed, [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1)])
        self.assertEqual(snapshot.committed_transactions, 5)

    def test_worker_start_failure_is_visible_and_lane_is_unhealthy(self) -> None:
        def fail():
            raise RuntimeError("connection refused")

        target = TrafficTarget(10, 1, 1, 10)
        lane = TrafficLane.start(fail, lambda: target, table_count=1)
        deadline = time.monotonic() + 1
        while lane.is_alive() and time.monotonic() < deadline:
            time.sleep(0.002)
        snapshot = lane.stop_and_join(1)
        self.assertEqual(snapshot.failed_transactions, 1)
        self.assertIn("connection refused", snapshot.last_error or "")

    def test_counts_commits_and_distributes_transactions_across_tables(self) -> None:
        committed: list[tuple[int, int]] = []
        target = TrafficTarget(200, 3, 2, 100)
        lane = TrafficLane.start(lambda: _Worker(committed), lambda: target, table_count=5)
        deadline = time.monotonic() + 1
        while lane.snapshot().committed_transactions < 10 and time.monotonic() < deadline:
            time.sleep(0.005)
        snapshot = lane.stop_and_join(1)
        self.assertGreaterEqual(snapshot.committed_transactions, 10)
        self.assertEqual(snapshot.committed_rows, snapshot.committed_transactions * 3)
        self.assertEqual(set(index for index, _ in committed[:5]), {0, 1, 2, 3, 4})
        self.assertEqual(snapshot.in_flight_transactions, 0)
        self.assertEqual(snapshot.queue_depth, 0)

    def test_quiescence_cancels_pending_work_and_waits_for_in_flight_commit(self) -> None:
        committed: list[tuple[int, int]] = []
        release = Event()
        target = TrafficTarget(10_000, 1, 1, 20)
        lane = TrafficLane.start(
            lambda: _Worker(committed, release),
            lambda: target,
            table_count=2,
        )
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            current = lane.snapshot()
            if current.in_flight_transactions > 0 and current.queue_depth > 0:
                break
            time.sleep(0.002)
        result = []
        stopper = Thread(target=lambda: result.append(lane.stop_and_join(1)))
        stopper.start()
        time.sleep(0.01)
        release.set()
        stopper.join(1)
        self.assertFalse(stopper.is_alive())
        snapshot = result[0]
        committed_at_quiescence = snapshot.committed_transactions
        time.sleep(0.01)
        self.assertEqual(lane.snapshot().committed_transactions, committed_at_quiescence)
        self.assertEqual(snapshot.in_flight_transactions, 0)
        self.assertEqual(snapshot.queue_depth, 0)
        self.assertGreater(snapshot.rejected_transactions, 0)

    def test_optimistic_fence_stops_admission_before_waiting_for_in_flight(self) -> None:
        committed: list[tuple[int, int]] = []
        release = Event()
        target = TrafficTarget(10_000, 1, 1, 20)
        lane = TrafficLane.start(
            lambda: _Worker(committed, release),
            lambda: target,
            table_count=2,
        )
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            current = lane.snapshot()
            if current.in_flight_transactions > 0 and current.queue_depth > 0:
                break
            time.sleep(0.002)

        fenced = lane.stop_admission(1)
        self.assertGreater(fenced.in_flight_transactions, 0)
        self.assertEqual(fenced.queue_depth, 0)

        results = []
        joiner = Thread(target=lambda: results.append(lane.finish_in_flight(1)))
        joiner.start()
        time.sleep(0.01)
        self.assertTrue(joiner.is_alive())
        release.set()
        joiner.join(1)

        self.assertFalse(joiner.is_alive())
        self.assertEqual(results[0].in_flight_transactions, 0)
        self.assertEqual(results[0].queue_depth, 0)

    def test_reset_measurement_discards_old_rate_but_preserves_totals(self) -> None:
        target = TrafficTarget(500, 1, 1, 100)
        lane = TrafficLane.start(lambda: _Worker([]), lambda: target, table_count=1)
        deadline = time.monotonic() + 1
        while lane.snapshot().committed_transactions < 5 and time.monotonic() < deadline:
            time.sleep(0.002)
        before = lane.snapshot()
        lane.reset_measurement()
        after = lane.snapshot()
        self.assertGreaterEqual(before.committed_transactions, 5)
        self.assertEqual(after.committed_transactions, before.committed_transactions)
        self.assertEqual(after.committed_tps, 0.0)
        lane.stop_and_join(1)

    def test_committed_cleanup_failure_is_counted_and_stops_the_lane(self) -> None:
        target = TrafficTarget(100, 2, 1, 10)
        lane = TrafficLane.start(
            lambda: _FatalAfterCommitWorker([]), lambda: target, table_count=1
        )
        deadline = time.monotonic() + 1
        while lane.is_alive() and time.monotonic() < deadline:
            time.sleep(0.002)
        snapshot = lane.stop_and_join(1)
        self.assertEqual(snapshot.committed_transactions, 1)
        self.assertEqual(snapshot.committed_rows, 2)
        self.assertEqual(snapshot.failed_transactions, 0)
        self.assertIn("guard release failed", snapshot.last_error or "")


if __name__ == "__main__":
    unittest.main()
