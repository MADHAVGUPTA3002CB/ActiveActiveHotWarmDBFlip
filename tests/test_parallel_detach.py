from __future__ import annotations

import threading
import unittest


class ParallelDetachTests(unittest.TestCase):
    def test_starts_every_leaf_before_any_worker_finishes(self) -> None:
        from flipbench.parallel_detach import run_all_parallel

        items = tuple(range(5))
        barrier = threading.Barrier(len(items), timeout=2)

        def worker(item: int) -> None:
            barrier.wait()

        result = run_all_parallel(items, worker)

        self.assertEqual(tuple(item.item for item in result), items)
        self.assertTrue(all(item.duration_ns >= 0 for item in result))

    def test_waits_for_all_workers_and_reports_partial_success(self) -> None:
        from flipbench.parallel_detach import ParallelDetachError, run_all_parallel

        completed: set[int] = set()
        completed_lock = threading.Lock()
        barrier = threading.Barrier(3, timeout=2)

        def worker(item: int) -> None:
            barrier.wait()
            if item == 1:
                raise RuntimeError("forced leaf failure")
            with completed_lock:
                completed.add(item)

        with self.assertRaises(ParallelDetachError) as raised:
            run_all_parallel((0, 1, 2), worker)

        self.assertEqual(completed, {0, 2})
        self.assertEqual(tuple(item.item for item in raised.exception.succeeded), (0, 2))
        self.assertEqual(tuple(item.item for item in raised.exception.failed), (1,))
        self.assertIn("forced leaf failure", raised.exception.failed[0].error)

    def test_rejects_empty_or_duplicate_work(self) -> None:
        from flipbench.parallel_detach import run_all_parallel

        with self.assertRaisesRegex(ValueError, "at least one"):
            run_all_parallel((), lambda _item: None)
        with self.assertRaisesRegex(ValueError, "unique"):
            run_all_parallel(("leaf", "leaf"), lambda _item: None)


if __name__ == "__main__":
    unittest.main()
