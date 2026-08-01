from __future__ import annotations

import time
import unittest

from flipbench.playground import (
    AdmissionThresholds,
    AdmissionWindow,
    LiveBatchWriter,
    LiveWorkload,
    WorkloadSettings,
    validate_local_batch_budget,
)


class WorkloadSettingsTests(unittest.TestCase):
    def test_accepts_production_shaped_settings(self) -> None:
        settings = WorkloadSettings(100, 10, 5, 10, 1024)
        self.assertEqual(settings.active_rows_per_partition, 100)
        self.assertEqual(settings.to_dict()["payload_bytes"], 1024)

    def test_rejects_invalid_or_excessive_values(self) -> None:
        invalid = (
            (0, 1, 1, 1, 1),
            (1, -1, 1, 1, 1),
            (100_001, 1, 1, 1, 1),
            (1, 1, -1, 1, 1),
            (1, 1, 1, 60_001, 1),
            (1, 1, 1, 1, 15),
            (1, 1, 1, 1, 65_537),
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(ValueError):
                WorkloadSettings(*values)


class AdmissionThresholdTests(unittest.TestCase):
    def test_requires_consecutive_healthy_samples(self) -> None:
        thresholds = AdmissionThresholds(1000, 5, 3, 5000, 500, 50)
        window = AdmissionWindow(thresholds)
        self.assertFalse(window.observe(900, {"a": 4, "b": 5}))
        self.assertFalse(window.observe(1001, {"a": 4, "b": 5}))
        self.assertFalse(window.observe(900, {"a": 4, "b": 5}))
        self.assertFalse(window.observe(900, {"a": 4, "b": 5}))
        self.assertTrue(window.observe(900, {"a": 4, "b": 5}))
        self.assertEqual(window.healthy_samples, 3)
        self.assertEqual(thresholds.to_dict()["stable_samples"], 3)
        window.reset()
        self.assertFalse(window.ready)
        self.assertEqual(window.healthy_samples, 0)

    def test_rejects_unsafe_budget_or_empty_lag_vector(self) -> None:
        with self.assertRaises(ValueError):
            AdmissionThresholds(1, 1, 1, 500, 500, 50)
        window = AdmissionWindow(AdmissionThresholds(1, 1, 1, 501, 500, 50))
        with self.assertRaises(ValueError):
            window.observe(0, {})

    def test_rejects_batches_larger_than_local_memory_budget(self) -> None:
        validate_local_batch_budget(WorkloadSettings(100, 10, 1, 1, 256), 20)
        with self.assertRaisesRegex(ValueError, "16 MiB"):
            validate_local_batch_budget(WorkloadSettings(10_000, 1, 1, 1, 65_536), 20)


class LiveBatchWriterTests(unittest.TestCase):
    def test_reads_new_batch_size_without_restart(self) -> None:
        batch_size = {"value": 2}
        calls: list[int] = []

        def write_batch(rows: int) -> int:
            calls.append(rows)
            time.sleep(0.002)
            return rows * 5

        writer = LiveBatchWriter.start(write_batch, lambda: (batch_size["value"], 0.001))
        deadline = time.monotonic() + 1
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.002)
        batch_size["value"] = 7
        while 7 not in calls and time.monotonic() < deadline:
            time.sleep(0.002)
        total = writer.stop_and_join(1)
        self.assertIn(2, calls)
        self.assertIn(7, calls)
        self.assertEqual(total, sum(calls) * 5)

    def test_surfaces_writer_errors(self) -> None:
        def fail(_: int) -> int:
            raise RuntimeError("database unavailable")

        writer = LiveBatchWriter.start(fail, lambda: (1, 0))
        deadline = time.monotonic() + 1
        while writer.is_alive() and time.monotonic() < deadline:
            time.sleep(0.002)
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            writer.stop_and_join(1)

    def test_validates_callbacks_and_join_timeout(self) -> None:
        with self.assertRaises(TypeError):
            LiveBatchWriter.start(None, lambda: (1, 0))  # type: ignore[arg-type]
        entered = False

        def blocked(_: int) -> int:
            nonlocal entered
            entered = True
            time.sleep(0.05)
            return 1

        writer = LiveBatchWriter.start(blocked, lambda: (1, 0))
        deadline = time.monotonic() + 1
        while not entered and time.monotonic() < deadline:
            time.sleep(0.001)
        with self.assertRaises(TimeoutError):
            writer.stop_and_join(0.001)
        writer.stop_and_join(1)


class LiveWorkloadTests(unittest.TestCase):
    def test_updates_running_writers_and_stops_independently(self) -> None:
        workload = LiveWorkload(WorkloadSettings(2, 1, 1, 1, 32))

        def write(rows: int) -> int:
            time.sleep(0.001)
            return rows * 5

        workload.start(write, write)
        deadline = time.monotonic() + 1
        while workload.retiring_total() == 0 and time.monotonic() < deadline:
            time.sleep(0.002)
        self.assertTrue(workload.running())
        self.assertTrue(workload.active_is_alive())
        self.assertTrue(workload.retiring_is_alive())
        self.assertGreater(workload.active_total(), 0)
        self.assertGreater(workload.retiring_total(), 0)
        updated = workload.update(WorkloadSettings(4, 3, 1, 1, 32))
        self.assertEqual(updated.active_rows_per_partition, 4)
        retiring_total = workload.stop_retiring(1)
        self.assertGreater(retiring_total, 0)
        self.assertTrue(workload.active_is_alive())
        self.assertFalse(workload.retiring_is_alive())
        workload.stop_all(1)
        self.assertFalse(workload.running())

    def test_rejects_duplicate_start(self) -> None:
        workload = LiveWorkload(WorkloadSettings())
        workload.start(lambda rows: rows, lambda rows: rows)
        try:
            with self.assertRaisesRegex(RuntimeError, "already running"):
                workload.start(lambda rows: rows, lambda rows: rows)
        finally:
            workload.stop_all(1)


if __name__ == "__main__":
    unittest.main()
