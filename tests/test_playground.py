from __future__ import annotations

import time
import unittest
from unittest.mock import Mock

from flipbench.playground import (
    AdmissionThresholds,
    AdmissionWindow,
    FlipStartRequest,
    LiveBatchWriter,
    LiveWorkload,
    WorkloadSettings,
    validate_local_batch_budget,
    workload_progress_valid,
)
from flipbench.core import FenceWakeupMode, SourceProofMode


class FlipStartRequestTests(unittest.TestCase):
    def test_defaults_to_passive_and_accepts_immediate_heartbeat(self) -> None:
        self.assertEqual(
            FlipStartRequest.from_payload({}).fence_wakeup_mode,
            FenceWakeupMode.PASSIVE,
        )
        self.assertEqual(
            FlipStartRequest.from_payload({}).source_proof_mode,
            SourceProofMode.SLOT_LSN,
        )
        self.assertEqual(
            FlipStartRequest.from_payload(
                {"fence_wakeup_mode": "immediate_heartbeat"}
            ).fence_wakeup_mode,
            FenceWakeupMode.IMMEDIATE_HEARTBEAT,
        )
        self.assertEqual(
            FlipStartRequest.from_payload(
                {"source_proof_mode": "per_leaf_marker_v1"}
            ).source_proof_mode,
            SourceProofMode.PER_LEAF_MARKER,
        )
        self.assertEqual(
            FlipStartRequest.from_payload(
                {"source_proof_mode": "atomic_detach_marker_v1"}
            ).source_proof_mode,
            SourceProofMode.ATOMIC_DETACH_MARKER,
        )
        self.assertEqual(
            FlipStartRequest.from_payload(
                {"source_proof_mode": "parallel_atomic_detach_marker_v1"}
            ).source_proof_mode,
            SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
        )

    def test_rejects_unknown_or_mistyped_flip_options(self) -> None:
        for payload in (
            {"fence_wakeup_mode": "fast"},
            {"fence_wakeup_mode": True},
            {"fence_wakeup_mode": None},
            {"source_proof_mode": "unsafe"},
            {"source_proof_mode": True},
            {"fence_wakeup_mode": "passive", "table": "dbz_heartbeat"},
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                FlipStartRequest.from_payload(payload)


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

    def test_target_rate_mode_enforces_aggregate_local_resource_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "total target TPS"):
            WorkloadSettings(
                mode="target_rate_v1",
                active_target_tps=60_000,
                retiring_target_tps=50_000,
            )
        with self.assertRaisesRegex(ValueError, "total workers"):
            WorkloadSettings(
                mode="target_rate_v1",
                active_workers=40,
                retiring_workers=25,
            )
        with self.assertRaisesRegex(ValueError, "queue size"):
            WorkloadSettings(mode="target_rate_v1", max_queue_size=100_001)
        with self.assertRaisesRegex(ValueError, "in-flight payload"):
            WorkloadSettings(
                mode="target_rate_v1",
                active_workers=32,
                retiring_workers=8,
                active_rows_per_transaction=128,
                retiring_rows_per_transaction=128,
                payload_bytes=65_536,
            )
        with self.assertRaisesRegex(ValueError, "in-flight rows"):
            WorkloadSettings(
                mode="target_rate_v1",
                active_workers=32,
                retiring_workers=32,
                active_rows_per_transaction=100_000,
                retiring_rows_per_transaction=100_000,
                payload_bytes=16,
            )

    def test_serializes_hot_local_transaction_write_fence_mode(self) -> None:
        settings = WorkloadSettings(
            mode="target_rate_v1",
            write_fence_mode="hot_transactional_v1",
        )

        self.assertEqual(settings.write_fence_mode, "hot_transactional_v1")
        self.assertEqual(settings.to_dict()["write_fence_mode"], "hot_transactional_v1")

    def test_serializes_optimistic_detach_write_fence_mode(self) -> None:
        settings = WorkloadSettings(
            mode="target_rate_v1",
            write_fence_mode="optimistic_detach_v1",
        )

        self.assertEqual(settings.write_fence_mode, "optimistic_detach_v1")
        self.assertEqual(settings.to_dict()["write_fence_mode"], "optimistic_detach_v1")

    def test_accepts_h_state_only_batch_admission_for_optimistic_detach(self) -> None:
        settings = WorkloadSettings(
            mode="target_rate_v1",
            write_fence_mode="optimistic_detach_v1",
            optimistic_admission_check_mode="state_only_v1",
        )

        self.assertEqual(settings.optimistic_admission_check_mode, "state_only_v1")
        self.assertEqual(
            settings.to_dict()["optimistic_admission_check_mode"],
            "state_only_v1",
        )

    def test_rejects_state_only_admission_without_optimistic_detach(self) -> None:
        with self.assertRaisesRegex(ValueError, "state_only_v1"):
            WorkloadSettings(
                mode="target_rate_v1",
                write_fence_mode="hot_transactional_v1",
                optimistic_admission_check_mode="state_only_v1",
            )

    def test_rejects_unknown_or_mistyped_write_fence_mode(self) -> None:
        for value in ("warm_remote", "", True, None):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "write_fence_mode"
            ):
                WorkloadSettings(
                    mode="target_rate_v1",
                    write_fence_mode=value,  # type: ignore[arg-type]
                )


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

    def test_target_rate_mode_uses_rows_per_transaction_for_budget(self) -> None:
        settings = WorkloadSettings(
            mode="target_rate_v1",
            active_target_tps=13_636,
            retiring_target_tps=1_364,
            active_rows_per_transaction=2,
            retiring_rows_per_transaction=1,
            active_workers=8,
            retiring_workers=2,
            max_queue_size=15_000,
            payload_bytes=256,
        )
        self.assertEqual(settings.total_target_tps, 15_000)
        validate_local_batch_budget(settings, 20)

    def test_target_rate_mode_rejects_invalid_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "mode"):
            WorkloadSettings(mode="unknown")
        with self.assertRaisesRegex(ValueError, "active_target_tps"):
            WorkloadSettings(mode="target_rate_v1", active_target_tps=0)

    def test_target_rate_progress_does_not_require_active_rows_to_be_larger(self) -> None:
        target = WorkloadSettings(
            mode="target_rate_v1",
            active_target_tps=100,
            retiring_target_tps=200,
        )
        self.assertTrue(workload_progress_valid(target, active_rows=1, retiring_rows=2))
        self.assertFalse(workload_progress_valid(target, active_rows=0, retiring_rows=2))
        self.assertFalse(
            workload_progress_valid(
                WorkloadSettings(mode="legacy_batch"),
                active_rows=1,
                retiring_rows=2,
            )
        )


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
        while (
            workload.active_total() == 0 or workload.retiring_total() == 0
        ) and time.monotonic() < deadline:
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

    def test_running_is_true_when_only_retiring_lane_survives(self) -> None:
        active = Mock()
        active.is_alive.return_value = False
        retiring = Mock()
        retiring.is_alive.return_value = True
        workload = LiveWorkload(WorkloadSettings())
        workload._active = active
        workload._retiring = retiring
        self.assertTrue(workload.running())

    def test_target_rate_update_resets_measurement_windows(self) -> None:
        active = Mock()
        retiring = Mock()
        active.is_alive.return_value = True
        retiring.is_alive.return_value = True
        workload = LiveWorkload(WorkloadSettings(mode="target_rate_v1"))
        workload._active = active
        workload._retiring = retiring
        workload.update(
            WorkloadSettings(mode="target_rate_v1", active_target_tps=12_000)
        )
        active.reset_measurement.assert_called_once_with()
        retiring.reset_measurement.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
