import unittest

from flipbench.core import (
    AttemptState,
    GateEvidence,
    StateError,
    TimingError,
    derive_stage_durations,
    derive_revert_durations,
    estimate_queue_latency,
    lag_admission_ready,
    overload_lag_vectors,
    production_admission_ready,
    transition,
)


class StateTests(unittest.TestCase):
    def test_only_guarded_path_can_grant_warm(self) -> None:
        locked = transition(AttemptState.HOT_PRIMARY, AttemptState.LOCKED)
        evidence = GateEvidence(True, True, True, True)
        drained = transition(locked, AttemptState.DRAINED, evidence)
        self.assertEqual(transition(drained, AttemptState.WARM_PRIMARY), AttemptState.WARM_PRIMARY)

    def test_locked_cannot_skip_to_warm(self) -> None:
        with self.assertRaises(StateError):
            transition(AttemptState.LOCKED, AttemptState.WARM_PRIMARY)

    def test_drain_requires_all_four_transfer_proofs_not_full_checksum(self) -> None:
        evidence_values = (True, True, True, True)
        for missing_index in range(4):
            incomplete = tuple(value if index != missing_index else False for index, value in enumerate(evidence_values))
            with self.subTest(missing_index=missing_index), self.assertRaises(StateError):
                transition(AttemptState.LOCKED, AttemptState.DRAINED, GateEvidence(*incomplete))
        with self.assertRaises(StateError):
            transition(AttemptState.LOCKED, AttemptState.DRAINED, None)

    def test_failure_enters_recovery_never_warm(self) -> None:
        self.assertEqual(
            transition(AttemptState.LOCKED, AttemptState.RECOVERING),
            AttemptState.RECOVERING,
        )


class TimingTests(unittest.TestCase):
    def test_hot_fence_timing_keeps_tracker_phase_semantics(self) -> None:
        durations = derive_stage_durations(
            {
                "t1": 10,
                "t2": 20,
                "t2h": 35,
                "t2w": 42,
                "t5": 50,
                "t7": 60,
                "t8": 70,
                "t11": 80,
                "t13": 100,
            }
        )
        self.assertEqual(durations["admission_to_park_ns"], 10)
        self.assertEqual(durations["hot_fence_park_ns"], 15)
        self.assertEqual(durations["tracker_lock_ns"], 7)
        self.assertEqual(durations["writer_park_ns"], 80)

    def test_optimistic_detach_reports_admission_and_in_flight_resolution(self) -> None:
        durations = derive_stage_durations(
            {
                "t1": 10,
                "t2": 20,
                "t2h": 30,
                "t2w": 40,
                "t2f": 45,
                "t2q": 75,
                "t5": 80,
                "t7": 90,
                "t8": 100,
                "t11": 110,
                "t13": 120,
            }
        )
        self.assertEqual(durations["admission_fence_ns"], 5)
        self.assertEqual(durations["in_flight_resolution_ns"], 30)

    def test_optimistic_detach_rejects_reversed_fence_stages(self) -> None:
        with self.assertRaises(TimingError):
            derive_stage_durations(
                {
                    "t1": 10,
                    "t2": 20,
                    "t2h": 30,
                    "t2w": 40,
                    "t2f": 60,
                    "t2q": 50,
                    "t5": 80,
                    "t7": 90,
                    "t8": 100,
                    "t11": 110,
                    "t13": 120,
                }
            )

    def test_production_admission_enforces_upper_lag_bounds(self) -> None:
        self.assertTrue(production_admission_ready(100, {"a:0": 1, "b:0": 2}, 100, 2))
        self.assertFalse(production_admission_ready(101, {"a:0": 1}, 100, 2))
        self.assertFalse(production_admission_ready(100, {"a:0": 3}, 100, 2))
        with self.assertRaises(TimingError):
            production_admission_ready(-1, {"a:0": 0}, 100, 2)

    def test_running_overload_admission_requires_both_lags(self) -> None:
        self.assertTrue(lag_admission_ready(1_000_000, 2_000, 1_000_000, 1_000))
        self.assertFalse(lag_admission_ready(999_999, 2_000, 1_000_000, 1_000))
        self.assertFalse(lag_admission_ready(1_000_000, 999, 1_000_000, 1_000))
        with self.assertRaises(ValueError):
            lag_admission_ready(-1, 1_000, 1, 1)

    def test_overload_lag_vectors_use_baseline_and_committed_hot_rows(self) -> None:
        source, sink = overload_lag_vectors(
            {"a:0": 100, "b:0": 200},
            {"a:0": 130, "b:0": 250},
            {"a:0": 120, "b:0": 245},
            {"a:0": 80, "b:0": 80},
        )
        self.assertEqual(dict(source), {"a:0": 50, "b:0": 30})
        self.assertEqual(dict(sink), {"a:0": 10, "b:0": 5})

    def test_overload_lag_vectors_reject_missing_or_regressing_offsets(self) -> None:
        with self.assertRaises(TimingError):
            overload_lag_vectors({"a:0": 10}, {"a:0": 9}, {"a:0": 9}, {"a:0": 1})
        with self.assertRaises(TimingError):
            overload_lag_vectors({"a:0": 10}, {"a:0": 11}, {}, {"a:0": 1})

    def test_queue_model_uses_net_headroom_and_overlap(self) -> None:
        estimate = estimate_queue_latency(
            source_backlog=5_000,
            sink_backlog=10_000,
            source_capacity=15_000,
            warm_capacity=10_000,
            live_rate=5_000,
            tracker_seconds=0.01,
            detach_seconds=(0.02, 0.03),
            fence_seconds=0.01,
            source_visibility_seconds=0.1,
            capture_seconds=0.02,
            sink_visibility_seconds=0.1,
            grant_seconds=0.01,
        )
        self.assertAlmostEqual(estimate.source_catchup_seconds, 0.5)
        self.assertAlmostEqual(estimate.warm_catchup_seconds, 3.0)
        self.assertAlmostEqual(estimate.total_seconds, 3.13)

    def test_non_converging_rates_fail(self) -> None:
        with self.assertRaises(TimingError):
            estimate_queue_latency(1, 1, 10, 10, 10, 0, (), 0, 0, 0, 0, 0)

    def test_derives_monotonic_stage_durations(self) -> None:
        durations = derive_stage_durations({"t1": 10, "t2": 20, "t5": 30, "t6": 35, "t6w": 40, "t7": 60, "t8": 70, "t11": 100, "t13": 120, "tverify": 170})
        self.assertEqual(durations["tracker_lock_ns"], 10)
        self.assertEqual(durations["source_proof_ns"], 30)
        self.assertEqual(durations["fence_wakeup_ns"], 5)
        self.assertEqual(durations["slot_wait_after_wakeup_ns"], 20)
        self.assertEqual(durations["writer_park_ns"], 100)
        self.assertEqual(durations["validation_ns"], 50)
        self.assertEqual(durations["whole_lifecycle_ns"], 160)

    def test_rejects_clock_reversal(self) -> None:
        with self.assertRaises(TimingError):
            derive_stage_durations({"t1": 20, "t2": 10, "t5": 30, "t7": 60, "t8": 70, "t11": 100, "t13": 120, "tverify": 170})

    def test_derives_revert_writer_park_and_recovery_time(self) -> None:
        durations = derive_revert_durations(
            {"t2": 100, "trevert_start": 220, "trevert_end": 260}
        )
        self.assertEqual(durations["forward_until_failure_ns"], 120)
        self.assertEqual(durations["revert_ns"], 40)
        self.assertEqual(durations["writer_park_ns"], 160)


if __name__ == "__main__":
    unittest.main()
