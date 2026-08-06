from __future__ import annotations

import unittest

from flipbench.matrix import (
    MatrixObservation,
    build_full_matrix,
    classify_capacity,
    variant_dimensions,
    summarize_observations,
)


class FullMatrixPlanTests(unittest.TestCase):
    def test_builds_balanced_missing_cell_and_four_rate_matrix(self) -> None:
        cases = build_full_matrix()
        self.assertEqual(len(cases), 17)
        self.assertEqual(
            (cases[0].variant, cases[0].target_tps),
            ("D", 1_000),
        )
        for target in (2_000, 3_000, 5_000, 15_000):
            self.assertEqual(
                {case.variant for case in cases if case.target_tps == target},
                {"A", "B", "B+", "D"},
            )
        for case in cases:
            self.assertEqual(case.active_target_tps + case.retiring_target_tps, case.target_tps)
            self.assertEqual(case.retiring_target_tps, case.target_tps // 10)

    def test_maps_variant_dimensions_exactly(self) -> None:
        by_variant = {
            case.variant: case
            for case in build_full_matrix()
            if case.target_tps == 2_000
        }
        self.assertEqual(
            (by_variant["A"].source_topology, by_variant["A"].fence_wakeup_mode,
             by_variant["A"].write_fence_mode),
            ("shared", "passive", "optimistic_detach_v1"),
        )
        self.assertEqual(
            (by_variant["B"].source_topology, by_variant["B"].fence_wakeup_mode,
             by_variant["B"].write_fence_mode),
            ("isolated", "passive", "warm_tracker_advisory_v1"),
        )
        self.assertEqual(
            (by_variant["B+"].source_topology, by_variant["B+"].fence_wakeup_mode,
             by_variant["B+"].write_fence_mode),
            ("isolated", "immediate_heartbeat", "warm_tracker_advisory_v1"),
        )
        self.assertEqual(
            (by_variant["D"].source_topology, by_variant["D"].fence_wakeup_mode,
             by_variant["D"].write_fence_mode),
            ("isolated", "immediate_heartbeat", "hot_transactional_v1"),
        )
        self.assertEqual(
            variant_dimensions("E"),
            (
                "isolated",
                "immediate_heartbeat",
                "optimistic_detach_v1",
                "slot_lsn_v1",
            ),
        )
        self.assertEqual(
            variant_dimensions("F"),
            (
                "isolated",
                "passive",
                "optimistic_detach_v1",
                "per_leaf_marker_v1",
            ),
        )
        self.assertEqual(
            variant_dimensions("G"),
            (
                "isolated",
                "passive",
                "optimistic_detach_v1",
                "atomic_detach_marker_v1",
            ),
        )
        self.assertEqual(
            variant_dimensions("H"),
            (
                "isolated",
                "passive",
                "optimistic_detach_v1",
                "parallel_atomic_detach_marker_v1",
            ),
        )
        self.assertEqual(
            variant_dimensions("H-Prod"),
            (
                "shared",
                "passive",
                "optimistic_detach_v1",
                "parallel_atomic_detach_marker_v1",
            ),
        )


class CapacityClassificationTests(unittest.TestCase):
    def test_classifies_sustainable_constrained_overloaded_and_severe(self) -> None:
        self.assertEqual(classify_capacity(95, 0.5, 10, 5_000), "sustainable")
        self.assertEqual(classify_capacity(75, 5, 400, 5_000), "constrained")
        self.assertEqual(classify_capacity(45, 20, 4_900, 5_000), "overloaded")
        self.assertEqual(classify_capacity(15, 60, 5_000, 5_000), "severely_overloaded")

    def test_summarizes_steady_window_without_using_one_spike_as_maximum(self) -> None:
        observations = tuple(
            MatrixObservation(
                achieved_tps=value,
                active_tps=value * 0.9,
                retiring_tps=value * 0.1,
                scheduled_transactions=scheduled,
                rejected_transactions=rejected,
                queue_depth=queue,
            )
            for value, scheduled, rejected, queue in (
                (2_000, 10_000, 10, 5),
                (2_100, 11_000, 20, 10),
                (2_200, 12_000, 30, 15),
                (9_999, 13_000, 40, 20),
            )
        )
        summary = summarize_observations(observations, target_tps=3_000, queue_limit=5_000)
        self.assertEqual(summary["peak_observed_tps"], 9_999)
        self.assertEqual(summary["median_achieved_tps"], 2_150)
        self.assertGreater(summary["p95_achieved_tps"], 2_200)
        self.assertEqual(summary["capacity_label"], "constrained")


if __name__ == "__main__":
    unittest.main()
