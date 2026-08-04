from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from flipbench.benchmark_plan import (
    BenchmarkPlanError,
    build_benchmark_cases,
    load_benchmark_plan,
    render_benchmark_report,
)


def valid_plan() -> dict[str, object]:
    return {
        "schema_version": 1,
        "name": "d-e-test",
        "description": "balanced test",
        "variants": ["D", "E"],
        "target_tps": [250, 1000],
        "repetitions": 2,
        "random_seed": 42,
        "table_count": 5,
        "active_percent": 90,
        "workload": {
            "rows_per_transaction": 1,
            "payload_bytes": 256,
            "active_workers": 4,
            "retiring_workers": 2,
            "queue_limit_per_lane": 5000,
            "rate_window_seconds": 5,
            "minimum_achievement_percent": 5,
        },
        "timing": {
            "warmup_seconds": 5,
            "measurement_seconds": 5,
            "sample_interval_seconds": 1,
            "final_healthy_samples": 2,
            "flip_timeout_seconds": 180,
            "restart_timeout_seconds": 900,
        },
        "admission": {
            "max_source_lag_bytes": 536870912,
            "max_sink_lag_records_per_partition": 250000,
            "stable_samples": 2,
            "poll_ms": 50,
            "park_budget_ms": 120000,
            "revert_reserve_ms": 20000,
        },
        "safety": {
            "minimum_free_disk_gib": 5,
            "stop_on_correctness_failure": True,
        },
    }


class BenchmarkPlanTests(unittest.TestCase):
    def load(self, payload: dict[str, object]):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_benchmark_plan(path)

    def test_builds_balanced_deterministic_cases_with_workload_shape(self) -> None:
        plan = self.load(valid_plan())
        first = build_benchmark_cases(plan)
        second = build_benchmark_cases(plan)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 8)
        self.assertEqual(len({case.case_id for case in first}), 8)
        for repetition in (1, 2):
            for target_tps in (250, 1000):
                block = [
                    case
                    for case in first
                    if case.repetition == repetition and case.target_tps == target_tps
                ]
                self.assertEqual({case.variant for case in block}, {"D", "E"})
        by_variant = {case.variant: case for case in first}
        self.assertEqual(by_variant["D"].transaction_shape, "api_batch_separate_commits_v1")
        self.assertEqual(by_variant["D"].tables_per_api_transaction, 1)
        self.assertEqual(by_variant["D"].operations_per_api_batch, 5)
        self.assertEqual(by_variant["D"].ownership_reads_per_api_batch, 5)
        self.assertEqual(by_variant["E"].transaction_shape, "api_batch_separate_commits_v1")
        self.assertEqual(by_variant["E"].tables_per_api_transaction, 1)
        self.assertEqual(by_variant["E"].operations_per_api_batch, 5)
        self.assertEqual(by_variant["E"].ownership_reads_per_api_batch, 1)

    def test_builds_marker_variants_with_explicit_source_proof_modes(self) -> None:
        payload = valid_plan()
        payload["variants"] = ["F", "G"]
        payload["target_tps"] = [5_000]
        payload["repetitions"] = 1
        plan = self.load(payload)

        by_variant = {
            case.variant: case for case in build_benchmark_cases(plan)
        }
        self.assertEqual(by_variant["F"].fence_wakeup_mode, "passive")
        self.assertEqual(by_variant["F"].source_proof_mode, "per_leaf_marker_v1")
        self.assertEqual(
            by_variant["G"].source_proof_mode,
            "atomic_detach_marker_v1",
        )
        self.assertEqual(by_variant["F"].write_fence_mode, "optimistic_detach_v1")
        self.assertEqual(by_variant["G"].write_fence_mode, "optimistic_detach_v1")

    def test_builds_parallel_atomic_marker_variant(self) -> None:
        payload = valid_plan()
        payload["variants"] = ["G", "H"]
        payload["target_tps"] = [3_000]
        payload["repetitions"] = 1
        plan = self.load(payload)

        by_variant = {
            case.variant: case for case in build_benchmark_cases(plan)
        }
        self.assertEqual(
            by_variant["H"].source_proof_mode,
            "parallel_atomic_detach_marker_v1",
        )
        self.assertEqual(by_variant["H"].ownership_reads_per_api_batch, 1)

    def test_rejects_unknown_duplicate_or_unsafe_plan_values(self) -> None:
        for mutate, message in (
            (lambda value: value.update(variants=["D", "unknown"]), "variant"),
            (lambda value: value.update(target_tps=[1000, 1000]), "target_tps"),
            (lambda value: value.update(repetitions=True), "repetitions"),
            (
                lambda value: value["admission"].update(
                    revert_reserve_ms=value["admission"]["park_budget_ms"]
                ),
                "revert",
            ),
            (
                lambda value: value["timing"].update(final_healthy_samples=6),
                "healthy",
            ),
        ):
            with self.subTest(message=message):
                payload = valid_plan()
                mutate(payload)
                with self.assertRaisesRegex(BenchmarkPlanError, message):
                    self.load(payload)

    def test_rejects_extra_fields_and_non_object_json(self) -> None:
        payload = valid_plan()
        payload["surprise"] = True
        with self.assertRaisesRegex(BenchmarkPlanError, "fields"):
            self.load(payload)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.json"
            path.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(BenchmarkPlanError, "object"):
                load_benchmark_plan(path)

    def test_report_aggregates_matched_transaction_shapes(self) -> None:
        plan = self.load(valid_plan())
        cases = build_benchmark_cases(plan)
        observations = [
            {
                "case_id": case.case_id,
                "variant": case.variant,
                "target_tps": case.target_tps,
                "repetition": case.repetition,
                "transaction_shape": case.transaction_shape,
                "tables_per_api_transaction": case.tables_per_api_transaction,
                "operations_per_api_batch": case.operations_per_api_batch,
                "ownership_reads_per_api_batch": case.ownership_reads_per_api_batch,
                "flip_status": "succeeded",
                "verification_outcome": "passed",
                "capacity": {"median_achieved_tps": float(case.target_tps), "capacity_label": "sustainable"},
                "durations_ns": {"writer_park_ns": 1_000_000, "source_proof_ns": 800_000},
            }
            for case in cases
        ]
        report = render_benchmark_report(plan, observations, "abc123")
        self.assertIn("api_batch_separate_commits_v1", report)
        self.assertNotIn("not directly comparable", report)
        self.assertIn("Median writer park", report)


if __name__ == "__main__":
    unittest.main()
