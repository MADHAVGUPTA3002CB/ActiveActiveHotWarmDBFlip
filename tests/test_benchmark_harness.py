from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flipbench.benchmark_plan import build_benchmark_cases, load_benchmark_plan
from tests.test_benchmark_plan import valid_plan


def _load_harness():
    path = Path(__file__).resolve().parents[1] / "tools" / "run_benchmark_plan.py"
    spec = importlib.util.spec_from_file_location("run_benchmark_plan", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load benchmark-plan harness")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HARNESS = _load_harness()


def quick_plan():
    payload = valid_plan()
    payload["target_tps"] = [100]
    payload["repetitions"] = 1
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "plan.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return load_benchmark_plan(path)


class BenchmarkHarnessTests(unittest.TestCase):
    def test_configure_case_verifies_workload_and_threshold_responses(self) -> None:
        plan = quick_plan()
        case = next(case for case in build_benchmark_cases(plan) if case.variant == "E")
        responses: list[tuple[str, dict[str, object]]] = []

        def fake_request(_base, path, *, method, payload):
            self.assertEqual(method, "PATCH")
            responses.append((path, payload))
            return {**payload, "server_default": 1}

        with patch.object(HARNESS, "request_json", side_effect=fake_request):
            HARNESS.configure_case(plan, case)
        self.assertEqual([item[0] for item in responses], ["/workload", "/thresholds"])
        self.assertEqual(responses[0][1]["write_fence_mode"], "optimistic_detach_v1")
        self.assertEqual(responses[0][1]["active_target_tps"], 90)
        self.assertEqual(responses[1][1]["park_budget_ms"], 120_000)

    def test_flip_request_selects_each_marker_source_proof(self) -> None:
        payload = valid_plan()
        payload["variants"] = ["F", "G"]
        payload["target_tps"] = [5_000]
        payload["repetitions"] = 1
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            plan = load_benchmark_plan(path)

        by_variant = {
            case.variant: case for case in build_benchmark_cases(plan)
        }
        self.assertEqual(
            HARNESS._flip_payload(by_variant["F"]),
            {
                "fence_wakeup_mode": "passive",
                "source_proof_mode": "per_leaf_marker_v1",
            },
        )
        self.assertEqual(
            HARNESS._flip_payload(by_variant["G"]),
            {
                "fence_wakeup_mode": "passive",
                "source_proof_mode": "atomic_detach_marker_v1",
            },
        )

    def test_flip_request_selects_parallel_atomic_source_proof(self) -> None:
        payload = valid_plan()
        payload["variants"] = ["H"]
        payload["target_tps"] = [3_000]
        payload["repetitions"] = 1
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            plan = load_benchmark_plan(path)

        case = build_benchmark_cases(plan)[0]
        self.assertEqual(
            HARNESS._flip_payload(case),
            {
                "fence_wakeup_mode": "passive",
                "source_proof_mode": "parallel_atomic_detach_marker_v1",
            },
        )

    def test_saved_outputs_bind_plan_hash_for_matched_shapes(self) -> None:
        plan = quick_plan()
        cases = build_benchmark_cases(plan)
        completed = [
            {
                "case_id": case.case_id,
                "variant": case.variant,
                "target_tps": case.target_tps,
                "transaction_shape": case.transaction_shape,
                "tables_per_api_transaction": case.tables_per_api_transaction,
                "operations_per_api_batch": case.operations_per_api_batch,
                "ownership_reads_per_api_batch": case.ownership_reads_per_api_batch,
                "flip_status": "succeeded",
                "verification_outcome": "passed",
                "capacity": {"median_achieved_tps": 100.0, "capacity_label": "sustainable"},
                "durations_ns": {"writer_park_ns": 1_000_000},
            }
            for case in cases
        ]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            HARNESS._save_outputs(output, plan, "now", completed)
            manifest = json.loads((output / "matrix.json").read_text())
            report = (output / "report.md").read_text()
        self.assertEqual(manifest["plan_sha256"], plan.sha256)
        self.assertEqual(len(manifest["cases"]), 2)
        self.assertNotIn("not directly comparable", report)

    def test_workload_payload_keeps_d_and_e_transaction_shapes_identical(self) -> None:
        plan = quick_plan()
        by_variant = {case.variant: case for case in build_benchmark_cases(plan)}
        self.assertEqual(HARNESS._workload_payload(plan, by_variant["D"]), {
            **HARNESS._workload_payload(plan, by_variant["E"]),
            "write_fence_mode": "hot_transactional_v1",
        })
        self.assertEqual(by_variant["D"].tables_per_api_transaction, 1)
        self.assertEqual(by_variant["E"].tables_per_api_transaction, 1)
        self.assertEqual(by_variant["D"].operations_per_api_batch, 5)
        self.assertEqual(by_variant["E"].operations_per_api_batch, 5)
        self.assertEqual(by_variant["D"].ownership_reads_per_api_batch, 5)
        self.assertEqual(by_variant["E"].ownership_reads_per_api_batch, 1)
        self.assertEqual(by_variant["D"].transaction_shape, "api_batch_separate_commits_v1")
        self.assertEqual(by_variant["E"].transaction_shape, "api_batch_separate_commits_v1")


if __name__ == "__main__":
    unittest.main()
