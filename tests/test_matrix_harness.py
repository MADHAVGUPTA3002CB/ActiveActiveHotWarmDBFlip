from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_harness():
    path = Path(__file__).resolve().parents[1] / "tools" / "run_full_tps_matrix.py"
    spec = importlib.util.spec_from_file_location("run_full_tps_matrix", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load full matrix harness")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HARNESS = _load_harness()


class FullMatrixHarnessTests(unittest.TestCase):
    def test_refuses_to_overwrite_an_existing_experiment_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "matrix"
            path.mkdir()
            with self.assertRaisesRegex(FileExistsError, "already exists"):
                HARNESS.prepare_output_dir(path)

    def test_contract_check_reports_configuration_drift(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "workload configuration drift"):
            HARNESS.require_contract(
                "workload",
                {"active_workers": 16, "payload_bytes": 256},
                {"active_workers": 32, "payload_bytes": 256},
            )

    def test_configure_case_verifies_both_patch_responses(self) -> None:
        case = next(
            case
            for case in HARNESS.build_full_matrix()
            if case.variant == "D" and case.target_tps == 2_000
        )
        responses = []

        def fake_request(_base, path, *, method, payload):
            self.assertEqual(method, "PATCH")
            responses.append((path, payload))
            return {**payload, "unrelated_server_default": 1}

        with patch.object(HARNESS, "request_json", side_effect=fake_request):
            HARNESS.configure_case(case)
        self.assertEqual([path for path, _ in responses], ["/workload", "/thresholds"])
        self.assertEqual(responses[0][1]["min_achievement_percent"], 5)
        self.assertEqual(responses[0][1]["write_fence_mode"], "hot_transactional_v1")
        self.assertEqual(responses[1][1]["park_budget_ms"], 120_000)

    def test_unresolved_case_error_is_always_fatal(self) -> None:
        error = TimeoutError("ambiguous flip start")
        fatal = HARNESS.as_matrix_abort(error, "03-05000tps-a")
        self.assertIsInstance(fatal, HARNESS.MatrixAbort)
        self.assertIn("03-05000tps-a", str(fatal))


if __name__ == "__main__":
    unittest.main()
