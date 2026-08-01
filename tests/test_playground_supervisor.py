from __future__ import annotations

import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from threading import Thread
from unittest.mock import Mock, patch
from http.server import ThreadingHTTPServer

from flipbench.playground_supervisor import (
    RestartCoordinator,
    RestartRequest,
    SupervisorHandler,
    _default_safety_probe,
    _default_verifier,
    _readiness_failures,
    _run_command,
    _saved_current_ownership,
    restart_commands,
)


class RestartRequestTests(unittest.TestCase):
    def test_requires_exact_confirmation_and_supported_table_count(self) -> None:
        request = RestartRequest.from_payload({"table_count": 10, "confirmation": "RESET"})
        self.assertEqual(request.table_count, 10)
        for payload in (
            {"table_count": 7, "confirmation": "RESET"},
            {"table_count": 5, "confirmation": "reset"},
            {"table_count": True, "confirmation": "RESET"},
            {"table_count": 5, "confirmation": "RESET", "command": "rm"},
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                RestartRequest.from_payload(payload)

    def test_restart_commands_are_fixed_and_scoped(self) -> None:
        commands = restart_commands(15)
        self.assertEqual(commands[0], ("make", "reset-rf3"))
        self.assertEqual(commands[1], ("make", "up-rf3"))
        self.assertEqual(commands[2], ("make", "setup-rf3", "TABLE_COUNT=15"))
        self.assertEqual(commands[3], ("make", "playground-api-rf3", "TABLE_COUNT=15"))


class RestartCoordinatorTests(unittest.TestCase):
    def test_current_ownership_checkpoint_requires_matching_identity(self) -> None:
        saved = [
            {"run_id": "old", "attempt_id": "attempt-old", "outcome": "success"},
            {"run_id": "current", "attempt_id": "attempt-current", "outcome": "success"},
        ]
        self.assertTrue(_saved_current_ownership(saved, "current", "attempt-current"))
        self.assertTrue(_saved_current_ownership(saved, None, "attempt-current"))
        self.assertFalse(_saved_current_ownership(saved, "missing", "attempt-missing"))
        self.assertFalse(_saved_current_ownership(saved, None, None))

    def test_runs_one_restart_at_a_time_and_records_progress(self) -> None:
        observed: list[tuple[str, ...]] = []

        def run(command: tuple[str, ...], _: Path) -> str:
            observed.append(command)
            time.sleep(0.002)
            return "ok"

        with tempfile.TemporaryDirectory() as directory:
            coordinator = RestartCoordinator(
                Path(directory),
                command_runner=run,
                safety_probe=lambda _: "safe",
                verifier=lambda _count, _generation: "ready",
            )
            coordinator.start(RestartRequest(5, "RESET"))
            with self.assertRaises(RuntimeError):
                coordinator.start(RestartRequest(10, "RESET"))
            deadline = time.monotonic() + 1
            while coordinator.snapshot()["status"] == "running" and time.monotonic() < deadline:
                time.sleep(0.003)
            state = coordinator.snapshot()
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["step"], 6)
        self.assertEqual(observed[:3], list(restart_commands(5))[:3])
        self.assertEqual(observed[3][:3], restart_commands(5)[3])

    def test_fails_closed_when_a_fixed_step_errors(self) -> None:
        def fail(_: tuple[str, ...], __: Path) -> str:
            raise RuntimeError("compose failed")

        with tempfile.TemporaryDirectory() as directory:
            coordinator = RestartCoordinator(
                Path(directory),
                command_runner=fail,
                safety_probe=lambda _: "safe",
                verifier=lambda _count, _generation: "ready",
            )
            coordinator.start(RestartRequest(5, "RESET"))
            deadline = time.monotonic() + 1
            while coordinator.snapshot()["status"] == "running" and time.monotonic() < deadline:
                time.sleep(0.002)
            state = coordinator.snapshot()
        self.assertEqual(state["status"], "failed")
        self.assertIn("compose failed", str(state["error"]))

    def test_exposes_structured_actionable_safety_failure_without_python_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = RestartCoordinator(
                Path(directory),
                safety_probe=Mock(
                    side_effect=RuntimeError(
                        "The control API at localhost:8090 could not be reached."
                    )
                ),
            )
            coordinator.start(RestartRequest(5, "RESET"))
            deadline = time.monotonic() + 1
            while coordinator.snapshot()["status"] == "running" and time.monotonic() < deadline:
                time.sleep(0.002)
            state = coordinator.snapshot()
        self.assertEqual(state["status"], "failed")
        self.assertNotIn("RuntimeError:", str(state["error"]))
        self.assertEqual(state["error_code"], "restart_failed")
        self.assertTrue(state["recovery_hint"])


class SupervisorSafetyTests(unittest.TestCase):
    def test_readiness_failures_name_each_unready_condition(self) -> None:
        state = {
            "environment": {"table_count": 5, "environment_generation_id": "old"},
            "latest": {"tracker_states": {"retiring": "recovering"}},
            "connectors": {"source": "FAILED", "sink": "RUNNING"},
            "metrics_error": "slot unavailable",
        }
        failures = _readiness_failures(state, 10, "fresh")
        self.assertTrue(any("table_count" in item for item in failures))
        self.assertTrue(any("generation" in item for item in failures))
        self.assertTrue(any("retiring ownership" in item for item in failures))
        self.assertTrue(any("connectors" in item for item in failures))
        self.assertTrue(any("metrics" in item for item in failures))

    def test_safety_probe_preserves_live_flip_reason_and_blocks_unsaved_current_grant(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "flipbench.playground_supervisor._api_json",
                side_effect=RuntimeError("restart is blocked while an ownership flip is running"),
            ), self.assertRaisesRegex(RuntimeError, "ownership flip is running"):
                _default_safety_probe(Path(directory))
            state = {
                "flip": {"status": "succeeded", "run_id": "current"},
                "reset_evidence": {
                    "tracker_states": {"retiring": "warm_primary"},
                    "tracker_attempt_ids": {"retiring": "attempt-current"},
                },
            }
            api = Mock(side_effect=[state, {}])
            with patch("flipbench.playground_supervisor._api_json", api), patch(
                "flipbench.playground_supervisor.load_saved_runs", return_value=[]
            ), self.assertRaisesRegex(RuntimeError, "no matching saved ownership result"):
                _default_safety_probe(Path(directory))
            api.assert_called_with("POST", "/api/environment/cancel-reset")

    def test_safety_probe_stops_workload_and_accepts_saved_grant(self) -> None:
        state = {
            "flip": {"status": "succeeded", "run_id": "current"},
            "reset_evidence": {
                "tracker_states": {"retiring": "warm_primary"},
                "tracker_attempt_ids": {"retiring": "attempt-current"},
            },
        }
        api = Mock(return_value=state)
        with tempfile.TemporaryDirectory() as directory, patch(
            "flipbench.playground_supervisor._api_json", api
        ), patch(
            "flipbench.playground_supervisor.load_saved_runs",
            return_value=[
                {
                    "run_id": "current",
                    "attempt_id": "attempt-current",
                    "outcome": "success",
                }
            ],
        ):
            self.assertIn("history is preserved", _default_safety_probe(Path(directory)))
        api.assert_called_once_with("POST", "/api/environment/prepare-reset")

    def test_safety_probe_fails_closed_when_api_is_absent(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "flipbench.playground_supervisor._api_json",
            side_effect=urllib.error.URLError(ConnectionRefusedError()),
        ), patch("flipbench.playground_supervisor.time.sleep"), self.assertRaisesRegex(
            RuntimeError, "localhost:8090"
        ) as raised:
            _default_safety_probe(Path(directory))
        self.assertIn("make playground-api-rf3", str(raised.exception))

    def test_safety_probe_retries_a_transient_unavailable_api(self) -> None:
        state = {
            "flip": {"status": "idle", "run_id": None},
            "reset_evidence": {
                "tracker_states": {"retiring": "hot_primary"},
                "tracker_attempt_ids": {},
            },
        }
        api = Mock(side_effect=[urllib.error.URLError(ConnectionRefusedError()), state])
        with tempfile.TemporaryDirectory() as directory, patch(
            "flipbench.playground_supervisor._api_json", api
        ), patch("flipbench.playground_supervisor.time.sleep"):
            message = _default_safety_probe(Path(directory))
        self.assertIn("history is preserved", message)
        self.assertEqual(api.call_count, 2)

    def test_verifier_accepts_only_fresh_ready_environment(self) -> None:
        ready = {
            "environment": {"table_count": 10, "environment_generation_id": "generation"},
            "latest": {"tracker_states": {"retiring": "hot_primary"}},
            "connectors": {"source": "RUNNING", "sink": "RUNNING"},
            "metrics_error": None,
        }
        with patch("flipbench.playground_supervisor._api_json", return_value=ready):
            self.assertIn("verified", _default_verifier(10, "generation"))

    def test_verifier_retries_a_transient_not_ready_response(self) -> None:
        ready = {
            "environment": {"table_count": 10, "environment_generation_id": "generation"},
            "latest": {"tracker_states": {"retiring": "hot_primary"}},
            "connectors": {"source": "RUNNING", "sink": "RUNNING"},
            "metrics_error": None,
        }
        api = Mock(side_effect=[RuntimeError("setup is still starting"), ready])
        with patch("flipbench.playground_supervisor._api_json", api), patch(
            "flipbench.playground_supervisor.time.sleep"
        ):
            self.assertIn("verified", _default_verifier(10, "generation"))
        self.assertEqual(api.call_count, 2)

    def test_fixed_command_runner_redacts_and_fails_on_nonzero(self) -> None:
        completed = Mock(returncode=1, stdout="", stderr="password=secret")
        with patch("flipbench.playground_supervisor.subprocess.run", return_value=completed):
            with self.assertRaisesRegex(RuntimeError, "REDACTED"):
                _run_command(("make", "reset-rf3"), Path("."))


class SupervisorHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.coordinator = RestartCoordinator(
            Path(self.temp.name),
            command_runner=lambda _command, _directory: "ok",
            safety_probe=lambda _directory: "safe",
            verifier=lambda _count, _generation: "ready",
        )
        SupervisorHandler.coordinator = self.coordinator
        SupervisorHandler.results_dir = Path(self.temp.name)
        SupervisorHandler.allowed_origin = "http://localhost:3000"
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), SupervisorHandler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=1)
        self.temp.cleanup()

    def request(
        self, method: str, path: str, body: bytes | None = None, origin: str = "http://localhost:3000"
    ) -> tuple[int, dict]:
        request = urllib.request.Request(
            f"{self.base}{path}",
            data=body,
            method=method,
            headers={
                "Origin": origin,
                "Host": "localhost:8091",
                "Content-Type": "application/json",
            },
        )
        try:
            response = urllib.request.urlopen(request, timeout=2)
        except urllib.error.HTTPError as error:
            try:
                return error.code, __import__("json").loads(error.read())
            finally:
                error.close()
        with response:
            payload = response.read()
            return response.status, {} if not payload else __import__("json").loads(payload)

    def test_reads_state_history_and_not_found(self) -> None:
        status, state = self.request("GET", "/api/state")
        self.assertEqual((status, state["data"]["status"]), (200, "idle"))
        status, runs = self.request("GET", "/api/runs")
        self.assertEqual((status, runs["data"]), (200, []))
        status, missing = self.request("GET", "/missing")
        self.assertEqual((status, missing["ok"]), (404, False))

    def test_post_rejects_cross_origin_and_bad_json(self) -> None:
        status, _ = self.request(
            "POST", "/api/environment/restart", b'{}', origin="http://evil.example"
        )
        self.assertEqual(status, 403)
        status, payload = self.request("POST", "/api/environment/restart", b'{}')
        self.assertEqual(status, 400)
        self.assertIn("only table_count", payload["error"])

    def test_post_starts_fixed_restart_and_reports_snapshot(self) -> None:
        status, payload = self.request(
            "POST",
            "/api/environment/restart",
            b'{"table_count":5,"confirmation":"RESET"}',
        )
        self.assertEqual(status, 202)
        self.assertIn(payload["data"]["status"], ("running", "completed"))



if __name__ == "__main__":
    unittest.main()
