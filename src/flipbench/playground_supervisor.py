from __future__ import annotations

import inspect
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Callable, Mapping

from .connect_api import redact_error_detail
from .playground_results import load_saved_runs


SUPPORTED_TABLE_COUNTS = (5, 10, 15, 20)
SUPPORTED_SOURCE_TOPOLOGIES = ("shared", "isolated")
CommandRunner = Callable[[tuple[str, ...], Path], str]
CONTROL_API_RECOVERY_HINT = (
    "From the prototype directory, run `make playground-api-rf3 "
    "TABLE_COUNT=<current table count>`, wait for localhost:8090/api/health, and retry. "
    "No benchmark volumes were deleted."
)


@dataclass(frozen=True, slots=True)
class RestartProblem:
    code: str
    message: str
    recovery_hint: str


class RestartBlockedError(RuntimeError):
    def __init__(self, problem: RestartProblem) -> None:
        self.problem = problem
        super().__init__(problem.message)


@dataclass(frozen=True, slots=True)
class RestartRequest:
    table_count: int
    confirmation: str
    source_topology: str = "shared"

    def __post_init__(self) -> None:
        if (
            not isinstance(self.table_count, int)
            or isinstance(self.table_count, bool)
            or self.table_count not in SUPPORTED_TABLE_COUNTS
        ):
            raise ValueError("table_count must be one of 5, 10, 15, or 20")
        if self.confirmation != "RESET":
            raise ValueError("type RESET exactly to confirm deletion of local benchmark volumes")
        if self.source_topology not in SUPPORTED_SOURCE_TOPOLOGIES:
            raise ValueError("source_topology must be shared or isolated")

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> RestartRequest:
        if set(payload) not in (
            {"table_count", "confirmation"},
            {"table_count", "source_topology", "confirmation"},
        ):
            raise ValueError(
                "restart body must contain only table_count, source_topology and confirmation"
            )
        return cls(
            payload["table_count"],
            payload["confirmation"],
            payload.get("source_topology", "shared"),
        )


def restart_commands(
    table_count: int,
    environment_generation_id: str | None = None,
    *,
    source_topology: str = "shared",
) -> tuple[tuple[str, ...], ...]:
    RestartRequest(table_count, "RESET", source_topology)
    count = f"TABLE_COUNT={table_count}"
    topology = f"SOURCE_TOPOLOGY={source_topology}"
    api_command = ["make", "playground-api-rf3", count, topology]
    if environment_generation_id is not None:
        uuid.UUID(environment_generation_id)
        api_command.append(f"PLAYGROUND_ENVIRONMENT_GENERATION_ID={environment_generation_id}")
    return (
        ("make", "reset-rf3"),
        ("make", "up-rf3"),
        ("make", "setup-rf3", count, topology),
        tuple(api_command),
    )


def _run_command(command: tuple[str, ...], project_dir: Path) -> str:
    result = subprocess.run(
        command,
        cwd=project_dir,
        check=False,
        capture_output=True,
        text=True,
        timeout=900,
    )
    output = redact_error_detail((result.stdout + result.stderr)[-8000:])
    if result.returncode != 0:
        raise RuntimeError(f"{command[1]} failed with exit code {result.returncode}: {output[-2000:]}")
    return output


def _api_json(method: str, path: str) -> Mapping[str, Any]:
    request = urllib.request.Request(
        f"http://127.0.0.1:8090{path}",
        data=b"{}" if method == "POST" else None,
        method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "http://localhost:3000",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            raw_detail = error.read().decode("utf-8", errors="replace")
        finally:
            error.close()
        try:
            error_payload = json.loads(raw_detail)
        except (json.JSONDecodeError, TypeError):
            error_payload = None
        detail = (
            error_payload.get("error")
            if isinstance(error_payload, Mapping) and isinstance(error_payload.get("error"), str)
            else f"playground API returned HTTP {error.code}"
        )
        raise RuntimeError(redact_error_detail(detail)) from error
    except (OSError, urllib.error.URLError, TimeoutError) as error:
        raise urllib.error.URLError("control API transport unavailable") from error
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as error:
        raise RuntimeError("playground API returned malformed JSON") from error
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise RuntimeError("playground API returned an invalid response")
    data = payload.get("data", {})
    return data if isinstance(data, dict) else {}


def _prepare_reset_state(attempts: int = 3, retry_delay_seconds: float = 0.25) -> Mapping[str, Any]:
    last_transport_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            return _api_json("POST", "/api/environment/prepare-reset")
        except (OSError, urllib.error.URLError, TimeoutError) as error:
            last_transport_error = error
            if attempt + 1 < attempts:
                time.sleep(retry_delay_seconds)
                continue
        except RuntimeError as error:
            raise RestartBlockedError(
                RestartProblem(
                    code="control_api_not_ready",
                    message=str(error),
                    recovery_hint=(
                        "Resolve the reported control API condition, then retry. "
                        "No benchmark volumes were deleted."
                    ),
                )
            ) from error
        break
    raise RestartBlockedError(
        RestartProblem(
            code="control_api_unavailable",
            message=(
                "The restart was not started because the control API at "
                "localhost:8090 could not be reached. "
                f"{CONTROL_API_RECOVERY_HINT}"
            ),
            recovery_hint=CONTROL_API_RECOVERY_HINT,
        )
    ) from last_transport_error


def _saved_current_ownership(
    saved_runs: list[Mapping[str, Any]], current_run_id: object, current_attempt_id: object
) -> bool:
    successful = [item for item in saved_runs if item.get("outcome") == "success"]
    if isinstance(current_attempt_id, str):
        return any(item.get("attempt_id") == current_attempt_id for item in successful)
    if isinstance(current_run_id, str):
        return any(item.get("run_id") == current_run_id for item in successful)
    return False


def _default_safety_probe(results_dir: Path) -> str:
    state = _prepare_reset_state()
    flip = state.get("flip", {})
    evidence = state.get("reset_evidence", {})
    tracker = evidence.get("tracker_states", {}) if isinstance(evidence, Mapping) else {}
    attempts = evidence.get("tracker_attempt_ids", {}) if isinstance(evidence, Mapping) else {}
    try:
        if not isinstance(tracker, Mapping) or tracker.get("retiring") not in (
            "hot_primary",
            "warm_primary",
        ):
            raise RuntimeError("restart is blocked because retiring ownership is unknown")
        if tracker.get("retiring") == "warm_primary":
            saved = load_saved_runs(results_dir)
            current_run_id = flip.get("run_id") if isinstance(flip, Mapping) else None
            current_attempt_id = (
                attempts.get("retiring") if isinstance(attempts, Mapping) else None
            )
            if not _saved_current_ownership(saved, current_run_id, current_attempt_id):
                raise RuntimeError(
                    "restart is blocked because warm_primary has no matching saved ownership result"
                )
    except BaseException:
        try:
            _api_json("POST", "/api/environment/cancel-reset")
        except BaseException:
            pass
        raise
    return "Live state checked; workloads are stopped and saved history is preserved."


def _readiness_failures(
    state: Mapping[str, Any], table_count: int, generation_id: str, source_topology: str = "shared"
) -> tuple[str, ...]:
    environment = state.get("environment", {})
    latest = state.get("latest", {})
    connectors = state.get("connectors", {})
    tracker = latest.get("tracker_states", {}) if isinstance(latest, Mapping) else {}
    failures: list[str] = []
    if not isinstance(environment, Mapping) or environment.get("table_count") != table_count:
        failures.append(f"table_count is not {table_count}")
    if (
        not isinstance(environment, Mapping)
        or environment.get("environment_generation_id") != generation_id
    ):
        failures.append("environment generation does not match the restart job")
    if (
        not isinstance(environment, Mapping)
        or environment.get("source_topology") != source_topology
    ):
        failures.append("source topology does not match the restart job")
    if not isinstance(tracker, Mapping) or tracker.get("retiring") != "hot_primary":
        failures.append("retiring ownership is not hot_primary")
    if (
        not isinstance(connectors, Mapping)
        or connectors.get("source") != "RUNNING"
        or connectors.get("sink") != "RUNNING"
    ):
        failures.append("connectors are not both RUNNING")
    if state.get("metrics_error") is not None:
        failures.append("metrics are not healthy")
    return tuple(failures)


def _default_verifier(
    table_count: int, generation_id: str, source_topology: str = "shared"
) -> str:
    deadline = time.monotonic() + 90
    last_error = "playground API unavailable"
    while time.monotonic() < deadline:
        try:
            state = _api_json("GET", "/api/state")
            failures = _readiness_failures(
                state, table_count, generation_id, source_topology
            )
            if not failures:
                return "Fresh API, hot ownership, connector tasks and metrics verified."
            last_error = "; ".join(failures)
        except (OSError, urllib.error.URLError, ValueError, RuntimeError) as error:
            last_error = str(error)
        time.sleep(1)
    raise RuntimeError(f"post-restart verification timed out: {last_error}")


class RestartCoordinator:
    def __init__(
        self,
        project_dir: Path,
        command_runner: CommandRunner = _run_command,
        safety_probe: Callable[[Path], str] = _default_safety_probe,
        verifier: Callable[..., str] = _default_verifier,
    ) -> None:
        self._project_dir = project_dir.resolve()
        self._command_runner = command_runner
        self._safety_probe = safety_probe
        self._verifier = verifier
        self._lock = Lock()
        self._thread: Thread | None = None
        self._state: dict[str, Any] = {
            "status": "idle",
            "phase": "Ready",
            "step": 0,
            "total_steps": 6,
            "job_id": None,
            "environment_generation_id": None,
            "table_count": None,
            "source_topology": None,
            "started_at_utc": None,
            "finished_at_utc": None,
            "error": None,
            "error_code": None,
            "recovery_hint": None,
            "logs": [],
        }

    def _replace(self, **changes: Any) -> None:
        with self._lock:
            self._state = {**self._state, **changes}

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {**self._state, "logs": list(self._state["logs"])}

    def start(self, request: RestartRequest) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("an environment restart is already running")
            self._state = {
                "status": "running",
                "phase": "Preparing scoped reset",
                "step": 0,
                "total_steps": 6,
                "job_id": str(uuid.uuid4()),
                "environment_generation_id": None,
                "table_count": request.table_count,
                "source_topology": request.source_topology,
                "started_at_utc": datetime.now(timezone.utc).isoformat(),
                "finished_at_utc": None,
                "error": None,
                "error_code": None,
                "recovery_hint": None,
                "logs": [],
            }
            self._thread = Thread(
                target=self._execute,
                args=(request,),
                name="flipbench-environment-restart",
                daemon=True,
            )
            self._thread.start()

    def _execute(self, request: RestartRequest) -> None:
        phases = ("Resetting volumes", "Starting RF3 stack", "Creating topology", "Starting control API")
        try:
            state = self.snapshot()
            generation_id = str(state["job_id"])
            self._replace(phase="Validating current experiment", step=1)
            probe_output = self._safety_probe(self._project_dir / "results")
            self._replace(logs=[probe_output])
            commands = restart_commands(
                request.table_count,
                generation_id,
                source_topology=request.source_topology,
            )
            for position, (phase, command) in enumerate(
                zip(phases, commands),
            ):
                index = position + 2
                self._replace(phase=phase, step=index)
                try:
                    output = self._command_runner(command, self._project_dir)
                except Exception as setup_error:
                    if position != 2:
                        raise
                    snapshot = self.snapshot()
                    self._replace(
                        phase="Retrying clean topology setup",
                        logs=[
                            *snapshot["logs"],
                            f"First topology setup failed: {setup_error}",
                        ][-4:],
                    )
                    retry_outputs: list[str] = []
                    for retry_position in range(3):
                        self._replace(
                            phase=f"Retry: {phases[retry_position]}",
                            step=retry_position + 2,
                        )
                        retry_outputs.append(
                            self._command_runner(
                                commands[retry_position], self._project_dir
                            )
                        )
                    output = "\n".join(retry_outputs)
                snapshot = self.snapshot()
                self._replace(logs=[*snapshot["logs"], output[-2000:]][-4:])
            self._replace(phase="Verifying fresh environment", step=6)
            try:
                parameters = tuple(inspect.signature(self._verifier).parameters.values())
            except (TypeError, ValueError):
                supports_topology = True
            else:
                supports_topology = any(
                    parameter.kind == inspect.Parameter.VAR_POSITIONAL
                    for parameter in parameters
                ) or len(
                    tuple(
                        parameter
                        for parameter in parameters
                        if parameter.kind
                        in (
                            inspect.Parameter.POSITIONAL_ONLY,
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        )
                    )
                ) >= 3
            verification = (
                self._verifier(request.table_count, generation_id, request.source_topology)
                if supports_topology
                else self._verifier(request.table_count, generation_id)
            )
            snapshot = self.snapshot()
            self._replace(logs=[*snapshot["logs"], verification][-4:])
        except BaseException as error:
            try:
                _api_json("POST", "/api/environment/cancel-reset")
            except BaseException:
                pass
            problem = (
                error.problem
                if isinstance(error, RestartBlockedError)
                else RestartProblem(
                    code="restart_failed",
                    message=redact_error_detail(str(error))[:2048],
                    recovery_hint=(
                        "Review the failed step and its bounded log, correct the local service, "
                        "then explicitly retry the restart."
                    ),
                )
            )
            self._replace(
                status="failed",
                phase="Restart failed",
                error=problem.message[:2048],
                error_code=problem.code,
                recovery_hint=problem.recovery_hint[:2048],
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
            )
            return
        self._replace(
            status="completed",
            phase="Fresh experiment ready",
            step=6,
            environment_generation_id=generation_id,
            finished_at_utc=datetime.now(timezone.utc).isoformat(),
        )


class SupervisorHandler(BaseHTTPRequestHandler):
    coordinator: RestartCoordinator
    allowed_origin: str
    results_dir: Path
    server_version = "FlipbenchSupervisor/1"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(5)

    def _cors(self) -> None:
        origin = self.headers.get("Origin")
        if origin == self.allowed_origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def _send(self, status: int, payload: Mapping[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def _require_origin(self) -> None:
        if self.headers.get("Origin") != self.allowed_origin:
            raise PermissionError("origin is not allowed")
        if self.headers.get("Host") not in ("localhost:8091", "127.0.0.1:8091"):
            raise PermissionError("host is not allowed")

    def _read_request(self) -> RestartRequest:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ValueError("Content-Type must be application/json")
        length = int(self.headers.get("Content-Length", "0"))
        if not 1 <= length <= 4096:
            raise ValueError("request body must be between 1 and 4096 bytes")
        payload = json.loads(self.rfile.read(length))
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return RestartRequest.from_payload(payload)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/state":
            self._send(HTTPStatus.OK, {"ok": True, "data": self.coordinator.snapshot()})
        elif self.path == "/api/runs":
            self._send(
                HTTPStatus.OK,
                {"ok": True, "data": load_saved_runs(self.results_dir)},
            )
        else:
            self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": "route not found"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._require_origin()
            if self.path != "/api/environment/restart":
                self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": "route not found"})
                return
            self.coordinator.start(self._read_request())
            self._send(
                HTTPStatus.ACCEPTED,
                {"ok": True, "data": self.coordinator.snapshot()},
            )
        except PermissionError as error:
            self._send(HTTPStatus.FORBIDDEN, {"ok": False, "error": str(error)})
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            self._send(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(error)[:2048]})
        except RuntimeError as error:
            self._send(HTTPStatus.CONFLICT, {"ok": False, "error": str(error)[:2048]})

    def log_message(self, format_string: str, *args: Any) -> None:
        if self.command in ("GET", "OPTIONS") and len(args) > 1 and str(args[1]).startswith("2"):
            return
        print(f"playground-supervisor {self.address_string()} {format_string % args}", flush=True)


def main() -> None:
    default_project = Path(__file__).resolve().parents[2]
    project_dir = Path(os.environ.get("FLIPBENCH_PROJECT_DIR", str(default_project)))
    if not (project_dir / "compose.yaml").is_file() or not (project_dir / "Makefile").is_file():
        raise RuntimeError("FLIPBENCH_PROJECT_DIR is not a Flipbench prototype directory")
    port = int(os.environ.get("PLAYGROUND_SUPERVISOR_PORT", "8091"))
    if not 1024 <= port <= 65535:
        raise ValueError("PLAYGROUND_SUPERVISOR_PORT must be between 1024 and 65535")
    SupervisorHandler.coordinator = RestartCoordinator(project_dir)
    SupervisorHandler.results_dir = project_dir / "results"
    SupervisorHandler.allowed_origin = os.environ.get(
        "PLAYGROUND_ALLOWED_ORIGIN", "http://localhost:3000"
    )
    server = ThreadingHTTPServer(("127.0.0.1", port), SupervisorHandler)
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
