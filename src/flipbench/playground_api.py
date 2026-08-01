from __future__ import annotations

import json
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Lock, Thread
from typing import Any, Mapping

from .connect_api import ConnectClient, redact_error_detail
from .core import TopicPartition, build_manifest
from .flip import FlipRunner
from .kafka_io import KafkaControl
from .playground import (
    AdmissionThresholds,
    AdmissionWindow,
    LiveWorkload,
    WorkloadSettings,
    validate_local_batch_budget,
)
from .postgres_io import connect, guarded_insert_events, slot_status
from .settings import Settings


class PlaygroundRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.manifest = build_manifest(settings.table_count, settings.cell, settings.timeslot)
        self.workload = LiveWorkload(WorkloadSettings())
        self._thresholds = AdmissionThresholds()
        self._admission = AdmissionWindow(self._thresholds)
        self._admission_generation = 0
        self._history: deque[dict[str, Any]] = deque(maxlen=240)
        self._lock = Lock()
        self._operation_lock = Lock()
        self._maintenance = False
        self._stop = Event()
        self._sampler: Thread | None = None
        self._retiring_run_id: uuid.UUID | None = None
        self._active_run_id: uuid.UUID | None = None
        self._flip_runner: FlipRunner | None = None
        self._flip_thread: Thread | None = None
        self._flip_result: Mapping[str, Any] | None = None
        self._flip_error: str | None = None
        self._last_error: str | None = None
        self._connector_states: dict[str, str] = {"source": "unknown", "sink": "unknown"}

    def start(self) -> None:
        if self._sampler is not None:
            return
        self._sampler = Thread(target=self._sample_loop, name="flipbench-metrics-sampler", daemon=True)
        self._sampler.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self.workload.stop_all()
        except RuntimeError:
            pass

    def _tracker_snapshot(self) -> tuple[dict[str, str], dict[str, str]]:
        with connect(self.settings.warm_dsn, autocommit=True) as warm:
            rows = warm.execute(
                """
                SELECT tracker.timeslot, tracker.state, attempts.attempt_id
                FROM public.partition_tracker AS tracker
                LEFT JOIN public.flip_attempts AS attempts
                  ON attempts.attempt_epoch = tracker.attempt_epoch
                WHERE tracker.cell=%s
                """,
                (self.settings.cell,),
            ).fetchall()
        states = {str(timeslot): str(state) for timeslot, state, _ in rows}
        attempts = {
            str(timeslot): str(attempt_id)
            for timeslot, _, attempt_id in rows
            if attempt_id is not None
        }
        return states, attempts

    def _ensure_operational(self) -> None:
        if self._maintenance:
            raise RuntimeError("playground is in restart maintenance mode")

    def _sample_loop(self) -> None:
        partitions = tuple(
            TopicPartition(route.topic, route.partition) for route in self.manifest.tables
        )
        groups = {partition: self.manifest.tables[0].sink_group for partition in partitions}
        kafka: KafkaControl | None = None
        last_connector_check = 0.0
        previous_at = time.monotonic()
        previous_active = 0
        previous_retiring = 0
        while not self._stop.is_set():
            try:
                with self._lock:
                    sample_generation = self._admission_generation
                if kafka is None:
                    kafka = KafkaControl(self.settings.kafka_bootstrap)
                with connect(self.settings.hot_dsn, autocommit=True) as hot:
                    source = slot_status(hot, self.settings.cell, self.settings.slot_name)
                end_offsets = kafka.end_offsets(partitions)
                committed = kafka.committed_offsets(groups, 2.0)
                sink_lag = {
                    partition.key: max(0, end_offsets[partition] - committed.get(partition, 0))
                    for partition in partitions
                }
                tracker, tracker_attempts = self._tracker_snapshot()
                now = time.monotonic()
                active = self.workload.active_total()
                retiring = self.workload.retiring_total()
                elapsed = max(0.001, now - previous_at)
                active_rate = (active - previous_active) / elapsed
                retiring_rate = (retiring - previous_retiring) / elapsed
                previous_at, previous_active, previous_retiring = now, active, retiring
                if now - last_connector_check >= 2:
                    self._connector_states = self._read_connector_states()
                    last_connector_check = now
                workload_valid = (
                    self.workload.active_is_alive()
                    and self.workload.retiring_is_alive()
                    and active > retiring > 0
                    and tracker.get(self.settings.timeslot) == "hot_primary"
                    and source.active
                    and self._connector_states == {"source": "RUNNING", "sink": "RUNNING"}
                )
                with self._lock:
                    if sample_generation != self._admission_generation:
                        self._admission.reset()
                        ready = False
                        workload_valid = False
                    elif workload_valid:
                        ready = self._admission.observe(source.lag_bytes, sink_lag)
                    else:
                        self._admission.reset()
                        ready = False
                    healthy_samples = self._admission.healthy_samples if workload_valid else 0
                sample = {
                    "at": datetime.now(timezone.utc).isoformat(),
                    "source_lag_bytes": source.lag_bytes,
                    "source_slot_active": source.active,
                    "sink_lag_records": sum(sink_lag.values()),
                    "max_sink_lag_records": max(sink_lag.values()),
                    "sink_lag_by_partition": sink_lag,
                    "active_rows_total": active,
                    "retiring_rows_total": retiring,
                    "active_rows_per_second": round(active_rate, 1),
                    "retiring_rows_per_second": round(retiring_rate, 1),
                    "admission_ready": ready and workload_valid,
                    "healthy_samples": healthy_samples,
                    "tracker_states": tracker,
                    "tracker_attempt_ids": tracker_attempts,
                }
                with self._lock:
                    self._history.append(sample)
                    self._last_error = None
            except BaseException as error:
                with self._lock:
                    self._last_error = redact_error_detail(f"{type(error).__name__}: {error}")
                if kafka is not None:
                    kafka.close()
                    kafka = None
            self._stop.wait(0.5)
        if kafka is not None:
            kafka.close()

    def _read_connector_states(self) -> dict[str, str]:
        def state(client: ConnectClient, connector: str) -> str:
            payload = client.status(connector)
            connector_state = str(payload.get("connector", {}).get("state", "unknown"))
            task_states = [str(task.get("state", "unknown")) for task in payload.get("tasks", ())]
            return (
                connector_state
                if task_states and all(item == connector_state for item in task_states)
                else "degraded"
            )

        sink_connector = self.manifest.tables[0].sink_connector
        return {
            "source": state(ConnectClient(self.settings.source_connect_url), self.settings.source_connector),
            "sink": state(ConnectClient(self.settings.sink_connect_url), sink_connector),
        }

    def update_workload(self, payload: Mapping[str, Any]) -> WorkloadSettings:
        with self._operation_lock:
            self._ensure_operational()
            if self.flip_running():
                raise RuntimeError("workload settings are frozen while the flip is running")
            current = self.workload.settings().to_dict()
            unknown = set(payload) - set(current)
            if unknown:
                raise ValueError(f"unknown workload fields: {', '.join(sorted(unknown))}")
            updated = WorkloadSettings(**{**current, **payload})
            validate_local_batch_budget(updated, self.settings.table_count)
            applied = self.workload.update(updated)
            with self._lock:
                self._admission_generation += 1
                self._admission.reset()
            return applied

    def update_thresholds(self, payload: Mapping[str, Any]) -> AdmissionThresholds:
        with self._operation_lock:
            self._ensure_operational()
            if self.flip_running():
                raise RuntimeError("admission thresholds are frozen while the flip is running")
            current = self._thresholds.to_dict()
            unknown = set(payload) - set(current)
            if unknown:
                raise ValueError(f"unknown threshold fields: {', '.join(sorted(unknown))}")
            updated = AdmissionThresholds(**{**current, **payload})
            with self._lock:
                self._thresholds = updated
                self._admission_generation += 1
                self._admission = AdmissionWindow(updated)
            return updated

    def start_workload(self) -> None:
        with self._operation_lock:
            self._ensure_operational()
            if self.workload.running():
                raise RuntimeError("workload is already running")
            tracker, _ = self._tracker_snapshot()
            if tracker.get(self.settings.timeslot) != "hot_primary":
                raise RuntimeError("retiring ownership is not hot_primary; reset and set up a fresh scenario")
            validate_local_batch_budget(self.workload.settings(), self.settings.table_count)
            self._retiring_run_id = uuid.uuid4()
            self._active_run_id = uuid.uuid4()
            with self._lock:
                self._admission_generation += 1
                self._admission.reset()
            retiring_run_id = self._retiring_run_id
            active_run_id = self._active_run_id

            def write(target_run_id: uuid.UUID, timeslot: str, rows: int) -> int:
                return guarded_insert_events(
                    self.settings.hot_dsn,
                    self.settings.warm_dsn,
                    self.manifest,
                    target_run_id,
                    rows,
                    timeslot,
                    self.workload.settings().payload_bytes,
                )

            self.workload.start(
                lambda rows: write(active_run_id, "active", rows),
                lambda rows: write(retiring_run_id, "retiring", rows),
            )

    def stop_workload(self) -> None:
        with self._operation_lock:
            self._ensure_operational()
            if self.flip_running():
                raise RuntimeError("cannot stop workload while flip is running")
            self.workload.stop_all()

    def flip_running(self) -> bool:
        return self._flip_thread is not None and self._flip_thread.is_alive()

    def start_flip(self) -> None:
        with self._operation_lock:
            self._ensure_operational()
            if self.flip_running():
                raise RuntimeError("flip is already running")
            with self._lock:
                latest = None if not self._history else dict(self._history[-1])
                thresholds = self._thresholds
                admission_ready = self._admission.ready
            if latest is None or not latest.get("admission_ready") or not admission_ready:
                raise RuntimeError("lag thresholds have not been healthy for the required stable samples")
            if self._retiring_run_id is None or not self.workload.retiring_is_alive():
                raise RuntimeError("retiring workload is not running")
            workload_settings = self.workload.settings()
            validate_local_batch_budget(workload_settings, self.settings.table_count)
            scenario = {
                "mode": "production-shaped",
                "active_events_per_table": workload_settings.active_rows_per_partition,
                "retiring_events_per_table": workload_settings.retiring_rows_per_partition,
                "active_pause_ms": workload_settings.active_pause_ms,
                "retiring_pause_ms": workload_settings.retiring_pause_ms,
                "max_source_lag_bytes": thresholds.max_source_lag_bytes,
                "max_sink_lag_records_per_partition": thresholds.max_sink_lag_records_per_partition,
                "required_stable_samples": thresholds.stable_samples,
                "stable_window": list(self._history)[-thresholds.stable_samples :],
                "active_run_id": str(self._active_run_id),
                "park_budget_ms": thresholds.park_budget_ms,
                "revert_reserve_ms": thresholds.revert_reserve_ms,
                "forward_budget_ms": thresholds.park_budget_ms - thresholds.revert_reserve_ms,
                "launched_from": "playground-ui",
                "environment_generation_id": os.environ.get(
                    "PLAYGROUND_ENVIRONMENT_GENERATION_ID", "legacy"
                ),
            }
            runner = FlipRunner(
                self.settings,
                self._retiring_run_id,
                (thresholds.park_budget_ms - thresholds.revert_reserve_ms) / 1000,
                thresholds.poll_ms / 1000,
                scenario,
                self.workload.stop_retiring,
                self.workload.retiring_total,
                self.workload.retiring_is_alive,
                self.workload.active_total,
                self.workload.active_is_alive,
                recovery_timeout_seconds=thresholds.revert_reserve_ms / 1000,
            )
            self._flip_runner = runner
            self._flip_result = None
            self._flip_error = None

            def execute() -> None:
                try:
                    result = runner.run(False)
                    with self._lock:
                        self._flip_result = result
                except BaseException as error:
                    result_path = self.settings.results_dir / str(self._retiring_run_id) / "run.json"
                    recovered_result: Mapping[str, Any] | None = None
                    try:
                        payload = json.loads(result_path.read_text(encoding="utf-8"))
                        if isinstance(payload, dict):
                            recovered_result = payload
                    except (OSError, ValueError):
                        pass
                    with self._lock:
                        self._flip_result = recovered_result
                        self._flip_error = redact_error_detail(f"{type(error).__name__}: {error}")

            self._flip_thread = Thread(target=execute, name="flipbench-playground-flip", daemon=True)
            self._flip_thread.start()

    def prepare_reset(self) -> Mapping[str, Any]:
        with self._operation_lock:
            if self.flip_running():
                raise RuntimeError("restart is blocked while an ownership flip is running")
            previous_maintenance = self._maintenance
            self._maintenance = True
            try:
                self.workload.stop_all()
                tracker_states, tracker_attempt_ids = self._tracker_snapshot()
                return {
                    **self.snapshot(),
                    "reset_evidence": {
                        "tracker_states": tracker_states,
                        "tracker_attempt_ids": tracker_attempt_ids,
                    },
                }
            except BaseException:
                self._maintenance = previous_maintenance
                raise

    def cancel_reset(self) -> None:
        with self._operation_lock:
            self._maintenance = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            history = list(self._history)
            thresholds = self._thresholds.to_dict()
            admission_ready = self._admission.ready
            healthy_samples = self._admission.healthy_samples
            result = self._flip_result
            flip_error = self._flip_error
            last_error = self._last_error
        runner = self._flip_runner
        events = [] if runner is None else list(runner.events)
        timestamps = {} if runner is None else dict(runner.timestamps)
        latest = None if not history else {
            **history[-1],
            "admission_ready": bool(history[-1]["admission_ready"] and admission_ready),
            "healthy_samples": healthy_samples,
        }
        if self.flip_running():
            flip_status = "running"
        elif result is not None and result.get("outcome") == "success":
            flip_status = (
                "succeeded"
                if result.get("verification_outcome") == "passed"
                else "verification_failed"
            )
        elif result is not None and result.get("outcome") == "reverted":
            flip_status = "reverted"
        elif flip_error:
            flip_status = "failed"
        else:
            flip_status = "idle"
        return {
            "environment": {
                "mode": "live-local-prototype",
                "table_count": self.settings.table_count,
                "database_partitions_per_table": 2,
                "retiring_topic_partitions": self.settings.table_count,
                "kafka_partitions_per_leaf_topic": 1,
                "cell": self.settings.cell,
                "retiring_timeslot": self.settings.timeslot,
                "topology_mutable": False,
                "topology_note": "This environment has one active and one retiring database leaf per table. Change TABLE_COUNT only during reset/setup.",
                "environment_generation_id": os.environ.get(
                    "PLAYGROUND_ENVIRONMENT_GENERATION_ID", "legacy"
                ),
                "maintenance_mode": self._maintenance,
            },
            "workload": {
                "running": self.workload.running(),
                "active_writer_alive": self.workload.active_is_alive(),
                "retiring_writer_alive": self.workload.retiring_is_alive(),
                "settings": self.workload.settings().to_dict(),
                "active_run_id": None if self._active_run_id is None else str(self._active_run_id),
                "retiring_run_id": None if self._retiring_run_id is None else str(self._retiring_run_id),
            },
            "thresholds": thresholds,
            "latest": latest,
            "history": history,
            "connectors": dict(self._connector_states),
            "flip": {
                "status": flip_status,
                "run_id": None if runner is None else str(runner.run_id),
                "events": events,
                "timestamps_ns": timestamps,
                "elapsed_ns": 0 if runner is None else runner.elapsed_ns(),
                "durations_ns": {} if result is None else dict(result.get("durations_ns", {})),
                "detach_ns_by_table": {} if result is None else dict(result.get("detach_ns_by_table", {})),
                "outcome": None if result is None else result.get("outcome"),
                "verification_outcome": None if result is None else result.get("verification_outcome"),
                "error": flip_error,
            },
            "metrics_error": last_error,
        }


class PlaygroundHandler(BaseHTTPRequestHandler):
    runtime: PlaygroundRuntime
    allowed_origin: str
    server_version = "FlipbenchPlayground/1"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(5)

    def _cors(self) -> None:
        origin = self.headers.get("Origin")
        if origin == self.allowed_origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")

    def _send(self, status: int, payload: Mapping[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def _read_json(self) -> Mapping[str, Any]:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ValueError("Content-Type must be application/json")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise ValueError("invalid Content-Length") from error
        if length <= 0 or length > 65_536:
            raise ValueError("request body must be between 1 and 65536 bytes")
        payload = json.loads(self.rfile.read(length))
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _require_allowed_origin(self) -> None:
        origin = self.headers.get("Origin")
        if origin != self.allowed_origin:
            raise PermissionError("origin is not allowed")

    @staticmethod
    def _public_error(error: BaseException) -> str:
        return redact_error_detail(str(error))

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/health":
            self._send(HTTPStatus.OK, {"ok": True})
        elif self.path == "/api/state":
            self._send(HTTPStatus.OK, {"ok": True, "data": self.runtime.snapshot()})
        else:
            self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": "route not found"})

    def do_PATCH(self) -> None:  # noqa: N802
        try:
            self._require_allowed_origin()
            payload = self._read_json()
            if self.path == "/api/workload":
                data = self.runtime.update_workload(payload).to_dict()
            elif self.path == "/api/thresholds":
                data = self.runtime.update_thresholds(payload).to_dict()
            else:
                self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": "route not found"})
                return
            self._send(HTTPStatus.OK, {"ok": True, "data": data})
        except (ValueError, TypeError) as error:
            self._send(HTTPStatus.BAD_REQUEST, {"ok": False, "error": self._public_error(error)})
        except RuntimeError as error:
            self._send(HTTPStatus.CONFLICT, {"ok": False, "error": self._public_error(error)})
        except PermissionError as error:
            self._send(HTTPStatus.FORBIDDEN, {"ok": False, "error": self._public_error(error)})

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._require_allowed_origin()
            content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                raise ValueError("Content-Type must be application/json")
            if self.path == "/api/workload/start":
                self.runtime.start_workload()
            elif self.path == "/api/workload/stop":
                self.runtime.stop_workload()
            elif self.path == "/api/flip/start":
                self.runtime.start_flip()
            elif self.path == "/api/environment/prepare-reset":
                self._send(
                    HTTPStatus.ACCEPTED,
                    {"ok": True, "data": self.runtime.prepare_reset()},
                )
                return
            elif self.path == "/api/environment/cancel-reset":
                self.runtime.cancel_reset()
            else:
                self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": "route not found"})
                return
            self._send(HTTPStatus.ACCEPTED, {"ok": True})
        except (ValueError, RuntimeError, TimeoutError) as error:
            self._send(HTTPStatus.CONFLICT, {"ok": False, "error": self._public_error(error)})
        except PermissionError as error:
            self._send(HTTPStatus.FORBIDDEN, {"ok": False, "error": self._public_error(error)})

    def log_message(self, format_string: str, *args: Any) -> None:
        print(f"playground-api {self.address_string()} {format_string % args}", flush=True)


def main() -> None:
    settings = Settings.from_env()
    runtime = PlaygroundRuntime(settings)
    PlaygroundHandler.runtime = runtime
    PlaygroundHandler.allowed_origin = os.environ.get("PLAYGROUND_ALLOWED_ORIGIN", "http://localhost:3000")
    port = int(os.environ.get("PLAYGROUND_API_PORT", "8090"))
    if not 1024 <= port <= 65535:
        raise ValueError("PLAYGROUND_API_PORT must be between 1024 and 65535")
    # Compose publishes this container port only on host loopback.
    server = ThreadingHTTPServer(("0.0.0.0", port), PlaygroundHandler)
    runtime.start()
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
        runtime.close()


if __name__ == "__main__":
    main()
