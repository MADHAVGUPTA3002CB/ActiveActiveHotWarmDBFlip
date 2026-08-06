"""H-DD-Prod: the generation-pinned rolling coordinator and driver.

``flip_generation`` runs Variant H's exact-marker flip for one generation on the
lane that generation was pinned to at provisioning time. ``run_rolling`` drives
consecutive generations end to end: provision on the free lane, rotate the
application route at the boundary, let the retiring lane quiesce, flip, verify,
release the lane, repeat. Traffic keeps writing to the current active generation
through every flip, exactly like the production design in
docs/variant-h-generation-pinned-connectors.md.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Callable, Mapping

from psycopg import sql
from psycopg.types.json import Jsonb

from .connect_api import ConnectClient
from .connector_configs import FENCE_SCHEMA
from .core import (
    AttemptState,
    GateEvidence,
    OptimisticAdmissionCheckMode,
    TopicPartition,
    build_leaf_fence_markers,
    canonical_manifest_json,
    transition,
)
from .generations import GenerationSpec, provision_generation, verify_lane_publication_membership
from .kafka_io import KafkaControl
from .parallel_detach import ParallelDetachError, run_all_parallel
from .postgres_io import (
    OptimisticDetachTransactionSession,
    atomic_detach_and_emit_leaf_fence_marker,
    bind_hot_write_gate_attempt,
    connect,
    hot_write_gate_status,
    observed_leaf_fence_receipts,
    park_hot_write_gate,
    reopen_hot_write_gate,
    slot_status,
)
from .recovery import CatalogLeafState, _catalog_state, revert_to_hot
from .settings import Settings
from .traffic import TrafficLane, TrafficTarget

import hashlib


def _lane_spec(settings: Settings, generation: GenerationSpec):
    from .connector_configs import lane_source_specs

    for spec in lane_source_specs(settings, generation.manifest):
        if spec.lane == generation.lane:
            return spec
    raise RuntimeError(f"no connector spec for lane {generation.lane}")


def flip_generation(
    settings: Settings,
    generation: GenerationSpec,
    kafka: KafkaControl,
    *,
    stop_admission: Callable[[], Mapping[str, Any]],
    resolve_in_flight: Callable[[], Mapping[str, Any]],
    timeout_seconds: float = 120.0,
    poll_seconds: float = 0.1,
    recovery_timeout_seconds: float = 60.0,
    detach_lock_timeout_ms: int = 250,
) -> dict[str, Any]:
    """Variant H on the generation's pinned lane; fail-closed with catalog revert."""
    manifest = generation.manifest
    lane = _lane_spec(settings, generation)
    attempt_id = uuid.uuid4()
    marks: dict[str, int] = {}
    base_ns = time.perf_counter_ns()

    def mark(stage: str) -> None:
        marks[stage] = time.perf_counter_ns() - base_ns

    result: dict[str, Any] = {
        "generation": generation.timeslot,
        "lane": generation.lane,
        "attempt_id": str(attempt_id),
        "outcome": "failed",
    }
    parked = False
    attempt_epoch: int | None = None
    with connect(settings.hot_dsn, autocommit=True) as hot, connect(
        settings.warm_dsn, autocommit=True
    ) as warm:
        try:
            mark("t0")
            source = ConnectClient(settings.source_connect_url)
            payload = source.status(lane.connector_name)
            connector_state = str(payload.get("connector", {}).get("state", ""))
            task_states = tuple(str(task.get("state", "")) for task in payload.get("tasks", ()))
            if connector_state != "RUNNING" or any(state != "RUNNING" for state in task_states):
                raise RuntimeError(
                    f"lane {generation.lane} connector is not RUNNING: {connector_state}/{task_states}"
                )
            lane_slot = slot_status(hot, settings.cell, lane.slot_name)
            if not lane_slot.active:
                raise RuntimeError(f"lane {generation.lane} slot is not active")
            result["admission_source_lag_bytes"] = lane_slot.lag_bytes
            tracker_state = warm.execute(
                "SELECT state FROM public.partition_tracker WHERE cell=%s AND timeslot=%s",
                (settings.cell, generation.timeslot),
            ).fetchone()
            if tracker_state is None or tracker_state[0] != "hot_primary":
                raise RuntimeError(f"generation tracker is not hot_primary: {tracker_state}")
            verify_lane_publication_membership(hot, settings, generation)
            gate_before = hot_write_gate_status(hot, settings.cell, generation.timeslot)
            if gate_before.state != "open":
                raise RuntimeError("generation hot gate is not open before the flip")
            mark("t1")

            park_started_ns = time.perf_counter_ns()
            ownership_epoch = park_hot_write_gate(
                hot, settings.cell, generation.timeslot, attempt_id, gate_before.ownership_epoch
            )
            parked = True
            result["hot_fence_park_ns"] = time.perf_counter_ns() - park_started_ns
            mark("t2")

            manifest_json = canonical_manifest_json(manifest)
            with warm.transaction():
                attempt_epoch = warm.execute(
                    """
                    INSERT INTO public.flip_attempts
                        (attempt_id, cell, timeslot, state, table_count, slot_name, publication_name,
                         manifest, manifest_sha256, connector_config_sha256,
                         write_fence_mode, hot_ownership_epoch, hot_gate_version)
                    VALUES (%s, %s, %s, 'locked', %s, %s, %s, %s, %s, %s,
                            'optimistic_detach_v1', %s, %s)
                    RETURNING attempt_epoch
                    """,
                    (
                        attempt_id,
                        settings.cell,
                        generation.timeslot,
                        settings.table_count,
                        lane.slot_name,
                        lane.publication_name,
                        Jsonb(json.loads(manifest_json)),
                        hashlib.sha256(manifest_json.encode("utf-8")).hexdigest(),
                        hashlib.sha256(
                            json.dumps(
                                {k: v for k, v in lane.config.items() if "password" not in k.lower()},
                                sort_keys=True,
                            ).encode("utf-8")
                        ).hexdigest(),
                        ownership_epoch,
                        gate_before.version,
                    ),
                ).fetchone()[0]
                tracker_updated = warm.execute(
                    """
                    UPDATE public.partition_tracker
                    SET state='locked', attempt_epoch=%s, version=version+1, updated_at=clock_timestamp()
                    WHERE cell=%s AND timeslot=%s AND state='hot_primary' AND attempt_epoch IS NULL
                    """,
                    (attempt_epoch, settings.cell, generation.timeslot),
                ).rowcount
                if tracker_updated != 1:
                    raise RuntimeError("ownership CAS hot_primary -> locked failed")
                with warm.cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO public.flip_table_states (attempt_epoch, parent_name, leaf_name, state)
                        VALUES (%s, %s, %s, 'attached')
                        """,
                        ((attempt_epoch, route.parent, route.leaf) for route in manifest.tables),
                    )
            bind_hot_write_gate_attempt(
                hot, settings.cell, generation.timeslot, attempt_id, attempt_epoch
            )
            transition(AttemptState.HOT_PRIMARY, AttemptState.LOCKED)
            result["attempt_epoch"] = attempt_epoch
            mark("t2w")

            admission = dict(stop_admission())
            mark("t2f")
            in_flight = dict(resolve_in_flight())
            mark("t2q")
            result["retiring_admission_stop"] = admission
            result["retiring_in_flight_resolution"] = in_flight

            markers = build_leaf_fence_markers(manifest, attempt_id, attempt_epoch)
            partitions = tuple(marker.partition for marker in markers)
            baselines = dict(kafka.end_offsets(partitions))
            mark("t5")

            def detach_one(marker) -> None:
                options = (
                    f"-c lock_timeout={detach_lock_timeout_ms}ms "
                    f"-c statement_timeout={int(timeout_seconds * 1000)}ms"
                )
                with connect(settings.hot_dsn, options=options) as worker_connection:
                    atomic_detach_and_emit_leaf_fence_marker(
                        worker_connection, marker, ownership_epoch
                    )

            detach_started_ns = time.perf_counter_ns()
            successes = run_all_parallel(markers, detach_one)
            result["parallel_detach_wall_ns"] = time.perf_counter_ns() - detach_started_ns
            result["detach_ns_by_leaf"] = {
                success.item.leaf: success.duration_ns for success in successes
            }
            with warm.cursor() as cursor:
                cursor.executemany(
                    """
                    UPDATE public.flip_table_states
                    SET state='detached', detach_finished_at=clock_timestamp(), detach_duration_ns=%s
                    WHERE attempt_epoch=%s AND parent_name=%s
                    """,
                    (
                        (success.duration_ns, attempt_epoch, success.item.parent)
                        for success in successes
                    ),
                )
            mark("t5d")

            marker_targets = dict(
                kafka.wait_leaf_fence_markers(markers, baselines, timeout_seconds)
            )
            mark("t7")
            result["marker_next_offsets"] = {
                partition.key: offset for partition, offset in marker_targets.items()
            }

            receipts_deadline = time.monotonic() + timeout_seconds
            while True:
                observed = observed_leaf_fence_receipts(warm, markers, ownership_epoch)
                if observed == frozenset(partitions):
                    break
                if time.monotonic() >= receipts_deadline:
                    missing = sorted(p.key for p in set(partitions) - observed)
                    raise TimeoutError(f"warm receipts missing: {missing}")
                time.sleep(poll_seconds)
            mark("t10")

            group_by_partition = {
                TopicPartition(route.topic, route.partition): route.sink_group
                for route in manifest.tables
            }
            offsets_deadline = time.monotonic() + timeout_seconds
            while True:
                committed = kafka.committed_offsets(group_by_partition, 5.0)
                if all(
                    committed.get(partition, -1) >= marker_targets[partition]
                    for partition in partitions
                ):
                    break
                if time.monotonic() >= offsets_deadline:
                    raise TimeoutError("sink committed offsets did not pass every exact marker")
                time.sleep(poll_seconds)
            mark("t11")

            parity: list[dict[str, Any]] = []
            for route in manifest.tables:
                hot_count = hot.execute(
                    sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(route.leaf))
                ).fetchone()[0]
                warm_count = warm.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} WHERE created_at >= %s AND created_at < %s"
                    ).format(sql.Identifier(route.parent)),
                    (generation.window_start, generation.window_end),
                ).fetchone()[0]
                parity.append(
                    {"parent": route.parent, "hot_rows": hot_count, "warm_rows": warm_count}
                )
                if hot_count != warm_count:
                    raise RuntimeError(
                        f"generation parity mismatch for {route.parent}: hot={hot_count} warm={warm_count}"
                    )
            result["parity"] = parity

            for route in manifest.tables:
                state = _catalog_state(hot, route.parent, route.leaf)
                if state is not CatalogLeafState.DETACHED:
                    raise RuntimeError(f"catalog check failed: {route.leaf} is {state.value}")
            mark("t12v")

            evidence = GateEvidence(True, True, True, True)
            transition(AttemptState.LOCKED, AttemptState.DRAINED, evidence)
            with warm.transaction():
                attempt_updated = warm.execute(
                    "UPDATE public.flip_attempts SET state='drained', updated_at=clock_timestamp() "
                    "WHERE attempt_epoch=%s AND state='locked'",
                    (attempt_epoch,),
                ).rowcount
                tracker_updated = warm.execute(
                    """
                    UPDATE public.partition_tracker
                    SET state='drained', version=version+1, updated_at=clock_timestamp()
                    WHERE cell=%s AND timeslot=%s AND state='locked' AND attempt_epoch=%s
                    """,
                    (settings.cell, generation.timeslot, attempt_epoch),
                ).rowcount
                if attempt_updated != 1 or tracker_updated != 1:
                    raise RuntimeError("attempt/tracker CAS failed while entering drained")
            mark("t12")
            transition(AttemptState.DRAINED, AttemptState.WARM_PRIMARY)
            with warm.transaction():
                attempt_updated = warm.execute(
                    "UPDATE public.flip_attempts SET state='warm_primary', updated_at=clock_timestamp() "
                    "WHERE attempt_epoch=%s AND state='drained'",
                    (attempt_epoch,),
                ).rowcount
                tracker_updated = warm.execute(
                    """
                    UPDATE public.partition_tracker
                    SET state='warm_primary', version=version+1, updated_at=clock_timestamp()
                    WHERE cell=%s AND timeslot=%s AND state='drained' AND attempt_epoch=%s
                    """,
                    (settings.cell, generation.timeslot, attempt_epoch),
                ).rowcount
                if attempt_updated != 1 or tracker_updated != 1:
                    raise RuntimeError("attempt/tracker CAS failed while granting warm_primary")
            mark("t13")
            result["outcome"] = "success"
        except (Exception, ParallelDetachError) as error:
            result["error"] = f"{type(error).__name__}: {error}"
            if parked and attempt_epoch is not None:
                recovered = revert_to_hot(
                    hot,
                    warm,
                    manifest,
                    attempt_epoch,
                    recovery_timeout_seconds,
                    window=(generation.window_start, generation.window_end),
                )
                reopen_hot_write_gate(
                    hot, settings.cell, generation.timeslot, attempt_id, attempt_epoch
                )
                result["recovered_tables"] = [item["leaf"] for item in recovered]
                result["outcome"] = "reverted"
            elif parked:
                reopen_hot_write_gate(
                    hot, settings.cell, generation.timeslot, attempt_id, None
                )
                result["outcome"] = "reverted"
        finally:
            result["stage_marks_ns"] = marks
            if "t2" in marks and "t13" in marks:
                result["writer_park_ns"] = marks["t13"] - marks["t2"]
                result["source_marker_proof_ns"] = marks["t7"] - marks["t5"]
                result["receipt_wait_ns"] = marks["t10"] - marks["t7"]
                result["sink_offset_gate_ns"] = marks["t11"] - marks["t10"]
                result["grant_ns"] = marks["t13"] - marks["t11"]
    return result


def _session_factory(
    settings: Settings,
    generation: GenerationSpec,
    run_id: uuid.UUID,
    payload_bytes: int,
) -> Callable[[], OptimisticDetachTransactionSession]:
    def factory() -> OptimisticDetachTransactionSession:
        return OptimisticDetachTransactionSession(
            settings.writer_hot_dsn,
            generation.manifest,
            run_id,
            generation.timeslot,
            payload_bytes,
            None,
            operations_per_batch=settings.table_count,
            admission_check_mode=OptimisticAdmissionCheckMode.STATE_ONLY,
            created_at=generation.window_start,
        )

    return factory


def run_rolling(
    settings: Settings,
    *,
    generations: int,
    active_tps: int,
    retiring_tps: int,
    duration_seconds: float,
    payload_bytes: int = 256,
    flip_timeout_seconds: float = 120.0,
    quiesce_seconds: float = 3.0,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Drive N consecutive generations: provision -> open -> rotate -> flip -> release."""
    if generations < 2:
        raise ValueError("rolling validation needs at least two generations")
    kafka = KafkaControl(settings.kafka_bootstrap)
    active_target = TrafficTarget(active_tps, 1, 8, 5_000)
    retiring_target = TrafficTarget(max(1, retiring_tps), 1, 2, 1_000)
    lane_free = {"lane_a": True, "lane_b": True}
    report: dict[str, Any] = {
        "variant": "H-DD-Prod",
        "source_topology": settings.source_topology,
        "table_count": settings.table_count,
        "generations": [],
        "outcome": "failed",
    }
    current: GenerationSpec | None = None
    current_lane_traffic: TrafficLane | None = None
    retiring_traffic: TrafficLane | None = None

    def flip_and_release(
        generation: GenerationSpec, traffic: TrafficLane, record: dict[str, Any]
    ) -> None:
        def stop_admission() -> Mapping[str, Any]:
            return traffic.stop_admission(10.0).to_dict()

        def resolve_in_flight() -> Mapping[str, Any]:
            return traffic.finish_in_flight(30.0).to_dict()

        flip = flip_generation(
            settings,
            generation,
            kafka,
            stop_admission=stop_admission,
            resolve_in_flight=resolve_in_flight,
            timeout_seconds=flip_timeout_seconds,
        )
        traffic.stop_and_join(10.0)
        record["flip"] = flip
        if flip["outcome"] != "success":
            raise RuntimeError(
                f"generation {generation.timeslot} flip did not grant warm ownership: "
                f"{flip.get('error', flip['outcome'])}"
            )
        lane_free[generation.lane] = True
        log(
            f"    flip {generation.timeslot}: outcome={flip['outcome']} "
            f"park={flip.get('writer_park_ns', 0) / 1e6:.1f}ms "
            f"marker_proof={flip.get('source_marker_proof_ns', 0) / 1e6:.1f}ms"
        )

    try:
        for index in range(generations):
            spec = GenerationSpec.build(settings, index)
            if not lane_free[spec.lane]:
                raise RuntimeError(
                    f"lane {spec.lane} is not free for generation {spec.timeslot}; "
                    "the previous generation on this lane has not been flipped"
                )
            log(f"[gen {index}] provisioning {spec.timeslot} on {spec.lane}")
            record: dict[str, Any] = {
                "index": index,
                "generation": spec.timeslot,
                "lane": spec.lane,
            }
            record["provision"] = provision_generation(settings, spec, kafka)
            lane_free[spec.lane] = False
            log(
                f"[gen {index}] canary passed in "
                f"{record['provision']['canary_ns'] / 1e6:.0f}ms; opening route"
            )
            run_id = uuid.uuid4()
            record["run_id"] = str(run_id)
            new_traffic = TrafficLane.start(
                _session_factory(settings, spec, run_id, payload_bytes),
                lambda: active_target,
                settings.table_count,
                operations_per_api_batch=settings.table_count,
            )
            previous = current
            previous_traffic = current_lane_traffic
            current = spec
            current_lane_traffic = new_traffic
            report["generations"].append(record)

            if previous is not None and previous_traffic is not None:
                demoted = previous_traffic.stop_admission(10.0)
                previous_traffic.finish_in_flight(30.0)
                previous_traffic.stop_and_join(10.0)
                previous_record = report["generations"][previous.index]
                previous_record["active_phase"] = demoted.to_dict()
                retiring_traffic = TrafficLane.start(
                    _session_factory(
                        settings, previous, uuid.UUID(previous_record["run_id"]), payload_bytes
                    ),
                    lambda: retiring_target,
                    settings.table_count,
                    operations_per_api_batch=settings.table_count,
                )
                log(
                    f"[gen {index}] boundary: {previous.timeslot} demoted to retiring, "
                    f"{spec.timeslot} active"
                )
                time.sleep(quiesce_seconds)
                flip_and_release(previous, retiring_traffic, previous_record)
                retiring_traffic = None
            time.sleep(duration_seconds)

        assert current is not None and current_lane_traffic is not None
        final_record = report["generations"][current.index]
        demoted = current_lane_traffic.stop_admission(10.0)
        current_lane_traffic.finish_in_flight(30.0)
        current_lane_traffic.stop_and_join(10.0)
        final_record["active_phase"] = demoted.to_dict()
        final_retiring = TrafficLane.start(
            _session_factory(settings, current, uuid.UUID(final_record["run_id"]), payload_bytes),
            lambda: retiring_target,
            settings.table_count,
            operations_per_api_batch=settings.table_count,
        )
        time.sleep(quiesce_seconds)
        flip_and_release(current, final_retiring, final_record)
        current_lane_traffic = None
        report["outcome"] = "success"
    finally:
        for lane_traffic in (current_lane_traffic, retiring_traffic):
            if lane_traffic is not None and lane_traffic.is_alive():
                try:
                    lane_traffic.stop_and_join(10.0)
                except Exception:
                    pass
    return report
