from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, Mapping


class ResultValidationError(ValueError):
    pass


_LSN_PATTERN = re.compile(r"^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$")


def _parse_lsn(value: object, label: str) -> int:
    if not isinstance(value, str) or _LSN_PATTERN.fullmatch(value) is None:
        raise ResultValidationError(f"{label} must be a PostgreSQL LSN")
    high, low = value.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def _validate_source_lane_evidence(
    evidence: object,
    source_topology: str,
    *,
    paused_at_t1: bool,
    fence_lane: str,
    fence_slot_name: str,
    fence_publication_name: str,
    fence_lsn: object,
    require_routing_identity: bool,
    declared_topic_prefixes: Mapping[str, str] | None,
    require_confirmed_fence_lsn: bool,
) -> None:
    if not isinstance(evidence, Mapping) or set(evidence) != {"t1", "t5", "t7", "t13"}:
        raise ResultValidationError("schema v2 requires t1/t5/t7/t13 source-lane evidence")
    expected_lanes = {"shared"} if source_topology == "shared" else {"active", "migration"}
    expected_slot_by_lane: dict[str, str] = {}
    expected_connector_by_lane: dict[str, str] = {}
    expected_publication_by_lane: dict[str, str] = {}
    expected_topic_prefix_by_lane: dict[str, str] = {}
    parsed_fence_lsn = _parse_lsn(fence_lsn, "schema v2 fence_lsn")
    for stage in ("t1", "t5", "t7", "t13"):
        snapshot = evidence[stage]
        if not isinstance(snapshot, Mapping) or set(snapshot) != expected_lanes:
            raise ResultValidationError(f"source-lane evidence at {stage} does not match topology")
        for lane, lane_evidence in snapshot.items():
            if not isinstance(lane_evidence, Mapping):
                raise ResultValidationError(f"source-lane evidence at {stage}/{lane} must be an object")
            expected_state = (
                "PAUSED"
                if stage == "t1" and paused_at_t1 and lane in ("shared", "migration")
                else "RUNNING"
            )
            task_states = lane_evidence.get("task_states")
            if (
                lane_evidence.get("connector_state") != expected_state
                or not isinstance(task_states, list)
                or not task_states
                or any(state != expected_state for state in task_states)
            ):
                raise ResultValidationError(
                    f"source-lane connector evidence at {stage}/{lane} is not {expected_state}"
                )
            if expected_state == "RUNNING" and lane_evidence.get("slot_active") is not True:
                raise ResultValidationError(f"source-lane slot at {stage}/{lane} was not active")
            slot_name = lane_evidence.get("slot_name")
            connector_name = lane_evidence.get("connector_name")
            if not isinstance(slot_name, str) or not 1 <= len(slot_name) <= 128:
                raise ResultValidationError(f"source-lane slot name at {stage}/{lane} is invalid")
            if not isinstance(connector_name, str) or not 1 <= len(connector_name) <= 128:
                raise ResultValidationError(
                    f"source-lane connector name at {stage}/{lane} is invalid"
                )
            publication_name = lane_evidence.get("publication_name")
            topic_prefix = lane_evidence.get("topic_prefix")
            if require_routing_identity and (
                not isinstance(publication_name, str)
                or not 1 <= len(publication_name) <= 128
            ):
                raise ResultValidationError(
                    f"source-lane publication name at {stage}/{lane} is invalid"
                )
            if require_routing_identity and (
                not isinstance(topic_prefix, str) or not 1 <= len(topic_prefix) <= 249
            ):
                raise ResultValidationError(
                    f"source-lane topic prefix at {stage}/{lane} is invalid"
                )
            if stage == "t1":
                expected_slot_by_lane[lane] = slot_name
                expected_connector_by_lane[lane] = connector_name
                if require_routing_identity:
                    expected_publication_by_lane[lane] = publication_name
                    expected_topic_prefix_by_lane[lane] = topic_prefix
            elif (
                slot_name != expected_slot_by_lane[lane]
                or connector_name != expected_connector_by_lane[lane]
                or (
                    require_routing_identity
                    and (
                        publication_name != expected_publication_by_lane[lane]
                        or topic_prefix != expected_topic_prefix_by_lane[lane]
                    )
                )
            ):
                raise ResultValidationError(
                    f"source-lane identity changed between stages for {lane}"
                )
            confirmed_lsn = _parse_lsn(
                lane_evidence.get("confirmed_lsn"),
                f"source-lane confirmed_lsn at {stage}/{lane}",
            )
            restart_lsn = lane_evidence.get("restart_lsn")
            if restart_lsn is not None:
                _parse_lsn(restart_lsn, f"source-lane restart_lsn at {stage}/{lane}")
            lag_bytes = lane_evidence.get("lag_bytes")
            if not isinstance(lag_bytes, int) or isinstance(lag_bytes, bool) or lag_bytes < 0:
                raise ResultValidationError(
                    f"source-lane lag_bytes at {stage}/{lane} must be non-negative"
                )
            if (
                require_confirmed_fence_lsn
                and stage in ("t7", "t13")
                and lane == fence_lane
                and confirmed_lsn < parsed_fence_lsn
            ):
                raise ResultValidationError(
                    f"fence lane had not confirmed fence_lsn at {stage}"
                )
    if expected_slot_by_lane.get(fence_lane) != fence_slot_name:
        raise ResultValidationError("source-lane evidence fence slot contradicts topology")
    if (
        require_routing_identity
        and expected_publication_by_lane.get(fence_lane) != fence_publication_name
    ):
        raise ResultValidationError(
            "source-lane evidence fence publication contradicts topology"
        )
    if len(set(expected_slot_by_lane.values())) != len(expected_slot_by_lane):
        raise ResultValidationError("isolated source-lane slots must be unique")
    if len(set(expected_connector_by_lane.values())) != len(expected_connector_by_lane):
        raise ResultValidationError("isolated source-lane connectors must be unique")
    if require_routing_identity and len(set(expected_publication_by_lane.values())) != len(
        expected_publication_by_lane
    ):
        raise ResultValidationError("isolated source-lane publications must be unique")
    if require_routing_identity and len(set(expected_topic_prefix_by_lane.values())) != len(
        expected_topic_prefix_by_lane
    ):
        raise ResultValidationError("isolated source-lane topic prefixes must be unique")
    if require_routing_identity and expected_topic_prefix_by_lane != declared_topic_prefixes:
        raise ResultValidationError(
            "source-lane evidence topic prefixes contradict topology"
        )


def _validate_hot_identity(
    payload: Mapping[str, Any], topology: Mapping[str, Any]
) -> None:
    identity = payload.get("hot_identity")
    if not isinstance(identity, Mapping):
        raise ResultValidationError("schema v2+ requires complete hot identity")
    system_identifier = identity.get("system_identifier")
    database = identity.get("database")
    cell = identity.get("cell")
    declared_cell = payload.get("cell", "cell01")
    if (
        identity.get("slot") != topology.get("fence_slot_name")
        or cell != declared_cell
        or database != "cards"
        or not isinstance(system_identifier, str)
        or not system_identifier.isdigit()
        or not 1 <= len(system_identifier) <= 20
    ):
        raise ResultValidationError("schema v2+ hot identity contradicts fence source")


def _validate_topology_provenance(payload: Mapping[str, Any], *, checkpoint: bool) -> None:
    schema_version = payload.get("schema_version")
    if schema_version not in (1, 2, 3, 4, 5, 6, 7):
        raise ResultValidationError("schema_version must be between 1 and 7")
    if schema_version == 1:
        return
    topology = payload if checkpoint else payload.get("topology")
    if not isinstance(topology, Mapping):
        raise ResultValidationError("schema v2 requires topology provenance")
    source_topology = topology.get("source_topology")
    expected = {
        "shared": (1, "shared"),
        "isolated": (2, "migration"),
    }.get(source_topology)
    if expected is None:
        raise ResultValidationError("schema v2 source_topology must be shared or isolated")
    connector_count, fence_lane = expected
    if topology.get("source_connector_count") != connector_count:
        raise ResultValidationError("schema v2 source connector count contradicts topology")
    if topology.get("fence_source_lane") != fence_lane:
        raise ResultValidationError("schema v2 fence lane contradicts topology")
    for field in ("fence_slot_name", "fence_publication_name"):
        value = topology.get(field)
        if not isinstance(value, str) or not 1 <= len(value) <= 128:
            raise ResultValidationError(f"schema v2 requires a bounded {field}")
    declared_topic_prefixes: Mapping[str, str] | None = None
    if schema_version >= 3:
        candidate_prefixes = topology.get("source_topic_prefixes")
        if (
            not isinstance(candidate_prefixes, Mapping)
            or set(candidate_prefixes) != ({"shared"} if source_topology == "shared" else {"active", "migration"})
            or any(
                not isinstance(value, str) or not 1 <= len(value) <= 249
                for value in candidate_prefixes.values()
            )
        ):
            raise ResultValidationError(
                "schema v3 requires exact per-lane source topic prefixes"
            )
        declared_topic_prefixes = dict(candidate_prefixes)
    successful = (
        payload.get("ownership_outcome") == "success"
        if checkpoint
        else payload.get("outcome") == "success"
    )
    if successful:
        scenario = payload.get("scenario")
        source_proof_mode = (
            scenario.get("source_proof_mode")
            if isinstance(scenario, Mapping)
            else None
        )
        _validate_source_lane_evidence(
            payload.get("source_lane_evidence"),
            source_topology,
            paused_at_t1=str(payload.get("profile", "")).endswith("-paused-backlog"),
            fence_lane=fence_lane,
            fence_slot_name=str(topology.get("fence_slot_name")),
            fence_publication_name=str(topology.get("fence_publication_name")),
            fence_lsn=payload.get("fence_lsn"),
            require_routing_identity=schema_version >= 3,
            declared_topic_prefixes=declared_topic_prefixes,
            require_confirmed_fence_lsn=(
                source_proof_mode
                not in (
                    "per_leaf_marker_v1",
                    "atomic_detach_marker_v1",
                    "parallel_atomic_detach_marker_v1",
                )
            ),
        )
        _validate_hot_identity(payload, topology)


def _validate_fence_wakeup(payload: Mapping[str, Any], *, checkpoint: bool) -> None:
    if payload.get("schema_version") not in (4, 5, 6, 7):
        return
    topology = payload if checkpoint else payload.get("topology")
    evidence = payload.get("fence_wakeup")
    if not isinstance(topology, Mapping) or not isinstance(evidence, Mapping):
        raise ResultValidationError("schema v4 requires fence wakeup evidence")
    required_fields = {
        "mode",
        "lane",
        "heartbeat_table",
        "attempted",
        "applied",
        "rows_updated",
        "post_update_wal_lsn",
        "duration_ns",
        "confirmed_flush_lsn_at_t7",
    }
    if set(evidence) != required_fields:
        raise ResultValidationError("schema v4 fence wakeup requires exact fields")
    mode = evidence.get("mode")
    if mode not in ("passive", "immediate_heartbeat"):
        raise ResultValidationError("fence wakeup mode is invalid")
    scenario = payload.get("scenario")
    if isinstance(scenario, Mapping) and scenario.get("fence_wakeup_mode") != mode:
        raise ResultValidationError("scenario contradicts fence wakeup mode")
    if evidence.get("lane") != topology.get("fence_source_lane"):
        raise ResultValidationError("fence wakeup lane contradicts topology")
    expected_table = (
        "dbz_heartbeat_migration"
        if topology.get("source_topology") == "isolated"
        else "dbz_heartbeat"
    )
    if evidence.get("heartbeat_table") != expected_table:
        raise ResultValidationError("fence wakeup heartbeat table contradicts topology")
    duration = evidence.get("duration_ns")
    rows_updated = evidence.get("rows_updated")
    attempted = evidence.get("attempted")
    applied = evidence.get("applied")
    if (
        not isinstance(duration, int)
        or isinstance(duration, bool)
        or duration < 0
        or not isinstance(rows_updated, int)
        or isinstance(rows_updated, bool)
        or rows_updated < 0
    ):
        raise ResultValidationError("fence wakeup duration and row count must be non-negative")
    if not isinstance(attempted, bool) or not isinstance(applied, bool):
        raise ResultValidationError("fence wakeup attempted and applied must be booleans")
    successful = (
        payload.get("ownership_outcome") == "success"
        if checkpoint
        else payload.get("outcome") == "success"
    )
    marker_proof = (
        isinstance(scenario, Mapping)
        and scenario.get("source_proof_mode")
        in (
            "per_leaf_marker_v1",
            "atomic_detach_marker_v1",
            "parallel_atomic_detach_marker_v1",
        )
    )
    if marker_proof and mode != "passive":
        raise ResultValidationError("per-leaf marker proof requires passive heartbeat mode")
    if mode == "passive":
        if (
            attempted
            or applied
            or rows_updated != 0
            or evidence.get("post_update_wal_lsn") is not None
        ):
            raise ResultValidationError("passive fence wakeup cannot contain a heartbeat update")
    else:
        post_update_raw = evidence.get("post_update_wal_lsn")
        if applied:
            if not attempted or rows_updated != 1:
                raise ResultValidationError(
                    "applied fence wakeup must update exactly one row"
                )
            if post_update_raw is None:
                if successful:
                    raise ResultValidationError(
                        "successful fence wakeup requires a post-update WAL position"
                    )
            else:
                post_update_lsn = _parse_lsn(
                    post_update_raw,
                    "fence wakeup post-update WAL position",
                )
                if post_update_lsn <= _parse_lsn(
                    payload.get("fence_lsn"), "fence_lsn"
                ):
                    raise ResultValidationError(
                        "fence wakeup WAL position must be after the fence"
                    )
        elif rows_updated != 0 or post_update_raw is not None:
            raise ResultValidationError(
                "not-applied fence wakeup cannot contain committed update evidence"
            )
        if successful and not applied:
            raise ResultValidationError("successful immediate heartbeat was not applied exactly once")

    confirmed_raw = evidence.get("confirmed_flush_lsn_at_t7")
    if (
        mode == "immediate_heartbeat"
        and confirmed_raw is not None
        and (
            not attempted
            or not applied
            or rows_updated != 1
            or evidence.get("post_update_wal_lsn") is None
        )
    ):
        raise ResultValidationError(
            "immediate fence confirmation requires a fully observed heartbeat update"
        )
    confirmed_at_t7: int | None = None
    if confirmed_raw is not None:
        confirmed_at_t7 = _parse_lsn(
            confirmed_raw,
            "fence wakeup confirmed WAL position at t7",
        )
        if confirmed_at_t7 < _parse_lsn(payload.get("fence_lsn"), "fence_lsn"):
            raise ResultValidationError("fence wakeup confirmed WAL position is before the fence")
    elif successful and not marker_proof:
        raise ResultValidationError("successful fence wakeup requires a t7 WAL confirmation")

    if confirmed_at_t7 is not None:
        lane = str(topology.get("fence_source_lane"))
        source_evidence = payload.get("source_lane_evidence")
        try:
            lane_confirmed_at_t7 = source_evidence["t7"][lane]["confirmed_lsn"]
        except (KeyError, TypeError):
            if successful:
                raise ResultValidationError(
                    "fence wakeup requires matching t7 source evidence"
                ) from None
        else:
            if _parse_lsn(
                lane_confirmed_at_t7, "fence lane t7 source evidence"
            ) < confirmed_at_t7:
                raise ResultValidationError(
                    "fence lane t7 source evidence is behind the recorded confirmation"
                )
    stages = payload.get("stages_ns")
    if successful:
        if not isinstance(stages, Mapping) or any(
            stage not in stages for stage in ("t6", "t6w", "t7")
        ):
            raise ResultValidationError("successful schema v4 result requires t6/t6w/t7")
        t6, t6w, t7 = (stages[name] for name in ("t6", "t6w", "t7"))
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (t6, t6w, t7)
        ) or not t6 <= t6w <= t7:
            raise ResultValidationError("fence wakeup stages must satisfy t6 <= t6w <= t7")
        if duration > t6w - t6:
            raise ResultValidationError("fence wakeup duration exceeds its stage interval")


def _validate_marker_fence(payload: Mapping[str, Any], *, checkpoint: bool) -> None:
    schema_version = payload.get("schema_version")
    if schema_version not in (5, 6, 7):
        return
    evidence = payload.get("marker_fence")
    scenario = payload.get("scenario")
    successful = (
        payload.get("ownership_outcome") == "success"
        if checkpoint
        else payload.get("outcome") == "success"
    )
    if evidence is None and not successful:
        return
    if not isinstance(evidence, Mapping) or not isinstance(scenario, Mapping):
        raise ResultValidationError("marker schema requires marker fence evidence and scenario")
    expected_mode = {
        5: "per_leaf_marker_v1",
        6: "atomic_detach_marker_v1",
        7: "parallel_atomic_detach_marker_v1",
    }[schema_version]
    if scenario.get("source_proof_mode") != expected_mode:
        raise ResultValidationError(
            f"schema v{schema_version} requires {expected_mode} source proof"
        )
    required = {
        "mode",
        "marker_schema_version",
        "exactly_once_source",
        "consumer_isolation",
        "marker_ids",
        "scan_start_offsets",
        "marker_next_offsets",
        "emission_duration_ns",
        "warm_receipts_complete",
    }
    if schema_version in (6, 7):
        required.update(
            {
                "detach_marker_contract",
                "detach_before_marker",
                "atomic_transaction_ns_by_leaf",
            }
        )
    if schema_version == 7:
        required.update(
            {
                "detach_execution_mode",
                "detach_parallelism",
                "parallel_wall_duration_ns",
            }
        )
    if set(evidence) != required:
        raise ResultValidationError(
            f"schema v{schema_version} marker fence evidence fields are incomplete"
        )
    if (
        evidence.get("mode") != expected_mode
        or evidence.get("marker_schema_version") != 1
        or evidence.get("exactly_once_source") is not True
        or evidence.get("consumer_isolation") != "read_committed"
    ):
        raise ResultValidationError("marker transport contract is invalid")
    if schema_version in (6, 7):
        atomic_timings = evidence.get("atomic_transaction_ns_by_leaf")
        expected_contract = (
            "per_leaf_parallel_transactions_v1"
            if schema_version == 7
            else "per_leaf_single_transaction_v1"
        )
        if (
            evidence.get("detach_marker_contract")
            != expected_contract
            or evidence.get("detach_before_marker") is not True
        ):
            raise ResultValidationError(
                f"schema v{schema_version} requires the detach-before-marker atomic contract"
            )
        if (
            not isinstance(atomic_timings, Mapping)
            or len(atomic_timings) != payload.get("table_count")
            or any(
                not isinstance(key, str)
                or not key
                or not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                for key, value in atomic_timings.items()
            )
        ):
            raise ResultValidationError(
                "schema v6 atomic transaction timings must cover every leaf"
            )
        if schema_version == 7:
            parallelism = evidence.get("detach_parallelism")
            wall_duration = evidence.get("parallel_wall_duration_ns")
            if (
                evidence.get("detach_execution_mode") != "all_parallel_v1"
                or parallelism != payload.get("table_count")
                or not isinstance(wall_duration, int)
                or isinstance(wall_duration, bool)
                or wall_duration < 0
            ):
                raise ResultValidationError(
                    "schema v7 parallelism and wall duration must cover every table"
                )
    duration = evidence.get("emission_duration_ns")
    if not isinstance(duration, int) or isinstance(duration, bool) or duration < 0:
        raise ResultValidationError("schema v5 marker emission duration is invalid")
    target = payload.get("target_next_offsets")
    marker_ids = evidence.get("marker_ids")
    baselines = evidence.get("scan_start_offsets")
    marker_offsets = evidence.get("marker_next_offsets")
    if not all(isinstance(value, Mapping) for value in (target, marker_ids, baselines, marker_offsets)):
        raise ResultValidationError("schema v5 marker vectors must be objects")
    expected_keys = set(target)
    if not expected_keys or any(set(vector) != expected_keys for vector in (marker_ids, baselines, marker_offsets)):
        raise ResultValidationError("schema v5 marker vectors must match the target vector")
    for marker_id in marker_ids.values():
        try:
            uuid.UUID(str(marker_id))
        except (ValueError, AttributeError) as error:
            raise ResultValidationError("schema v5 marker IDs must be UUIDs") from error
    max_offset = (1 << 63) - 1
    if any(
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= max_offset
        for value in (*baselines.values(), *marker_offsets.values())
    ):
        raise ResultValidationError("schema v5 marker offsets are invalid")
    if any(baselines[key] >= marker_offsets[key] for key in expected_keys):
        raise ResultValidationError("schema v5 marker must occur after its scan baseline")
    if dict(marker_offsets) != dict(target):
        raise ResultValidationError("schema v5 target must equal the exact marker offset vector")
    if successful and evidence.get("warm_receipts_complete") is not True:
        raise ResultValidationError("schema v5 ownership requires every warm marker receipt")


def validate_result(payload: Mapping[str, Any]) -> None:
    required = {
        "schema_version",
        "run_id",
        "attempt_id",
        "outcome",
        "profile",
        "table_count",
        "stages_ns",
        "detach_ns_by_table",
        "target_next_offsets",
        "final_committed_next_offsets",
        "error",
    }
    missing = required.difference(payload)
    if missing:
        raise ResultValidationError(f"result is missing fields: {sorted(missing)}")
    _validate_topology_provenance(payload, checkpoint=False)
    _validate_fence_wakeup(payload, checkpoint=False)
    _validate_marker_fence(payload, checkpoint=False)
    try:
        uuid.UUID(str(payload["run_id"]))
        uuid.UUID(str(payload["attempt_id"]))
    except (ValueError, AttributeError) as error:
        raise ResultValidationError("run_id and attempt_id must be UUIDs") from error
    table_count = payload["table_count"]
    if table_count not in (5, 10, 15, 20):
        raise ResultValidationError("table_count must be 5, 10, 15, or 20")
    outcome = payload["outcome"]
    if outcome not in ("success", "failed", "reverted"):
        raise ResultValidationError("outcome must be success, failed, or reverted")
    if outcome in ("failed", "reverted"):
        if not isinstance(payload["error"], Mapping):
            raise ResultValidationError("failed and reverted results require an error object")
        return
    if payload["profile"] not in (
        "local-rf1-paused-backlog",
        "local-rf1-running",
        "local-rf1-healthy-overload",
        "local-rf1-production-shaped",
        "local-rf3-single-host-production-shaped",
    ):
        raise ResultValidationError("successful result has an unknown benchmark profile")
    attempt_epoch = payload.get("attempt_epoch")
    if not isinstance(attempt_epoch, int) or isinstance(attempt_epoch, bool) or attempt_epoch <= 0:
        raise ResultValidationError("successful result requires a positive attempt_epoch")
    if payload["error"] is not None:
        raise ResultValidationError("successful results cannot contain an error")
    stages = payload["stages_ns"]
    scenario = payload.get("scenario")
    scenario = scenario if isinstance(scenario, Mapping) else {}
    write_fence_mode = scenario.get("write_fence_mode", "warm_tracker_advisory_v1")
    if write_fence_mode not in (
        "warm_tracker_advisory_v1",
        "hot_transactional_v1",
        "optimistic_detach_v1",
    ):
        raise ResultValidationError("successful result has an unknown write-fence mode")
    required_stages = {
        "t0", "t1", "t2", "t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "tverify"
    }
    requires_quiescence_stage = payload["profile"] in (
        "local-rf1-healthy-overload",
        "local-rf1-production-shaped",
        "local-rf3-single-host-production-shaped",
    )
    if requires_quiescence_stage:
        required_stages.add("t2q")
    hot_gate_mode = write_fence_mode in (
        "hot_transactional_v1",
        "optimistic_detach_v1",
    )
    if hot_gate_mode:
        required_stages.update(("t2h", "t2w"))
    if write_fence_mode == "optimistic_detach_v1":
        required_stages.add("t2f")
    required_stages.update({f"t3_{index}" for index in range(1, table_count + 1)})
    required_stages.update({f"t4_{index}" for index in range(1, table_count + 1)})
    parallel_detach = payload.get("schema_version") == 7
    if parallel_detach:
        required_stages.update(("t3_parallel", "t4_parallel"))
    if not required_stages.issubset(stages):
        raise ResultValidationError(f"successful result is missing stages: {sorted(required_stages.difference(stages))}")
    ordered_stages = ["t0", "t1", "t2"]
    if hot_gate_mode:
        ordered_stages.extend(("t2h", "t2w"))
    if write_fence_mode == "optimistic_detach_v1":
        ordered_stages.append("t2f")
    elif requires_quiescence_stage:
        ordered_stages.append("t2q")
    if parallel_detach:
        ordered_stages.extend(
            f"t3_{index}" for index in range(1, table_count + 1)
        )
        ordered_stages.append("t3_parallel")
        ordered_stages.extend(
            f"t4_{index}" for index in range(1, table_count + 1)
        )
        ordered_stages.append("t4_parallel")
    else:
        for index in range(1, table_count + 1):
            ordered_stages.extend((f"t3_{index}", f"t4_{index}"))
    if write_fence_mode == "optimistic_detach_v1" and requires_quiescence_stage:
        ordered_stages.append("t2q")
    ordered_stages.extend(("t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "tverify"))
    ordered_values = [stages[name] for name in ordered_stages]
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in ordered_values):
        raise ResultValidationError("successful result stage timestamps must be non-negative integers")
    if any(right < left for left, right in zip(ordered_values, ordered_values[1:])):
        raise ResultValidationError("successful result stage timestamps are not monotonic")
    if hot_gate_mode:
        write_fence = payload.get("write_fence")
        admitted_epoch = scenario.get("retiring_write_gate_epoch")
        if (
            not isinstance(write_fence, Mapping)
            or write_fence.get("mode") != write_fence_mode
            or write_fence.get("state_before") != "open"
            or write_fence.get("state_after") != "parked"
            or write_fence.get("park_attempt_id") != str(payload["attempt_id"])
            or write_fence.get("ownership_epoch") != admitted_epoch
            or write_fence.get("attempt_epoch") != attempt_epoch
            or write_fence.get("verified_before_grant") is not True
            or write_fence.get("active_gate_state") != "open"
            or write_fence.get("reopened") is not False
        ):
            raise ResultValidationError(
                "hot-gated result lacks exact ownership-fence evidence"
            )
        if write_fence_mode == "optimistic_detach_v1":
            transaction_shape = scenario.get("transaction_shape")
            tables_per_transaction = scenario.get("tables_per_api_transaction")
            contract_version = scenario.get("optimistic_contract_version")
            if contract_version is None:
                corrected_shape_without_label = (
                    transaction_shape is None and tables_per_transaction == 1
                )
                legacy_all_tables = (
                    transaction_shape is None and tables_per_transaction == table_count
                )
                if not (corrected_shape_without_label or legacy_all_tables) and (
                    transaction_shape != "single_table_api"
                    or tables_per_transaction != 1
                ):
                    raise ResultValidationError(
                        "Variant E must declare one selected table operation per PostgreSQL transaction"
                    )
            elif contract_version == "batch_admission_separate_commits_v1":
                if (
                    transaction_shape != "api_batch_separate_commits_v1"
                    or tables_per_transaction != 1
                    or scenario.get("operations_per_api_batch") != table_count
                    or scenario.get("ownership_reads_per_api_batch") != 1
                    or scenario.get("partial_batch_completion_allowed") is not True
                ):
                    raise ResultValidationError(
                        "legacy Variant E batch-admission evidence is inconsistent"
                    )
            elif contract_version == "batch_first_write_admission_v2":
                if (
                    transaction_shape != "api_batch_separate_commits_v1"
                    or tables_per_transaction != 1
                    or scenario.get("operations_per_api_batch") != table_count
                    or scenario.get("ownership_reads_per_api_batch") != 1
                    or scenario.get("postgres_transactions_per_api_batch") != table_count
                    or scenario.get("partial_batch_completion_allowed") is not True
                ):
                    raise ResultValidationError(
                        "legacy Variant E first-write admission evidence is inconsistent"
                    )
            elif contract_version == "state_only_batch_first_write_admission_v4":
                if (
                    scenario.get("source_proof_mode")
                    != "parallel_atomic_detach_marker_v1"
                    or scenario.get("optimistic_admission_check_mode")
                    != "state_only_v1"
                    or transaction_shape != "api_batch_separate_commits_v1"
                    or tables_per_transaction != 1
                    or scenario.get("operations_per_api_batch") != table_count
                    or scenario.get("ownership_reads_per_api_batch") != 1
                    or scenario.get("ownership_epoch_checks_per_api_batch") != 0
                    or scenario.get("postgres_transactions_per_api_batch")
                    != table_count
                    or scenario.get("partial_batch_completion_allowed") is not True
                    or scenario.get("api_batch_scheduling")
                    != "single_worker_reserved_v1"
                ):
                    raise ResultValidationError(
                        "Variant H state-only batch-admission evidence is inconsistent"
                    )
            else:
                if (
                    contract_version != "reserved_batch_first_write_admission_v3"
                    or transaction_shape != "api_batch_separate_commits_v1"
                    or tables_per_transaction != 1
                    or scenario.get("operations_per_api_batch") != table_count
                ):
                    raise ResultValidationError(
                        "Variant E must declare one selected table operation per PostgreSQL transaction"
                    )
                if scenario.get("ownership_reads_per_api_batch") != 1:
                    raise ResultValidationError(
                        "Variant E must declare one ownership read per API batch"
                    )
                if scenario.get("partial_batch_completion_allowed") is not True:
                    raise ResultValidationError(
                        "Variant E must declare that partial completion is allowed"
                    )
                if scenario.get("postgres_transactions_per_api_batch") != table_count:
                    raise ResultValidationError(
                        "Variant E must use one PostgreSQL transaction per operation"
                    )
                if scenario.get("api_batch_scheduling") != "single_worker_reserved_v1":
                    raise ResultValidationError(
                        "Variant E API batch must be reserved to one worker"
                    )
    if payload.get("ownership_outcome") != "success":
        raise ResultValidationError("successful result requires a successful ownership outcome")
    verification_outcome = payload.get("verification_outcome")
    if verification_outcome not in ("passed", "failed"):
        raise ResultValidationError("successful result requires a terminal verification outcome")
    expected_gc_eligible = verification_outcome == "passed"
    if payload.get("gc_eligible") is not expected_gc_eligible:
        raise ResultValidationError("GC eligibility must exactly match post-grant verification")
    if len(payload["detach_ns_by_table"]) != table_count:
        raise ResultValidationError("successful result must contain one detach duration per table")
    if len(payload["target_next_offsets"]) != table_count:
        raise ResultValidationError("successful result must contain the complete target offset vector")
    if len(payload["final_committed_next_offsets"]) != table_count:
        raise ResultValidationError("successful result must contain the complete committed offset vector")
    target = payload["target_next_offsets"]
    committed = payload["final_committed_next_offsets"]
    if set(target) != set(committed):
        raise ResultValidationError("target and committed offset vectors must have identical keys")
    max_offset = (1 << 63) - 1
    if any(
        not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= max_offset
        for value in (*target.values(), *committed.values())
    ):
        raise ResultValidationError("Kafka offsets must be non-negative signed 64-bit integers")
    if payload.get("schema_version") not in (5, 6, 7) and any(
        committed[key] < target[key] for key in target
    ):
        raise ResultValidationError("successful result contains a committed offset behind its target")
    if payload["profile"] == "local-rf1-healthy-overload":
        scenario = payload.get("scenario")
        admission = payload.get("admission")
        if not isinstance(scenario, Mapping) or scenario.get("mode") != "healthy-overload":
            raise ResultValidationError("healthy-overload result requires scenario metadata")
        if not isinstance(admission, Mapping) or admission.get("connector_state") != "RUNNING":
            raise ResultValidationError("healthy-overload admission requires RUNNING connectors")
        min_source_bytes = scenario.get("min_source_lag_bytes")
        min_source_records = scenario.get("min_source_lag_records_per_partition")
        min_sink_records = scenario.get("min_sink_lag_records_per_partition")
        required_samples = scenario.get("required_stable_samples")
        max_admitted_rows = scenario.get("max_admitted_rows_per_partition")
        stable_window = scenario.get("stable_window")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in (
                min_source_bytes,
                min_source_records,
                min_sink_records,
                required_samples,
                max_admitted_rows,
            )
        ):
            raise ResultValidationError("healthy-overload thresholds must be positive integers")
        if not isinstance(stable_window, list) or len(stable_window) < required_samples:
            raise ResultValidationError("healthy-overload result lacks its stable admission window")
        source_lag_bytes = admission.get("source_lag_bytes")
        source_lag = admission.get("source_lag_records_by_partition")
        sink_lag = admission.get("sink_lag_by_partition")
        if not isinstance(source_lag_bytes, int) or source_lag_bytes < min_source_bytes:
            raise ResultValidationError("healthy-overload source lag is below its admission floor")
        if not isinstance(source_lag, Mapping) or set(source_lag) != set(target):
            raise ResultValidationError("healthy-overload source lag vector does not match the manifest")
        if any(not isinstance(value, int) or value < min_source_records for value in source_lag.values()):
            raise ResultValidationError("healthy-overload source record lag is below its per-partition floor")
        if not isinstance(sink_lag, Mapping) or set(sink_lag) != set(target):
            raise ResultValidationError("healthy-overload sink lag vector does not match the manifest")
        t1_min_sink_records = scenario.get("t1_min_sink_lag_records_per_partition")
        if (
            not isinstance(t1_min_sink_records, int)
            or isinstance(t1_min_sink_records, bool)
            or t1_min_sink_records <= 0
        ):
            raise ResultValidationError("healthy-overload t1 sink floor must be a positive integer")
        if any(
            not isinstance(value, int) or value < t1_min_sink_records
            for value in sink_lag.values()
        ):
            raise ResultValidationError("healthy-overload sink lag is below its t1 per-partition floor")
        if admission.get("writer_active_at_t1") is not True:
            raise ResultValidationError("healthy-overload writer was not active at t1")
        writer_inserted_at_t1 = admission.get("writer_inserted_at_t1")
        if (
            not isinstance(writer_inserted_at_t1, int)
            or writer_inserted_at_t1 <= 0
            or writer_inserted_at_t1 > max_admitted_rows * table_count
        ):
            raise ResultValidationError("healthy-overload workload exceeded its pre-lock admission cap")
        for sample in stable_window:
            if not isinstance(sample, Mapping):
                raise ResultValidationError("healthy-overload stable samples must be objects")
            sample_source = sample.get("source_lag_records_by_partition")
            sample_sink = sample.get("sink_lag_records_by_partition")
            if not isinstance(sample_source, Mapping) or set(sample_source) != set(target):
                raise ResultValidationError("stable source lag vector does not match the manifest")
            if not isinstance(sample_sink, Mapping) or set(sample_sink) != set(target):
                raise ResultValidationError("stable sink lag vector does not match the manifest")
            if any(value < min_source_records for value in sample_source.values()):
                raise ResultValidationError("stable source lag is below its per-partition floor")
            if any(value < min_sink_records for value in sample_sink.values()):
                raise ResultValidationError("stable sink lag is below its per-partition floor")
        workload_inserted_total = payload.get("workload_inserted_total")
        if (
            not isinstance(workload_inserted_total, int)
            or isinstance(workload_inserted_total, bool)
            or workload_inserted_total <= 0
        ):
            raise ResultValidationError("healthy-overload result requires a positive committed workload")
    if payload["profile"] in ("local-rf1-production-shaped", "local-rf3-single-host-production-shaped"):
        scenario = payload.get("scenario")
        admission = payload.get("admission")
        workload = payload.get("workload_by_timeslot")
        if not isinstance(scenario, Mapping) or scenario.get("mode") != "production-shaped":
            raise ResultValidationError("production-shaped result requires its scenario metadata")
        max_source_lag = scenario.get("max_source_lag_bytes")
        max_sink_lag = scenario.get("max_sink_lag_records_per_partition")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (max_source_lag, max_sink_lag)
        ):
            raise ResultValidationError("production-shaped lag ceilings must be non-negative integers")
        if not isinstance(admission, Mapping) or admission.get("connector_state") != "RUNNING":
            raise ResultValidationError("production-shaped admission requires RUNNING connectors")
        source_lag = admission.get("source_lag_bytes")
        sink_lag = admission.get("sink_lag_by_partition")
        if not isinstance(source_lag, int) or isinstance(source_lag, bool) or not 0 <= source_lag <= max_source_lag:
            raise ResultValidationError("production-shaped source lag exceeds its admission ceiling")
        if not isinstance(sink_lag, Mapping) or set(sink_lag) != set(payload["target_next_offsets"]):
            raise ResultValidationError("production-shaped sink lag vector does not match the manifest")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= max_sink_lag
            for value in sink_lag.values()
        ):
            raise ResultValidationError("production-shaped sink lag exceeds its per-partition ceiling")
        if not isinstance(workload, Mapping) or set(workload) != {"t1", "t2q", "t13"}:
            raise ResultValidationError("production-shaped result requires t1/t2q/t13 workload evidence")
        snapshots = tuple(workload[name] for name in ("t1", "t2q", "t13"))
        if any(not isinstance(snapshot, Mapping) for snapshot in snapshots):
            raise ResultValidationError("production-shaped workload snapshots must be objects")
        active = tuple(snapshot.get("active") for snapshot in snapshots)
        retiring = tuple(snapshot.get("retiring") for snapshot in snapshots)
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in (*active, *retiring)):
            raise ResultValidationError("production-shaped workload counters must be non-negative integers")
        if active[1] < active[0] or active[2] <= active[1]:
            raise ResultValidationError("active workload must make positive progress after retiring quiescence")
        if retiring[2] != retiring[1]:
            raise ResultValidationError("retiring workload advanced after writer quiescence")
        if snapshots[2].get("writer_alive") is not True:
            raise ResultValidationError("active writer was not alive at warm ownership grant")
    if payload.get("hot_identity") is None or not payload.get("fence_lsn"):
        raise ResultValidationError("successful result requires hot identity and fence LSN")


def validate_ownership_checkpoint(payload: Mapping[str, Any]) -> None:
    required = {
        "schema_version",
        "artifact_type",
        "non_authoritative",
        "run_id",
        "attempt_id",
        "attempt_epoch",
        "ownership_outcome",
        "verification_outcome",
        "table_count",
        "cell",
        "timeslot",
        "environment_generation_id",
        "stages_ns",
        "durations_ns",
        "target_next_offsets",
        "final_committed_next_offsets",
    }
    missing = required.difference(payload)
    if missing:
        raise ResultValidationError(f"ownership checkpoint is missing fields: {sorted(missing)}")
    _validate_topology_provenance(payload, checkpoint=True)
    _validate_fence_wakeup(payload, checkpoint=True)
    _validate_marker_fence(payload, checkpoint=True)
    if payload["artifact_type"] != "ownership_grant" or payload["non_authoritative"] is not True:
        raise ResultValidationError("ownership checkpoint must be non-authoritative history")
    if payload["ownership_outcome"] != "success" or payload["verification_outcome"] != "pending":
        raise ResultValidationError("ownership checkpoint requires successful grant and pending verification")
    try:
        uuid.UUID(str(payload["run_id"]))
        uuid.UUID(str(payload["attempt_id"]))
    except (ValueError, AttributeError) as error:
        raise ResultValidationError("ownership checkpoint IDs must be UUIDs") from error
    if payload["table_count"] not in (5, 10, 15, 20):
        raise ResultValidationError("ownership checkpoint table_count is unsupported")
    attempt_epoch = payload["attempt_epoch"]
    if not isinstance(attempt_epoch, int) or isinstance(attempt_epoch, bool) or attempt_epoch <= 0:
        raise ResultValidationError("ownership checkpoint requires a positive attempt_epoch")
    if not isinstance(payload["cell"], str) or not 1 <= len(payload["cell"]) <= 64:
        raise ResultValidationError("ownership checkpoint requires a bounded cell")
    if not isinstance(payload["timeslot"], str) or not 1 <= len(payload["timeslot"]) <= 64:
        raise ResultValidationError("ownership checkpoint requires a bounded timeslot")
    generation_id = payload["environment_generation_id"]
    if generation_id != "legacy":
        try:
            uuid.UUID(str(generation_id))
        except (ValueError, AttributeError) as error:
            raise ResultValidationError(
                "ownership checkpoint environment_generation_id must be legacy or a UUID"
            ) from error
    stages = payload["stages_ns"]
    if not isinstance(stages, Mapping):
        raise ResultValidationError("ownership checkpoint stages_ns must be an object")
    t13 = stages.get("t13")
    if not isinstance(t13, int) or isinstance(t13, bool) or t13 < 0:
        raise ResultValidationError("ownership checkpoint requires a non-negative t13")
    durations = payload["durations_ns"]
    if not isinstance(durations, Mapping) or any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in durations.values()
    ):
        raise ResultValidationError("ownership checkpoint durations must be non-negative integers")
    targets = payload["target_next_offsets"]
    currents = payload["final_committed_next_offsets"]
    if not isinstance(targets, Mapping) or not isinstance(currents, Mapping):
        raise ResultValidationError("ownership checkpoint offset vectors must be objects")
    if set(targets) != set(currents):
        raise ResultValidationError("ownership checkpoint offset vectors must have identical keys")
    if len(targets) != payload["table_count"]:
        raise ResultValidationError("ownership checkpoint requires one offset per retiring table")
    for key, target in targets.items():
        current = currents[key]
        if (
            not isinstance(key, str)
            or not key.endswith(":0")
            or not isinstance(target, int)
            or isinstance(target, bool)
            or target < 0
            or not isinstance(current, int)
            or isinstance(current, bool)
            or current < 0
            or (
                payload.get("schema_version") not in (5, 6, 7)
                and current < target
            )
        ):
            raise ResultValidationError(
                "ownership checkpoint offsets must be non-negative partition-0 integers; "
                "consumer offsets must be at or beyond target before schema v5"
            )


def _write_document_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    if path.parent.is_symlink() or path.is_symlink():
        raise ResultValidationError("result path must not contain a symlinked run artifact")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.is_symlink():
        raise ResultValidationError("result path must not contain a symlinked run artifact")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    validate_result(payload)
    _write_document_atomic(path, payload)


def write_ownership_checkpoint_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    validate_ownership_checkpoint(payload)
    _write_document_atomic(path, payload)
