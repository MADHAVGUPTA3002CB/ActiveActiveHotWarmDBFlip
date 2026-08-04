import json
import tempfile
import unittest
import uuid
from pathlib import Path

from flipbench.results import (
    ResultValidationError,
    validate_ownership_checkpoint,
    validate_result,
    write_json_atomic,
)


def successful_result() -> dict[str, object]:
    ordered = ["t0", "t1", "t2"]
    for index in range(1, 6):
        ordered.extend((f"t3_{index}", f"t4_{index}"))
    ordered.extend(("t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "tverify"))
    stages = {name: index for index, name in enumerate(ordered)}
    return {
        "schema_version": 1,
        "run_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "outcome": "success",
        "attempt_epoch": 1,
        "profile": "local-rf1-running",
        "table_count": 5,
        "stages_ns": stages,
        "detach_ns_by_table": {f"t{index}": index for index in range(5)},
        "target_next_offsets": {f"topic{index}:0": index for index in range(5)},
        "final_committed_next_offsets": {f"topic{index}:0": index for index in range(5)},
        "hot_identity": {"cell": "cell01"},
        "fence_lsn": "1/0",
        "ownership_outcome": "success",
        "verification_outcome": "passed",
        "gc_eligible": True,
        "error": None,
    }


def healthy_result() -> dict[str, object]:
    payload = successful_result()
    payload["profile"] = "local-rf1-healthy-overload"
    payload["stages_ns"]["t2q"] = payload["stages_ns"]["t2"]
    payload["scenario"] = {
        "mode": "healthy-overload",
        "min_source_lag_bytes": 100,
        "min_source_lag_records_per_partition": 10,
        "min_sink_lag_records_per_partition": 10,
        "t1_min_sink_lag_records_per_partition": 1,
        "required_stable_samples": 3,
        "max_admitted_rows_per_partition": 100,
        "stable_window": [
            {
                "source_lag_records_by_partition": {f"topic{index}:0": 10 for index in range(5)},
                "sink_lag_records_by_partition": {f"topic{index}:0": 10 for index in range(5)},
            }
            for _ in range(3)
        ],
    }
    payload["admission"] = {
        "source_lag_bytes": 100,
        "source_lag_records_by_partition": {f"topic{index}:0": 10 for index in range(5)},
        "sink_lag_by_partition": {f"topic{index}:0": 10 for index in range(5)},
        "connector_state": "RUNNING",
        "writer_active_at_t1": True,
        "writer_inserted_at_t1": 500,
    }
    payload["workload_inserted_total"] = 500
    return payload


def production_result() -> dict[str, object]:
    payload = successful_result()
    payload["profile"] = "local-rf1-production-shaped"
    payload["stages_ns"]["t2q"] = payload["stages_ns"]["t2"]
    payload["scenario"] = {
        "mode": "production-shaped",
        "max_source_lag_bytes": 100,
        "max_sink_lag_records_per_partition": 2,
    }
    payload["admission"] = {
        "source_lag_bytes": 100,
        "sink_lag_by_partition": {f"topic{index}:0": 2 for index in range(5)},
        "connector_state": "RUNNING",
    }
    payload["workload_by_timeslot"] = {
        "t1": {"active": 100, "retiring": 10},
        "t2q": {"active": 120, "retiring": 12},
        "t13": {"active": 150, "retiring": 12, "writer_alive": True},
    }
    return payload


def optimistic_detach_result() -> dict[str, object]:
    payload = production_result()
    ordered = ["t0", "t1", "t2", "t2h", "t2w", "t2f"]
    for index in range(1, 6):
        ordered.extend((f"t3_{index}", f"t4_{index}"))
    ordered.extend(
        ("t2q", "t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "tverify")
    )
    payload["stages_ns"] = {name: index for index, name in enumerate(ordered)}
    payload["scenario"].update(
        {
            "write_fence_mode": "optimistic_detach_v1",
            "retiring_write_gate_epoch": 1,
            "optimistic_contract_version": "reserved_batch_first_write_admission_v3",
            "api_batch_scheduling": "single_worker_reserved_v1",
            "transaction_shape": "api_batch_separate_commits_v1",
            "tables_per_api_transaction": 1,
            "operations_per_api_batch": 5,
            "ownership_reads_per_api_batch": 1,
            "postgres_transactions_per_api_batch": 5,
            "partial_batch_completion_allowed": True,
        }
    )
    payload["write_fence"] = {
        "mode": "optimistic_detach_v1",
        "state_before": "open",
        "state_after": "parked",
        "park_attempt_id": payload["attempt_id"],
        "ownership_epoch": 1,
        "attempt_epoch": 1,
        "verified_before_grant": True,
        "active_gate_state": "open",
        "reopened": False,
    }
    return payload


def v2_isolated_result() -> dict[str, object]:
    payload = successful_result()
    payload["schema_version"] = 2
    payload["cell"] = "cell01"
    payload["topology"] = {
        "source_topology": "isolated",
        "source_connector_count": 2,
        "fence_source_lane": "migration",
        "fence_slot_name": "flipbench_slot_migration",
        "fence_publication_name": "flipbench_pub_migration",
        "source_topic_prefixes": {
            "active": "cards.cell01.active",
            "migration": "cards.cell01.migration",
        },
    }
    payload["fence_lsn"] = "1/0"
    payload["hot_identity"] = {
        "cell": "cell01",
        "database": "cards",
        "slot": "flipbench_slot_migration",
        "system_identifier": "123456789",
    }
    payload["source_lane_evidence"] = {
        stage: {
            lane: {
                "connector_state": "RUNNING",
                "connector_name": f"flipbench-source-{lane}",
                "task_states": ["RUNNING"],
                "slot_name": f"flipbench_slot_{lane}",
                "slot_active": True,
                "confirmed_lsn": "1/0",
                "restart_lsn": "0/10",
                "lag_bytes": 0,
            }
            for lane in ("active", "migration")
        }
        for stage in ("t1", "t5", "t7", "t13")
    }
    return payload


def v3_isolated_result() -> dict[str, object]:
    payload = v2_isolated_result()
    payload["schema_version"] = 3
    for snapshot in payload["source_lane_evidence"].values():
        for lane, lane_evidence in snapshot.items():
            lane_evidence["publication_name"] = f"flipbench_pub_{lane}"
            lane_evidence["topic_prefix"] = f"cards.cell01.{lane}"
    return payload


def v4_isolated_result(mode: str = "immediate_heartbeat") -> dict[str, object]:
    payload = v3_isolated_result()
    payload["schema_version"] = 4
    payload["stages_ns"]["t6w"] = payload["stages_ns"]["t6"]
    payload["scenario"] = {
        "fence_wakeup_mode": mode,
    }
    payload["fence_wakeup"] = {
        "mode": mode,
        "lane": "migration",
        "heartbeat_table": "dbz_heartbeat_migration",
        "attempted": mode == "immediate_heartbeat",
        "applied": mode == "immediate_heartbeat",
        "rows_updated": 1 if mode == "immediate_heartbeat" else 0,
        "post_update_wal_lsn": "2/0" if mode == "immediate_heartbeat" else None,
        "duration_ns": 0,
        "confirmed_flush_lsn_at_t7": "2/0",
    }
    for stage in ("t7", "t13"):
        payload["source_lane_evidence"][stage]["migration"]["confirmed_lsn"] = "2/0"
    return payload


def v5_marker_result() -> dict[str, object]:
    payload = v4_isolated_result("passive")
    payload["schema_version"] = 5
    payload["scenario"]["source_proof_mode"] = "per_leaf_marker_v1"
    payload["fence_wakeup"]["confirmed_flush_lsn_at_t7"] = None
    targets = {f"topic{index}:0": 11 + index for index in range(5)}
    payload["target_next_offsets"] = targets
    payload["final_committed_next_offsets"] = {
        key: 0 for key in targets
    }
    payload["marker_fence"] = {
        "mode": "per_leaf_marker_v1",
        "marker_schema_version": 1,
        "exactly_once_source": True,
        "consumer_isolation": "read_committed",
        "marker_ids": {key: str(uuid.uuid4()) for key in targets},
        "scan_start_offsets": {key: value - 1 for key, value in targets.items()},
        "marker_next_offsets": dict(targets),
        "emission_duration_ns": 1,
        "warm_receipts_complete": True,
    }
    return payload


def v5_marker_checkpoint() -> dict[str, object]:
    result = v5_marker_result()
    return {
        **result,
        **result["topology"],
        "artifact_type": "ownership_grant",
        "non_authoritative": True,
        "ownership_outcome": "success",
        "verification_outcome": "pending",
        "cell": "cell01",
        "timeslot": "retiring",
        "environment_generation_id": "legacy",
        "durations_ns": {"writer_park_ns": 1},
    }


def v6_atomic_detach_marker_result() -> dict[str, object]:
    payload = v5_marker_result()
    payload["schema_version"] = 6
    payload["scenario"]["source_proof_mode"] = "atomic_detach_marker_v1"
    payload["marker_fence"].update(
        {
            "mode": "atomic_detach_marker_v1",
            "detach_marker_contract": "per_leaf_single_transaction_v1",
            "detach_before_marker": True,
            "atomic_transaction_ns_by_leaf": {
                f"bench_table_{index:02d}_p_retiring": index
                for index in range(1, 6)
            },
        }
    )
    return payload


def v7_parallel_atomic_detach_marker_result() -> dict[str, object]:
    payload = v6_atomic_detach_marker_result()
    payload["schema_version"] = 7
    payload["scenario"]["source_proof_mode"] = (
        "parallel_atomic_detach_marker_v1"
    )
    payload["marker_fence"].update(
        {
            "mode": "parallel_atomic_detach_marker_v1",
            "detach_marker_contract": "per_leaf_parallel_transactions_v1",
            "detach_execution_mode": "all_parallel_v1",
            "detach_parallelism": 5,
            "parallel_wall_duration_ns": 5,
        }
    )
    stages = payload["stages_ns"]
    before_parallel = min(stages[f"t3_{index}"] for index in range(1, 6))
    for index in range(1, 6):
        stages[f"t3_{index}"] = before_parallel + index - 1
        stages[f"t4_{index}"] = before_parallel + 6 + index - 1
    stages["t3_parallel"] = before_parallel + 5
    stages["t4_parallel"] = before_parallel + 11
    shift = stages["t4_parallel"] - stages["t5"] + 1
    for stage in ("t5", "t6", "t6w", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "tverify"):
        stages[stage] += shift
    return payload


def v2_shared_result(*, paused: bool = False) -> dict[str, object]:
    payload = successful_result()
    payload["schema_version"] = 2
    payload["cell"] = "cell01"
    payload["profile"] = "local-rf1-paused-backlog" if paused else "local-rf1-running"
    payload["topology"] = {
        "source_topology": "shared",
        "source_connector_count": 1,
        "fence_source_lane": "shared",
        "fence_slot_name": "flipbench_slot",
        "fence_publication_name": "flipbench_pub",
    }
    payload["fence_lsn"] = "1/0"
    payload["hot_identity"] = {
        "cell": "cell01",
        "database": "cards",
        "slot": "flipbench_slot",
        "system_identifier": "123456789",
    }
    payload["source_lane_evidence"] = {
        stage: {
            "shared": {
                "connector_state": "PAUSED" if paused and stage == "t1" else "RUNNING",
                "connector_name": "flipbench-source",
                "task_states": ["PAUSED" if paused and stage == "t1" else "RUNNING"],
                "slot_name": "flipbench_slot",
                "slot_active": False if paused and stage == "t1" else True,
                "confirmed_lsn": "1/0",
                "restart_lsn": "0/10",
                "lag_bytes": 0,
            }
        }
        for stage in ("t1", "t5", "t7", "t13")
    }
    return payload


class ResultTests(unittest.TestCase):
    def test_schema_v5_accepts_receipt_proof_without_committed_offset_or_slot_flush_gate(self) -> None:
        validate_result(v5_marker_result())

        missing_receipt = v5_marker_result()
        missing_receipt["marker_fence"]["warm_receipts_complete"] = False
        with self.assertRaisesRegex(ResultValidationError, "warm marker receipt"):
            validate_result(missing_receipt)

        wrong_target = v5_marker_result()
        wrong_target["marker_fence"]["marker_next_offsets"]["topic0:0"] += 1
        with self.assertRaisesRegex(ResultValidationError, "target"):
            validate_result(wrong_target)

    def test_schema_v6_requires_atomic_detach_marker_evidence(self) -> None:
        validate_result(v6_atomic_detach_marker_result())

        reverted_before_marker = v6_atomic_detach_marker_result()
        reverted_before_marker.update(
            {
                "outcome": "reverted",
                "ownership_outcome": "reverted",
                "verification_outcome": "not_run",
                "gc_eligible": False,
                "marker_fence": None,
                "error": {"type": "TimeoutError", "message": "detach timed out"},
            }
        )
        validate_result(reverted_before_marker)

        missing_contract = v6_atomic_detach_marker_result()
        del missing_contract["marker_fence"]["detach_marker_contract"]
        with self.assertRaisesRegex(ResultValidationError, "fields"):
            validate_result(missing_contract)

        wrong_order = v6_atomic_detach_marker_result()
        wrong_order["marker_fence"]["detach_before_marker"] = False
        with self.assertRaisesRegex(ResultValidationError, "detach-before-marker"):
            validate_result(wrong_order)

    def test_schema_v7_requires_all_parallel_atomic_detach_evidence(self) -> None:
        validate_result(v7_parallel_atomic_detach_marker_result())

        wrong_parallelism = v7_parallel_atomic_detach_marker_result()
        wrong_parallelism["marker_fence"]["detach_parallelism"] = 4
        with self.assertRaisesRegex(ResultValidationError, "parallelism"):
            validate_result(wrong_parallelism)

        missing_wall_time = v7_parallel_atomic_detach_marker_result()
        del missing_wall_time["marker_fence"]["parallel_wall_duration_ns"]
        with self.assertRaisesRegex(ResultValidationError, "fields"):
            validate_result(missing_wall_time)

        missing_parallel_stage = v7_parallel_atomic_detach_marker_result()
        del missing_parallel_stage["stages_ns"]["t4_parallel"]
        with self.assertRaisesRegex(ResultValidationError, "missing stages"):
            validate_result(missing_parallel_stage)

    def test_schema_v5_checkpoint_treats_consumer_offsets_as_audit_only(self) -> None:
        validate_ownership_checkpoint(v5_marker_checkpoint())

    def test_variant_e_requires_single_table_transaction_evidence(self) -> None:
        validate_result(optimistic_detach_result())

        multi_table = optimistic_detach_result()
        multi_table["scenario"]["tables_per_api_transaction"] = 5
        with self.assertRaisesRegex(ResultValidationError, "one selected table"):
            validate_result(multi_table)

        repeated_reads = optimistic_detach_result()
        repeated_reads["scenario"]["ownership_reads_per_api_batch"] = 5
        with self.assertRaisesRegex(ResultValidationError, "one ownership read"):
            validate_result(repeated_reads)

        atomic_batch = optimistic_detach_result()
        atomic_batch["scenario"]["partial_batch_completion_allowed"] = False
        with self.assertRaisesRegex(ResultValidationError, "partial completion"):
            validate_result(atomic_batch)

        extra_admission_transaction = optimistic_detach_result()
        extra_admission_transaction["scenario"]["postgres_transactions_per_api_batch"] = 6
        with self.assertRaisesRegex(ResultValidationError, "one PostgreSQL transaction per operation"):
            validate_result(extra_admission_transaction)

        unreserved = optimistic_detach_result()
        unreserved["scenario"]["api_batch_scheduling"] = "unreserved"
        with self.assertRaisesRegex(ResultValidationError, "reserved to one worker"):
            validate_result(unreserved)

        legacy_all_tables = optimistic_detach_result()
        legacy_all_tables["scenario"].pop("optimistic_contract_version")
        legacy_all_tables["scenario"].pop("transaction_shape")
        legacy_all_tables["scenario"].pop("operations_per_api_batch")
        legacy_all_tables["scenario"].pop("ownership_reads_per_api_batch")
        legacy_all_tables["scenario"].pop("postgres_transactions_per_api_batch")
        legacy_all_tables["scenario"].pop("api_batch_scheduling")
        legacy_all_tables["scenario"].pop("partial_batch_completion_allowed")
        legacy_all_tables["scenario"]["tables_per_api_transaction"] = 5
        validate_result(legacy_all_tables)

        missing_admission_fence = optimistic_detach_result()
        del missing_admission_fence["stages_ns"]["t2f"]
        with self.assertRaisesRegex(ResultValidationError, "t2f"):
            validate_result(missing_admission_fence)

    def test_schema_v4_binds_fence_wakeup_to_fence_lane_and_proof(self) -> None:
        validate_result(v4_isolated_result())
        validate_result(v4_isolated_result("passive"))

        wrong_lane = v4_isolated_result()
        wrong_lane["fence_wakeup"]["lane"] = "active"
        with self.assertRaisesRegex(ResultValidationError, "wakeup lane"):
            validate_result(wrong_lane)

        missing_update = v4_isolated_result()
        missing_update["fence_wakeup"]["applied"] = False
        with self.assertRaisesRegex(ResultValidationError, "not-applied"):
            validate_result(missing_update)

        before_fence = v4_isolated_result()
        before_fence["fence_wakeup"]["post_update_wal_lsn"] = "0/FF"
        with self.assertRaisesRegex(ResultValidationError, "WAL position"):
            validate_result(before_fence)

        non_boolean_attempt = v4_isolated_result()
        non_boolean_attempt["fence_wakeup"]["attempted"] = 1
        with self.assertRaisesRegex(ResultValidationError, "attempted and applied"):
            validate_result(non_boolean_attempt)

        t7_before_fence = v4_isolated_result()
        t7_before_fence["fence_wakeup"]["confirmed_flush_lsn_at_t7"] = "0/FF"
        with self.assertRaisesRegex(ResultValidationError, "confirmed WAL position"):
            validate_result(t7_before_fence)

        extra_field = v4_isolated_result()
        extra_field["fence_wakeup"]["unexpected"] = True
        with self.assertRaisesRegex(ResultValidationError, "exact fields"):
            validate_result(extra_field)

        equal_to_fence = v4_isolated_result()
        equal_to_fence["fence_wakeup"]["post_update_wal_lsn"] = "1/0"
        with self.assertRaisesRegex(ResultValidationError, "after the fence"):
            validate_result(equal_to_fence)

        lane_t7_behind = v4_isolated_result()
        lane_t7_behind["source_lane_evidence"]["t7"]["migration"][
            "confirmed_lsn"
        ] = "1/0"
        with self.assertRaisesRegex(ResultValidationError, "t7 source evidence"):
            validate_result(lane_t7_behind)

        failed_missing_field = v4_isolated_result()
        failed_missing_field["outcome"] = "failed"
        failed_missing_field["ownership_outcome"] = "failed"
        failed_missing_field["verification_outcome"] = "not_run"
        failed_missing_field["gc_eligible"] = False
        failed_missing_field["error"] = {"type": "TimeoutError", "message": "slot timeout"}
        del failed_missing_field["fence_wakeup"]["confirmed_flush_lsn_at_t7"]
        with self.assertRaisesRegex(ResultValidationError, "exact fields"):
            validate_result(failed_missing_field)

        def failed_wakeup_result() -> dict[str, object]:
            failed = v4_isolated_result()
            failed["outcome"] = "failed"
            failed["ownership_outcome"] = "failed"
            failed["verification_outcome"] = "not_run"
            failed["gc_eligible"] = False
            failed["error"] = {"type": "TimeoutError", "message": "slot timeout"}
            return failed

        committed_without_observed_lsn = failed_wakeup_result()
        committed_without_observed_lsn["fence_wakeup"]["post_update_wal_lsn"] = None
        committed_without_observed_lsn["fence_wakeup"]["confirmed_flush_lsn_at_t7"] = None
        validate_result(committed_without_observed_lsn)

        contradictory_not_applied = failed_wakeup_result()
        contradictory_not_applied["fence_wakeup"].update(
            {"applied": False, "rows_updated": 1, "post_update_wal_lsn": "2/0"}
        )
        with self.assertRaisesRegex(ResultValidationError, "not-applied"):
            validate_result(contradictory_not_applied)

        malformed_failed_lsn = failed_wakeup_result()
        malformed_failed_lsn["fence_wakeup"]["post_update_wal_lsn"] = "bad"
        with self.assertRaisesRegex(ResultValidationError, "post-update WAL position"):
            validate_result(malformed_failed_lsn)

        pre_fence_failed_lsn = failed_wakeup_result()
        pre_fence_failed_lsn["fence_wakeup"]["post_update_wal_lsn"] = "0/FF"
        with self.assertRaisesRegex(ResultValidationError, "after the fence"):
            validate_result(pre_fence_failed_lsn)

        confirmed_without_attempt = failed_wakeup_result()
        confirmed_without_attempt["fence_wakeup"].update(
            {
                "attempted": False,
                "applied": False,
                "rows_updated": 0,
                "post_update_wal_lsn": None,
            }
        )
        with self.assertRaisesRegex(ResultValidationError, "confirmation requires"):
            validate_result(confirmed_without_attempt)

        confirmed_without_observed_lsn = failed_wakeup_result()
        confirmed_without_observed_lsn["fence_wakeup"]["post_update_wal_lsn"] = None
        with self.assertRaisesRegex(ResultValidationError, "confirmation requires"):
            validate_result(confirmed_without_observed_lsn)
    def test_schema_v2_requires_consistent_topology_and_lane_evidence(self) -> None:
        validate_result(v2_isolated_result())
        validate_result(v2_shared_result())
        validate_result(v2_shared_result(paused=True))

        missing_topology = v2_isolated_result()
        del missing_topology["topology"]
        with self.assertRaisesRegex(ResultValidationError, "topology provenance"):
            validate_result(missing_topology)

        contradictory_count = v2_isolated_result()
        contradictory_count["topology"]["source_connector_count"] = 1
        with self.assertRaisesRegex(ResultValidationError, "connector count"):
            validate_result(contradictory_count)

        inactive_lane = v2_isolated_result()
        inactive_lane["source_lane_evidence"]["t13"]["active"]["slot_active"] = False
        with self.assertRaisesRegex(ResultValidationError, "was not active"):
            validate_result(inactive_lane)

        contradictory_slot = v2_isolated_result()
        contradictory_slot["source_lane_evidence"]["t13"]["migration"]["slot_name"] = "other"
        with self.assertRaisesRegex(ResultValidationError, "identity changed"):
            validate_result(contradictory_slot)

        fence_not_reached = v2_isolated_result()
        fence_not_reached["source_lane_evidence"]["t7"]["migration"]["confirmed_lsn"] = "0/FF"
        with self.assertRaisesRegex(ResultValidationError, "had not confirmed"):
            validate_result(fence_not_reached)

        contradictory_hot_identity = v2_isolated_result()
        contradictory_hot_identity["hot_identity"]["slot"] = "flipbench_slot_active"
        with self.assertRaisesRegex(ResultValidationError, "hot identity"):
            validate_result(contradictory_hot_identity)

    def test_schema_v3_binds_publication_and_topic_prefix_to_lane_evidence(self) -> None:
        validate_result(v3_isolated_result())

        contradictory_publication = v3_isolated_result()
        contradictory_publication["source_lane_evidence"]["t13"]["migration"][
            "publication_name"
        ] = "other"
        with self.assertRaisesRegex(ResultValidationError, "identity changed"):
            validate_result(contradictory_publication)

        contradictory_fence = v3_isolated_result()
        contradictory_fence["topology"]["fence_publication_name"] = "other"
        with self.assertRaisesRegex(ResultValidationError, "fence publication"):
            validate_result(contradictory_fence)

        contradictory_prefix = v3_isolated_result()
        for snapshot in contradictory_prefix["source_lane_evidence"].values():
            snapshot["migration"]["topic_prefix"] = "cards.cell01.other"
        with self.assertRaisesRegex(ResultValidationError, "topic prefixes"):
            validate_result(contradictory_prefix)

    def test_schema_v2_requires_complete_hot_identity_for_results(self) -> None:
        incomplete = v2_isolated_result()
        incomplete["hot_identity"] = {"slot": "flipbench_slot_migration"}
        with self.assertRaisesRegex(ResultValidationError, "hot identity"):
            validate_result(incomplete)
    def test_validates_and_atomically_writes_success(self) -> None:
        payload = successful_result()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            write_json_atomic(path, payload)
            self.assertEqual(json.loads(path.read_text())["outcome"], "success")

    def test_rejects_incomplete_success(self) -> None:
        payload = successful_result()
        payload["target_next_offsets"] = {}
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

    def test_failed_result_requires_error(self) -> None:
        payload = successful_result()
        payload["outcome"] = "failed"
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

    def test_success_requires_ordered_stages_and_component_wise_offsets(self) -> None:
        payload = successful_result()
        payload["stages_ns"]["t5"] = -1
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

    def test_success_requires_post_grant_verification_and_honest_gc_gate(self) -> None:
        payload = successful_result()
        validate_result(payload)

        payload["stages_ns"]["tverify"] = payload["stages_ns"]["t13"] - 1
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

    def test_production_shaped_result_requires_active_continuity_and_retiring_quiescence(self) -> None:
        payload = production_result()
        validate_result(payload)

        missing_quiescence = production_result()
        del missing_quiescence["stages_ns"]["t2q"]
        with self.assertRaises(ResultValidationError):
            validate_result(missing_quiescence)

        no_active_progress = production_result()
        no_active_progress["workload_by_timeslot"]["t13"]["active"] = 120
        with self.assertRaises(ResultValidationError):
            validate_result(no_active_progress)

        crossed_lag_ceiling = production_result()
        crossed_lag_ceiling["admission"]["source_lag_bytes"] = 101
        with self.assertRaises(ResultValidationError):
            validate_result(crossed_lag_ceiling)

        payload["workload_by_timeslot"]["t13"]["retiring"] = 13
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

        payload = production_result()
        payload["workload_by_timeslot"]["t13"]["writer_alive"] = False
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

        payload = production_result()
        payload["verification_outcome"] = "failed"
        payload["gc_eligible"] = False
        validate_result(payload)

        payload = successful_result()
        payload["verification_outcome"] = "failed"
        payload["gc_eligible"] = True
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

    def test_healthy_overload_requires_stable_per_partition_lag_and_t2q(self) -> None:
        payload = healthy_result()
        validate_result(payload)

        payload["admission"]["sink_lag_by_partition"]["topic4:0"] = 0
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

    def test_healthy_overload_rejects_weak_source_window_and_overshoot(self) -> None:
        payload = healthy_result()
        payload["admission"]["source_lag_records_by_partition"]["topic4:0"] = 9
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

        payload = healthy_result()
        payload["scenario"]["stable_window"][0]["sink_lag_records_by_partition"]["topic4:0"] = 9
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

        payload = healthy_result()
        payload["admission"]["writer_inserted_at_t1"] = 501
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

        payload = successful_result()
        payload["final_committed_next_offsets"]["topic4:0"] = -1
        with self.assertRaises(ResultValidationError):
            validate_result(payload)

        payload = successful_result()
        payload["final_committed_next_offsets"]["topic4:0"] = 3
        payload["target_next_offsets"]["topic4:0"] = 4
        with self.assertRaises(ResultValidationError):
            validate_result(payload)


if __name__ == "__main__":
    unittest.main()
