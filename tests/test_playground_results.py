from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from pathlib import Path

from flipbench.playground_results import load_saved_runs, summarize_result
from flipbench.results import write_ownership_checkpoint_atomic
from tests.test_results import (
    optimistic_detach_result,
    successful_result,
    v4_isolated_result,
    v6_atomic_detach_marker_result,
    v7_parallel_atomic_detach_marker_result,
)


def ownership_checkpoint(run_id: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "artifact_type": "ownership_grant",
        "non_authoritative": True,
        "run_id": run_id,
        "attempt_id": str(uuid.uuid4()),
        "attempt_epoch": 1,
        "recorded_at_utc": "2026-08-01T10:00:00+00:00",
        "ownership_outcome": "success",
        "verification_outcome": "pending",
        "table_count": 5,
        "profile": "local-rf3-single-host-production-shaped",
        "cell": "cell01",
        "timeslot": "retiring",
        "environment_generation_id": "legacy",
        "stages_ns": {"t13": 12},
        "durations_ns": {"writer_park_ns": 1_000_000},
        "admission": {"source_lag_bytes": 100, "sink_lag_records": 4},
        "target_next_offsets": {f"topic{index}:0": 2 for index in range(5)},
        "final_committed_next_offsets": {f"topic{index}:0": 2 for index in range(5)},
    }


def v2_ownership_checkpoint(run_id: str) -> dict[str, object]:
    payload = ownership_checkpoint(run_id)
    payload.update(
        {
            "schema_version": 2,
            "source_topology": "isolated",
            "source_connector_count": 2,
            "fence_source_lane": "migration",
            "fence_slot_name": "flipbench_slot_migration",
            "fence_publication_name": "flipbench_pub_migration",
            "fence_lsn": "1/0",
            "hot_identity": {
                "cell": "cell01",
                "database": "cards",
                "slot": "flipbench_slot_migration",
                "system_identifier": "123456789",
            },
            "source_lane_evidence": {
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
            },
        }
    )
    return payload


class PlaygroundResultHistoryTests(unittest.TestCase):
    def test_summarizes_variant_e_timing(self) -> None:
        payload = successful_result()
        payload["scenario"] = {"write_fence_mode": "optimistic_detach_v1"}
        payload["durations_ns"] = {
            "admission_fence_ns": 101,
            "in_flight_resolution_ns": 202,
        }
        summary = summarize_result(payload)
        self.assertEqual(summary["write_fence_mode"], "optimistic_detach_v1")
        self.assertEqual(summary["admission_fence_ns"], 101)
        self.assertEqual(summary["in_flight_resolution_ns"], 202)

    def test_labels_old_all_table_variant_e_as_superseded_legacy_shape(self) -> None:
        payload = optimistic_detach_result()
        payload["scenario"].pop("optimistic_contract_version")
        payload["scenario"].pop("transaction_shape")
        payload["scenario"].pop("operations_per_api_batch")
        payload["scenario"].pop("ownership_reads_per_api_batch")
        payload["scenario"].pop("postgres_transactions_per_api_batch")
        payload["scenario"].pop("api_batch_scheduling")
        payload["scenario"].pop("partial_batch_completion_allowed")
        payload["scenario"]["tables_per_api_transaction"] = 5
        summary = summarize_result(payload)
        self.assertEqual(summary["transaction_shape"], "legacy_all_tables_api")

    def test_labels_current_variant_e_batch_admission_shape(self) -> None:
        summary = summarize_result(optimistic_detach_result())
        self.assertEqual(summary["transaction_shape"], "api_batch_separate_commits_v1")
        self.assertEqual(summary["operations_per_api_batch"], 5)
        self.assertEqual(summary["ownership_reads_per_api_batch"], 1)
        self.assertEqual(summary["postgres_transactions_per_api_batch"], 5)

    def test_summarizes_variant_g_atomic_detach_marker_time(self) -> None:
        summary = summarize_result(v6_atomic_detach_marker_result())

        self.assertEqual(summary["source_proof_mode"], "atomic_detach_marker_v1")
        self.assertEqual(summary["atomic_detach_marker_ns"], 15)

    def test_summarizes_variant_h_parallel_detach_wall_time(self) -> None:
        summary = summarize_result(v7_parallel_atomic_detach_marker_result())

        self.assertEqual(
            summary["source_proof_mode"],
            "parallel_atomic_detach_marker_v1",
        )
        self.assertEqual(
            summary["optimistic_admission_check_mode"], "state_only_v1"
        )
        self.assertEqual(summary["ownership_epoch_checks_per_api_batch"], 0)
        self.assertEqual(summary["parallel_detach_wall_ns"], 5)

    def test_labels_previous_per_transaction_gate_e_as_superseded(self) -> None:
        payload = optimistic_detach_result()
        payload["scenario"].pop("optimistic_contract_version")
        payload["scenario"].pop("operations_per_api_batch")
        payload["scenario"].pop("ownership_reads_per_api_batch")
        payload["scenario"].pop("postgres_transactions_per_api_batch")
        payload["scenario"].pop("api_batch_scheduling")
        payload["scenario"].pop("partial_batch_completion_allowed")
        payload["scenario"]["transaction_shape"] = "single_table_api"
        summary = summarize_result(payload)
        self.assertEqual(
            summary["transaction_shape"], "legacy_per_transaction_gate_api"
        )

    def test_labels_standalone_batch_admission_transaction_as_superseded(self) -> None:
        payload = optimistic_detach_result()
        payload["scenario"]["optimistic_contract_version"] = (
            "batch_admission_separate_commits_v1"
        )
        payload["scenario"]["postgres_transactions_per_api_batch"] = 6
        summary = summarize_result(payload)
        self.assertEqual(
            summary["transaction_shape"],
            "legacy_batch_admission_extra_transaction",
        )

    def test_labels_unreserved_batch_scheduler_as_superseded(self) -> None:
        payload = optimistic_detach_result()
        payload["scenario"]["optimistic_contract_version"] = (
            "batch_first_write_admission_v2"
        )
        payload["scenario"].pop("api_batch_scheduling")
        summary = summarize_result(payload)
        self.assertEqual(
            summary["transaction_shape"], "legacy_unreserved_batch_scheduler"
        )

    def test_labels_variant_d_one_table_transaction_shape(self) -> None:
        payload = successful_result()
        payload["scenario"] = {
            "write_fence_mode": "hot_transactional_v1",
            "tables_per_api_transaction": 1,
        }
        summary = summarize_result(payload)
        self.assertEqual(summary["transaction_shape"], "single_table_api")

    def test_summarizes_target_and_achieved_tps(self) -> None:
        payload = successful_result()
        payload["scenario"] = {
            "mode": "production-shaped",
            "workload_mode": "target_rate_v1",
            "workload_settings": {
                "active_target_tps": 13_636,
                "retiring_target_tps": 1_364,
            },
            "stable_window": [
                {"transactions": {"achieved_tps": 14_250.0}}
            ],
        }
        summary = summarize_result(payload)
        self.assertEqual(summary["target_tps"], 15_000)
        self.assertEqual(summary["achieved_tps"], 14_250.0)
        self.assertEqual(summary["workload_mode"], "target_rate_v1")

    def test_schema_v2_checkpoint_enforces_topology_provenance(self) -> None:
        run_id = str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / run_id / "ownership-grant.json"
            write_ownership_checkpoint_atomic(path, v2_ownership_checkpoint(run_id))

        invalid = v2_ownership_checkpoint(run_id)
        invalid["fence_source_lane"] = "shared"
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(
            ValueError, "fence lane"
        ):
            write_ownership_checkpoint_atomic(
                Path(directory) / run_id / "ownership-grant.json", invalid
            )

        incomplete_identity = v2_ownership_checkpoint(run_id)
        incomplete_identity["hot_identity"] = None
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(
            ValueError, "hot identity"
        ):
            write_ownership_checkpoint_atomic(
                Path(directory) / run_id / "ownership-grant.json", incomplete_identity
            )

    def test_checkpoint_writer_is_atomic_and_requires_non_authoritative_t13(self) -> None:
        run_id = str(uuid.uuid4())
        attempt_id = str(uuid.uuid4())
        payload = {
            "schema_version": 1,
            "artifact_type": "ownership_grant",
            "non_authoritative": True,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "attempt_epoch": 1,
            "ownership_outcome": "success",
            "verification_outcome": "pending",
            "table_count": 5,
            "cell": "cell01",
            "timeslot": "retiring",
            "environment_generation_id": "legacy",
            "stages_ns": {"t13": 12},
            "durations_ns": {"writer_park_ns": 10},
            "target_next_offsets": {f"topic{index}:0": 2 for index in range(5)},
            "final_committed_next_offsets": {f"topic{index}:0": 2 for index in range(5)},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / run_id / "ownership-grant.json"
            write_ownership_checkpoint_atomic(path, payload)
            self.assertEqual(json.loads(path.read_text())["non_authoritative"], True)
            self.assertEqual(list(path.parent.glob(".ownership-grant.json.*")), [])

    def test_prefers_completed_result_over_grant_checkpoint(self) -> None:
        run_id = str(uuid.uuid4())
        checkpoint = ownership_checkpoint(run_id)
        completed = successful_result()
        completed.update({
            "run_id": run_id,
            "artifact_type": "completed_run",
            "finished_at_utc": "2026-08-01T10:00:01+00:00",
            "durations_ns": {
                "tracker_lock_ns": 100_000,
                "source_proof_ns": 200_000,
                "capture_e_ns": 300_000,
                "sink_proof_ns": 400_000,
                "grant_ns": 500_000,
                "writer_park_ns": 2_000_000,
                "whole_lifecycle_ns": 3_000_000,
            },
        })
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / run_id
            run_dir.mkdir()
            (run_dir / "ownership-grant.json").write_text(json.dumps(checkpoint))
            (run_dir / "run.json").write_text(json.dumps(completed))
            summaries = load_saved_runs(Path(directory))
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["artifact_type"], "completed_run")
        self.assertEqual(summaries[0]["verification_outcome"], "passed")
        self.assertEqual(summaries[0]["writer_park_ns"], 2_000_000)
        self.assertEqual(summaries[0]["tracker_lock_ns"], 100_000)
        self.assertEqual(summaries[0]["source_proof_ns"], 200_000)
        self.assertEqual(summaries[0]["capture_e_ns"], 300_000)
        self.assertEqual(summaries[0]["sink_proof_ns"], 400_000)
        self.assertEqual(summaries[0]["grant_ns"], 500_000)

    def test_falls_back_to_valid_checkpoint_when_completed_file_is_invalid(self) -> None:
        run_id = str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / run_id
            run_dir.mkdir()
            (run_dir / "run.json").write_text("{}")
            (run_dir / "ownership-grant.json").write_text(json.dumps(ownership_checkpoint(run_id)))
            summaries = load_saved_runs(Path(directory))
        self.assertEqual(summaries[0]["artifact_type"], "ownership_grant")

    def test_summarizes_pending_grant_without_claiming_verification(self) -> None:
        run_id = str(uuid.uuid4())
        summary = summarize_result(
            {
                "artifact_type": "ownership_grant",
                "run_id": run_id,
                "recorded_at_utc": "2026-08-01T10:00:00+00:00",
                "ownership_outcome": "success",
                "verification_outcome": "pending",
                "table_count": 10,
                "profile": "local-rf3-single-host-production-shaped",
                "durations_ns": {"writer_park_ns": 7},
                "admission": {"source_lag_bytes": 9, "sink_lag_records": 11},
            }
        )
        self.assertEqual(summary["run_id"], run_id)
        self.assertEqual(summary["verification_outcome"], "pending")
        self.assertEqual(summary["source_lag_bytes"], 9)
        self.assertEqual(summary["source_topology"], "legacy/unknown")

    def test_preserves_revert_timing_semantics(self) -> None:
        payload = successful_result()
        payload.update(
            {
                "outcome": "reverted",
                "verification_outcome": "not_run",
                "error": {"type": "TimeoutError"},
                "durations_ns": {
                    "forward_until_failure_ns": 3_800_000_000,
                    "revert_ns": 6_000_000,
                    "writer_park_ns": 3_806_000_000,
                },
            }
        )
        summary = summarize_result(payload)
        self.assertEqual(summary["forward_until_failure_ns"], 3_800_000_000)
        self.assertEqual(summary["revert_ns"], 6_000_000)

    def test_summary_preserves_fence_wakeup_experiment_metadata(self) -> None:
        payload = successful_result()
        payload["fence_wakeup"] = {
            "mode": "immediate_heartbeat",
            "applied": True,
            "duration_ns": 2_000_000,
        }
        payload["durations_ns"] = {
            "fence_wakeup_ns": 3_000_000,
            "slot_wait_after_wakeup_ns": 7_000_000,
        }
        summary = summarize_result(payload)
        self.assertEqual(summary["fence_wakeup_mode"], "immediate_heartbeat")
        self.assertIs(summary["fence_wakeup_applied"], True)
        self.assertEqual(summary["fence_wakeup_ns"], 3_000_000)
        self.assertEqual(summary["slot_wait_after_wakeup_ns"], 7_000_000)

        legacy = summarize_result(successful_result())
        self.assertEqual(legacy["fence_wakeup_mode"], "legacy/unknown")
        self.assertIsNone(legacy["fence_wakeup_applied"])

        v4_summary = summarize_result(v4_isolated_result())
        self.assertEqual(v4_summary["source_topology"], "isolated")

    def test_ignores_malformed_or_oversized_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / "bad"
            bad.mkdir()
            (bad / "run.json").write_text("not-json")
            huge = Path(directory) / "huge"
            huge.mkdir()
            (huge / "run.json").write_bytes(b"x" * 1_000_001)
            self.assertEqual(load_saved_runs(Path(directory), max_file_bytes=1_000_000), [])

    def test_rejects_invalid_summary_and_history_limits(self) -> None:
        with self.assertRaises(ValueError):
            summarize_result({"run_id": "bad"})
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                load_saved_runs(Path(directory), limit=0)

    def test_checkpoint_writer_rejects_symlinked_run_directory(self) -> None:
        run_id = str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            linked = Path(directory) / run_id
            linked.symlink_to(Path(outside), target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "symlinked"):
                write_ownership_checkpoint_atomic(
                    linked / "ownership-grant.json", ownership_checkpoint(run_id)
                )

    def test_checkpoint_rejects_impossible_stage_and_offset_evidence(self) -> None:
        run_id = str(uuid.uuid4())
        invalid_cases = []
        invalid_schema = ownership_checkpoint(run_id)
        invalid_schema["schema_version"] = 999
        invalid_cases.append(invalid_schema)
        invalid_epoch = ownership_checkpoint(run_id)
        invalid_epoch["attempt_epoch"] = -1
        invalid_cases.append(invalid_epoch)
        invalid_stage = ownership_checkpoint(run_id)
        invalid_stage["stages_ns"] = {"t13": -1}
        invalid_cases.append(invalid_stage)
        behind = ownership_checkpoint(run_id)
        behind["final_committed_next_offsets"] = {
            **behind["final_committed_next_offsets"],
            "topic0:0": 1,
        }
        invalid_cases.append(behind)
        for payload in invalid_cases:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    write_ownership_checkpoint_atomic(
                        Path(directory) / run_id / "ownership-grant.json", payload
                    )

    def test_history_skips_symlinks_and_unknown_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outside = root / "outside.json"
            outside.write_text("{}")
            linked_dir = root / "linked"
            linked_dir.symlink_to(root, target_is_directory=True)
            run_id = str(uuid.uuid4())
            run_dir = root / run_id
            run_dir.mkdir()
            (run_dir / "run.json").write_text(
                json.dumps(
                    {
                        "artifact_type": "unknown",
                        "run_id": run_id,
                        "table_count": 5,
                    }
                )
            )
            self.assertEqual(load_saved_runs(root), [])


if __name__ == "__main__":
    unittest.main()
