from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from pathlib import Path

from flipbench.playground_results import load_saved_runs, summarize_result
from flipbench.results import write_ownership_checkpoint_atomic
from tests.test_results import successful_result


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


class PlaygroundResultHistoryTests(unittest.TestCase):
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
