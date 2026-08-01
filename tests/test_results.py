import json
import tempfile
import unittest
import uuid
from pathlib import Path

from flipbench.results import ResultValidationError, validate_result, write_json_atomic


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


class ResultTests(unittest.TestCase):
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
