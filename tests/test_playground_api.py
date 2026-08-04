from __future__ import annotations

import unittest
import sys
from types import ModuleType, SimpleNamespace
from threading import Lock
from unittest.mock import Mock, patch


def _load_runtime_class():
    psycopg = ModuleType("psycopg")
    psycopg.Connection = object
    psycopg.connect = Mock()
    psycopg.sql = SimpleNamespace()
    psycopg_json = ModuleType("psycopg.types.json")
    psycopg_json.Jsonb = lambda value: value
    confluent = ModuleType("confluent_kafka")
    confluent.Consumer = object
    confluent.TopicPartition = object
    confluent_admin = ModuleType("confluent_kafka.admin")
    confluent_admin.AdminClient = object
    confluent_admin.ConfigResource = object
    confluent_admin.NewTopic = object
    modules = {
        "psycopg": psycopg,
        "psycopg.types": ModuleType("psycopg.types"),
        "psycopg.types.json": psycopg_json,
        "confluent_kafka": confluent,
        "confluent_kafka.admin": confluent_admin,
    }
    with patch.dict(sys.modules, modules):
        from flipbench.playground_api import PlaygroundRuntime

    return PlaygroundRuntime


PlaygroundRuntime = _load_runtime_class()


class PlaygroundMaintenanceTests(unittest.TestCase):
    def runtime(self) -> PlaygroundRuntime:
        runtime = PlaygroundRuntime.__new__(PlaygroundRuntime)
        runtime._operation_lock = Lock()
        runtime._maintenance = False
        runtime.workload = Mock()
        runtime._flip_thread = None
        runtime._tracker_snapshot = Mock(return_value=({"retiring": "hot_primary"}, {}))
        return runtime

    def test_prepare_reset_atomically_blocks_new_operations_and_stops_writes(self) -> None:
        runtime = self.runtime()
        runtime.snapshot = Mock(return_value={"flip": {"status": "idle"}})
        state = runtime.prepare_reset()
        self.assertEqual(state["flip"]["status"], "idle")
        runtime.workload.stop_all.assert_called_once_with()
        with self.assertRaisesRegex(RuntimeError, "maintenance"):
            runtime.start_workload()

    def test_prepare_reset_rejects_live_flip_and_cancel_reopens_operations(self) -> None:
        runtime = self.runtime()
        runtime._flip_thread = Mock()
        runtime._flip_thread.is_alive.return_value = True
        with self.assertRaisesRegex(RuntimeError, "flip is running"):
            runtime.prepare_reset()
        runtime._flip_thread.is_alive.return_value = False
        runtime.snapshot = Mock(return_value={})
        runtime.prepare_reset()
        runtime.cancel_reset()
        self.assertFalse(runtime._maintenance)

    def test_prepare_reset_is_idempotent_after_an_ambiguous_response_loss(self) -> None:
        runtime = self.runtime()
        runtime.snapshot = Mock(return_value={"flip": {"status": "idle"}})
        first = runtime.prepare_reset()
        second = runtime.prepare_reset()
        self.assertEqual(second, first)
        self.assertTrue(runtime._maintenance)
        self.assertEqual(runtime.workload.stop_all.call_count, 2)

    def test_live_target_rate_change_is_allowed_but_pool_shape_is_frozen(self) -> None:
        from flipbench.playground import WorkloadSettings

        runtime = self.runtime()
        runtime.settings = SimpleNamespace(table_count=5)
        runtime._lock = Lock()
        runtime._admission_generation = 0
        runtime._admission = Mock()
        current = WorkloadSettings(mode="target_rate_v1")
        runtime.workload.settings.return_value = current
        runtime.workload.running.return_value = True
        runtime.workload.update.side_effect = lambda value: value

        updated = runtime.update_workload({"active_target_tps": 12_000})
        self.assertEqual(updated.active_target_tps, 12_000)
        runtime._admission.reset.assert_called_once_with()

        with self.assertRaisesRegex(RuntimeError, "restart writes"):
            runtime.update_workload({"active_workers": 40})


if __name__ == "__main__":
    unittest.main()
