from __future__ import annotations

import time
import unittest
import uuid
from importlib.util import find_spec
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flipbench.connector_configs import source_specs
from flipbench.core import build_manifest
from flipbench.settings import Settings


def isolated_settings() -> Settings:
    return Settings(
        hot_dsn="postgresql://hot/cards",
        warm_dsn="postgresql://warm/cards",
        kafka_bootstrap="kafka:19092",
        source_connect_url="http://source:8083",
        sink_connect_url="http://sink:8083",
        postgres_password="test-only",
        table_count=5,
        results_dir=Path("results"),
        source_topology="isolated",
    )


def shared_settings() -> Settings:
    return Settings(
        hot_dsn="postgresql://hot/cards",
        warm_dsn="postgresql://warm/cards",
        kafka_bootstrap="kafka:19092",
        source_connect_url="http://source:8083",
        sink_connect_url="http://sink:8083",
        postgres_password="test-only",
        table_count=5,
        results_dir=Path("results"),
        source_topology="shared",
    )


@unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in the runner image")
class PublicationContractTests(unittest.TestCase):
    def test_variant_h_requires_state_only_application_admission(self) -> None:
        from flipbench.core import SourceProofMode
        from flipbench.flip import FlipRunner

        configured = isolated_settings()
        scenario = {
            "write_fence_mode": "optimistic_detach_v1",
            "retiring_write_gate_epoch": 1,
            "optimistic_admission_check_mode": "state_only_v1",
        }
        runner = FlipRunner(
            configured,
            uuid.uuid4(),
            1.0,
            0.05,
            scenario_metadata=scenario,
            source_proof_mode=SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
        )
        self.assertEqual(
            runner.optimistic_admission_check_mode.value,
            "state_only_v1",
        )

        with self.assertRaisesRegex(ValueError, "Variant H"):
            FlipRunner(
                configured,
                uuid.uuid4(),
                1.0,
                0.05,
                scenario_metadata={
                    **scenario,
                    "optimistic_admission_check_mode": "state_and_epoch_v1",
                },
                source_proof_mode=(
                    SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER
                ),
            )

    def test_variant_h_prod_accepts_the_shared_source_topology(self) -> None:
        from flipbench.core import SourceProofMode
        from flipbench.flip import FlipRunner

        runner = FlipRunner(
            shared_settings(),
            uuid.uuid4(),
            1.0,
            0.05,
            scenario_metadata={
                "write_fence_mode": "optimistic_detach_v1",
                "retiring_write_gate_epoch": 1,
                "optimistic_admission_check_mode": "state_only_v1",
            },
            source_proof_mode=SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER,
        )
        self.assertEqual(runner.settings.source_topology, "shared")
        self.assertEqual(
            runner.optimistic_admission_check_mode.value,
            "state_only_v1",
        )

    def test_variant_a_accepts_hot_state_only_admission_with_shared_lsn_proof(self) -> None:
        from flipbench.core import SourceProofMode
        from flipbench.flip import FlipRunner

        runner = FlipRunner(
            shared_settings(),
            uuid.uuid4(),
            1.0,
            0.05,
            scenario_metadata={
                "write_fence_mode": "optimistic_detach_v1",
                "retiring_write_gate_epoch": 1,
                "optimistic_admission_check_mode": "state_only_v1",
            },
            source_proof_mode=SourceProofMode.SLOT_LSN,
        )

        self.assertEqual(runner.settings.source_topology, "shared")
        self.assertEqual(runner.source_proof_mode, SourceProofMode.SLOT_LSN)
        self.assertEqual(
            runner.optimistic_admission_check_mode.value,
            "state_only_v1",
        )

    def test_serial_marker_variants_reject_the_shared_source_topology(self) -> None:
        from flipbench.core import SourceProofMode
        from flipbench.flip import FlipRunner

        for proof_mode in (
            SourceProofMode.PER_LEAF_MARKER,
            SourceProofMode.ATOMIC_DETACH_MARKER,
        ):
            with self.assertRaisesRegex(ValueError, "isolated sources"):
                FlipRunner(
                    shared_settings(),
                    uuid.uuid4(),
                    1.0,
                    0.05,
                    scenario_metadata={
                        "write_fence_mode": "optimistic_detach_v1",
                        "retiring_write_gate_epoch": 1,
                    },
                    source_proof_mode=proof_mode,
                )

    def test_state_only_application_admission_is_not_enabled_for_g(self) -> None:
        from flipbench.core import SourceProofMode
        from flipbench.flip import FlipRunner

        with self.assertRaisesRegex(ValueError, "reserved for Variant H"):
            FlipRunner(
                isolated_settings(),
                uuid.uuid4(),
                1.0,
                0.05,
                scenario_metadata={
                    "write_fence_mode": "optimistic_detach_v1",
                    "retiring_write_gate_epoch": 1,
                    "optimistic_admission_check_mode": "state_only_v1",
                },
                source_proof_mode=SourceProofMode.ATOMIC_DETACH_MARKER,
            )

    def test_immediate_heartbeat_runs_after_fence_persistence_before_slot_wait(self) -> None:
        from flipbench.core import FenceWakeupMode, HotSourceIdentity
        from flipbench.flip import FlipRunner

        configured = isolated_settings()
        manifest = build_manifest(5, "cell01", "retiring")
        migration = source_specs(configured, manifest)[1]
        runner = FlipRunner(
            configured,
            uuid.uuid4(),
            1.0,
            0.05,
            fence_wakeup_mode=FenceWakeupMode.IMMEDIATE_HEARTBEAT,
        )
        runner._attempt_epoch = 7
        order: list[str] = []
        hot = Mock()
        warm = Mock()
        warm.execute.side_effect = lambda *_args, **_kwargs: (
            order.append("persist") or Mock(rowcount=1)
        )
        identity = HotSourceIdentity(
            "cell01", "123456789", "cards", migration.slot_name
        )
        confirmed = SimpleNamespace(identity=identity, confirmed_lsn="2/0")
        runner._set_statement_timeout = Mock(
            side_effect=lambda *_args: order.append("timeout")
        )

        with patch(
            "flipbench.flip.hot_identity",
            side_effect=lambda *_args: (order.append("capture") or (identity, "1/0")),
        ), patch(
            "flipbench.flip.trigger_source_heartbeat",
            side_effect=lambda *_args: (order.append("heartbeat") or 1),
        ), patch(
            "flipbench.flip.current_source_wal_flush_lsn",
            side_effect=lambda *_args: (order.append("observe") or "2/0"),
        ), patch(
            "flipbench.flip.wait_slot_lsn",
            side_effect=lambda *_args: (order.append("wait") or confirmed),
        ):
            _, fence_lsn, _, evidence = runner._capture_and_confirm_source_fence(
                hot, warm, migration, time.monotonic() + 1
            )

        self.assertEqual(
            order,
            [
                "capture",
                "persist",
                "timeout",
                "heartbeat",
                "timeout",
                "observe",
                "wait",
            ],
        )
        self.assertEqual(fence_lsn, "1/0")
        self.assertEqual(evidence["heartbeat_table"], "dbz_heartbeat_migration")
        self.assertEqual(list(runner.timestamps)[-4:], ["t5", "t6", "t6w", "t7"])

    def test_passive_fence_wakeup_does_not_issue_an_update(self) -> None:
        from flipbench.core import HotSourceIdentity
        from flipbench.flip import FlipRunner

        configured = isolated_settings()
        migration = source_specs(configured, build_manifest(5, "cell01", "retiring"))[1]
        runner = FlipRunner(configured, uuid.uuid4(), 1.0, 0.05)
        runner._attempt_epoch = 7
        hot = Mock()
        warm = Mock()
        warm.execute.return_value.rowcount = 1
        identity = HotSourceIdentity("cell01", "123456789", "cards", migration.slot_name)
        confirmed = SimpleNamespace(identity=identity, confirmed_lsn="1/0")

        with patch("flipbench.flip.hot_identity", return_value=(identity, "1/0")), patch(
            "flipbench.flip.trigger_source_heartbeat"
        ) as heartbeat, patch("flipbench.flip.wait_slot_lsn", return_value=confirmed):
            _, _, _, evidence = runner._capture_and_confirm_source_fence(
                hot, warm, migration, time.monotonic() + 1
            )

        heartbeat.assert_not_called()
        self.assertFalse(evidence["attempted"])
        self.assertIn("t6w", runner.timestamps)

    def test_committed_heartbeat_evidence_survives_a_later_slot_timeout(self) -> None:
        from flipbench.core import FenceWakeupMode, HotSourceIdentity
        from flipbench.flip import FlipRunner

        configured = isolated_settings()
        migration = source_specs(configured, build_manifest(5, "cell01", "retiring"))[1]
        runner = FlipRunner(
            configured,
            uuid.uuid4(),
            1.0,
            0.05,
            fence_wakeup_mode=FenceWakeupMode.IMMEDIATE_HEARTBEAT,
        )
        runner._attempt_epoch = 7
        hot = Mock()
        warm = Mock()
        warm.execute.return_value.rowcount = 1
        identity = HotSourceIdentity("cell01", "123456789", "cards", migration.slot_name)

        with patch("flipbench.flip.hot_identity", return_value=(identity, "1/0")), patch(
            "flipbench.flip.trigger_source_heartbeat",
            return_value=1,
        ), patch(
            "flipbench.flip.current_source_wal_flush_lsn", return_value="2/0"
        ), patch("flipbench.flip.wait_slot_lsn", side_effect=TimeoutError("slot timeout")):
            with self.assertRaisesRegex(TimeoutError, "slot timeout"):
                runner._capture_and_confirm_source_fence(
                    hot, warm, migration, time.monotonic() + 1
                )

        self.assertTrue(runner._fence_wakeup_evidence["attempted"])
        self.assertTrue(runner._fence_wakeup_evidence["applied"])
        self.assertEqual(runner._fence_wakeup_evidence["rows_updated"], 1)
        self.assertIsNone(runner._fence_wakeup_evidence["confirmed_flush_lsn_at_t7"])

    def test_committed_heartbeat_evidence_survives_wal_observation_failure(self) -> None:
        from flipbench.core import FenceWakeupMode, HotSourceIdentity
        from flipbench.flip import FlipRunner

        configured = isolated_settings()
        migration = source_specs(configured, build_manifest(5, "cell01", "retiring"))[1]
        runner = FlipRunner(
            configured,
            uuid.uuid4(),
            1.0,
            0.05,
            fence_wakeup_mode=FenceWakeupMode.IMMEDIATE_HEARTBEAT,
        )
        runner._attempt_epoch = 7
        hot = Mock()
        warm = Mock()
        warm.execute.return_value.rowcount = 1
        identity = HotSourceIdentity("cell01", "123456789", "cards", migration.slot_name)

        with patch("flipbench.flip.hot_identity", return_value=(identity, "1/0")), patch(
            "flipbench.flip.trigger_source_heartbeat", return_value=1
        ), patch(
            "flipbench.flip.current_source_wal_flush_lsn",
            side_effect=TimeoutError("WAL observation timeout"),
        ):
            with self.assertRaisesRegex(TimeoutError, "WAL observation timeout"):
                runner._capture_and_confirm_source_fence(
                    hot, warm, migration, time.monotonic() + 1
                )

        self.assertTrue(runner._fence_wakeup_evidence["attempted"])
        self.assertTrue(runner._fence_wakeup_evidence["applied"])
        self.assertEqual(runner._fence_wakeup_evidence["rows_updated"], 1)
        self.assertIsNone(runner._fence_wakeup_evidence["post_update_wal_lsn"])

    def test_paused_connectors_are_proven_running_before_flip_continues(self) -> None:
        from flipbench.flip import FlipRunner

        configured = isolated_settings()
        manifest = build_manifest(5, "cell01", "retiring")
        migration = source_specs(configured, manifest)[1]
        runner = FlipRunner(configured, uuid.uuid4(), 1.0, 0.05)
        source = Mock()
        sink = Mock()

        runner._resume_paused_connectors(
            source,
            sink,
            migration,
            "flipbench-sink",
            time.monotonic() + 1,
        )

        self.assertEqual(source.method_calls[0].args, (migration.connector_name, False))
        self.assertGreater(source.method_calls[0].kwargs["timeout_seconds"], 0)
        self.assertEqual(source.method_calls[1].args[:2], (migration.connector_name, "RUNNING"))
        self.assertEqual(sink.method_calls[0].args, ("flipbench-sink", False))
        self.assertGreater(sink.method_calls[0].kwargs["timeout_seconds"], 0)
        self.assertEqual(sink.method_calls[1].args[:2], ("flipbench-sink", "RUNNING"))

    def test_isolated_publication_memberships_are_exact_and_disjoint(self) -> None:
        from flipbench.postgres_io import expected_source_publication_tables

        manifest = build_manifest(5, "cell01", "retiring")
        active, migration = source_specs(isolated_settings(), manifest)
        active_tables = expected_source_publication_tables(manifest, active)
        migration_tables = expected_source_publication_tables(manifest, migration)

        self.assertIn(("public", "bench_table_01_p_active"), active_tables)
        self.assertNotIn(("public", "bench_table_01_p_retiring"), active_tables)
        self.assertIn(("public", "bench_table_01_p_retiring"), migration_tables)
        self.assertNotIn(("public", "bench_table_01_p_active"), migration_tables)
        self.assertIn(("flipbench_fence", "bench_table_01_p_retiring"), migration_tables)
        self.assertFalse(active_tables.intersection(migration_tables))

    def test_runtime_verifier_rejects_membership_and_option_drift(self) -> None:
        from flipbench.postgres_io import (
            expected_source_publication_tables,
            verify_source_publication,
        )

        manifest = build_manifest(5, "cell01", "retiring")
        migration = source_specs(isolated_settings(), manifest)[1]
        expected = expected_source_publication_tables(manifest, migration)

        valid_connection = Mock()
        valid_connection.execute.side_effect = [
            Mock(fetchone=Mock(return_value=(True, True, False, False, False))),
            Mock(fetchall=Mock(return_value=list(expected))),
        ]
        verify_source_publication(valid_connection, manifest, migration)

        drifted_membership = Mock()
        drifted_membership.execute.side_effect = [
            Mock(fetchone=Mock(return_value=(True, True, False, False, False))),
            Mock(fetchall=Mock(return_value=list(tuple(expected)[1:]))),
        ]
        with self.assertRaisesRegex(RuntimeError, "membership mismatch"):
            verify_source_publication(drifted_membership, manifest, migration)

        cross_schema_member = Mock()
        cross_schema_member.execute.side_effect = [
            Mock(fetchone=Mock(return_value=(True, True, False, False, False))),
            Mock(
                fetchall=Mock(
                    return_value=[
                        *expected,
                        ("audit", "unexpected_table"),
                    ]
                )
            ),
        ]
        with self.assertRaisesRegex(RuntimeError, "membership mismatch"):
            verify_source_publication(cross_schema_member, manifest, migration)

        drifted_options = Mock()
        drifted_options.execute.return_value.fetchone.return_value = (
            True,
            True,
            True,
            False,
            False,
        )
        with self.assertRaisesRegex(RuntimeError, "options mismatch"):
            verify_source_publication(drifted_options, manifest, migration)

    def test_trigger_source_heartbeat_targets_fence_table_and_fails_closed(self) -> None:
        from flipbench.postgres_io import trigger_source_heartbeat

        manifest = build_manifest(5, "cell01", "retiring")
        migration = source_specs(isolated_settings(), manifest)[1]
        connection = Mock()
        connection.execute.side_effect = [
            Mock(rowcount=1),
            Mock(fetchone=Mock(return_value=("2/0",))),
        ]

        rows_updated = trigger_source_heartbeat(connection, migration)

        query, parameters = connection.execute.call_args_list[0].args
        self.assertIn('"dbz_heartbeat_migration"', query.as_string())
        self.assertEqual(parameters, (1,))
        self.assertEqual(rows_updated, 1)
        self.assertEqual(connection.execute.call_count, 1)

        missing = Mock()
        missing.execute.return_value.rowcount = 0
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            trigger_source_heartbeat(missing, migration)

    def test_slot_wait_refreshes_statement_timeout_before_each_query(self) -> None:
        from flipbench.core import HotSourceIdentity
        from flipbench.postgres_io import wait_slot_lsn

        connection = Mock()
        connection.execute.side_effect = [
            Mock(),
            Mock(),
            Mock(fetchone=Mock(return_value=(True,))),
        ]
        status = SimpleNamespace(
            identity=HotSourceIdentity("cell01", "123", "cards", "slot"),
            confirmed_lsn="2/0",
        )

        with patch("flipbench.postgres_io.slot_status", return_value=status):
            observed = wait_slot_lsn(connection, "cell01", "slot", "1/0", 1, 0.01)

        self.assertIs(observed, status)
        self.assertEqual(connection.execute.call_count, 3)
        self.assertIn("statement_timeout", connection.execute.call_args_list[0].args[0])
        self.assertIn("statement_timeout", connection.execute.call_args_list[1].args[0])
        self.assertIn("pg_lsn", connection.execute.call_args_list[2].args[0])


if __name__ == "__main__":
    unittest.main()
