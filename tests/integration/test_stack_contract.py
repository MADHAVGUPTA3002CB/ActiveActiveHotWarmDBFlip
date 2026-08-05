import os
import time
import unittest
import uuid


@unittest.skipUnless(os.environ.get("FLIPBENCH_INTEGRATION") == "1", "requires the running Compose stack")
class StackContractTests(unittest.TestCase):
    def test_atomic_detach_marker_commits_and_rolls_back_as_one_unit(self) -> None:
        from psycopg import sql

        from flipbench.connector_configs import FENCE_SCHEMA
        from flipbench.core import build_leaf_fence_markers, build_manifest
        from flipbench.postgres_io import (
            atomic_detach_and_emit_leaf_fence_marker,
            connect,
        )
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        route = manifest.tables[0]
        conflict_marker = build_leaf_fence_markers(
            manifest, uuid.uuid4(), 1_001
        )[0]
        success_marker = build_leaf_fence_markers(
            manifest, uuid.uuid4(), 1_002
        )[0]
        marker_table = sql.Identifier(FENCE_SCHEMA, route.leaf)
        with connect(settings.hot_dsn, autocommit=True) as hot:
            hot.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (
                        marker_schema_version, marker_id, attempt_id, attempt_epoch,
                        ownership_epoch, cell, timeslot, parent_name, leaf_name
                    ) VALUES (1, %s, %s, %s, 1, %s, %s, %s, %s)
                    """
                ).format(marker_table),
                (
                    uuid.uuid4(),
                    conflict_marker.attempt_id,
                    conflict_marker.attempt_epoch,
                    conflict_marker.cell,
                    conflict_marker.timeslot,
                    conflict_marker.parent,
                    conflict_marker.leaf,
                ),
            )
            try:
                with self.assertRaisesRegex(RuntimeError, "conflicting durable"):
                    atomic_detach_and_emit_leaf_fence_marker(
                        hot, conflict_marker, ownership_epoch=1
                    )
                attached_after_rollback = hot.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_inherits
                        WHERE inhparent=%s::regclass AND inhrelid=%s::regclass
                    )
                    """,
                    (f"public.{route.parent}", f"public.{route.leaf}"),
                ).fetchone()[0]
                self.assertTrue(attached_after_rollback)

                atomic_detach_and_emit_leaf_fence_marker(
                    hot, success_marker, ownership_epoch=1
                )
                attached_after_commit = hot.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_inherits
                        WHERE inhparent=%s::regclass AND inhrelid=%s::regclass
                    )
                    """,
                    (f"public.{route.parent}", f"public.{route.leaf}"),
                ).fetchone()[0]
                committed_marker = hot.execute(
                    sql.SQL("SELECT marker_id FROM {} WHERE attempt_id=%s").format(
                        marker_table
                    ),
                    (success_marker.attempt_id,),
                ).fetchone()
                self.assertFalse(attached_after_commit)
                self.assertEqual(committed_marker, (success_marker.marker_id,))
            finally:
                attached = hot.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_inherits
                        WHERE inhparent=%s::regclass AND inhrelid=%s::regclass
                    )
                    """,
                    (f"public.{route.parent}", f"public.{route.leaf}"),
                ).fetchone()[0]
                if not attached:
                    hot.execute(
                        sql.SQL(
                            "ALTER TABLE {} ATTACH PARTITION {} "
                            "FOR VALUES FROM ('2026-07-31 12:00:00+00') "
                            "TO ('2026-08-01 00:00:00+00')"
                        ).format(
                            sql.Identifier(route.parent),
                            sql.Identifier(route.leaf),
                        )
                    )
                hot.execute(
                    sql.SQL("DELETE FROM {} WHERE attempt_id IN (%s, %s)").format(
                        marker_table
                    ),
                    (conflict_marker.attempt_id, success_marker.attempt_id),
                )

    def test_per_leaf_markers_reach_exact_topics_and_warm_receipts_only(self) -> None:
        from flipbench.core import build_leaf_fence_markers, build_manifest
        from flipbench.kafka_io import KafkaControl
        from flipbench.postgres_io import (
            connect,
            emit_leaf_fence_markers,
            observed_leaf_fence_receipts,
        )
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        self.assertEqual(settings.source_topology, "isolated")
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        attempt_id = uuid.uuid4()
        markers = build_leaf_fence_markers(manifest, attempt_id, 999)
        kafka = KafkaControl(settings.kafka_bootstrap)
        partitions = tuple(marker.partition for marker in markers)
        baselines = kafka.end_offsets(partitions)
        with connect(settings.hot_dsn, autocommit=True) as hot:
            emit_leaf_fence_markers(hot, markers, ownership_epoch=1)
        targets = kafka.wait_leaf_fence_markers(markers, baselines, 20)
        self.assertEqual(set(targets), set(partitions))
        self.assertTrue(all(targets[item] > baselines[item] for item in partitions))

        deadline = time.monotonic() + 20
        with connect(settings.warm_dsn, autocommit=True) as warm:
            while time.monotonic() < deadline:
                receipts = observed_leaf_fence_receipts(warm, markers, 1)
                if receipts == frozenset(partitions):
                    break
                time.sleep(0.05)
            else:
                self.fail("warm JDBC sink did not persist every leaf marker receipt")
            business_rows = sum(
                warm.execute(
                    f'SELECT count(*) FROM public."{route.parent}" WHERE experiment_run_id=%s',
                    (attempt_id,),
                ).fetchone()[0]
                for route in manifest.tables
            )
        self.assertEqual(business_rows, 0)

    def test_optimistic_batch_routes_are_allowlisted_and_direct_table_bypass_is_denied(self) -> None:
        from psycopg import sql
        from psycopg.types.json import Jsonb

        from flipbench.core import build_manifest
        from flipbench.postgres_io import (
            OptimisticDetachTransactionSession,
            connect,
            hot_write_gate_status,
        )
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        run_id = uuid.uuid4()
        with connect(settings.hot_dsn, autocommit=True) as admin:
            gate = hot_write_gate_status(admin, settings.cell, "retiring")
            self.assertFalse(
                admin.execute(
                    "SELECT has_table_privilege(%s, %s, 'INSERT')",
                    (settings.writer_database_user, f"public.{manifest.tables[0].parent}"),
                ).fetchone()[0]
            )
            self.assertTrue(
                admin.execute(
                    "SELECT has_function_privilege(%s, %s, 'EXECUTE')",
                    (
                        settings.writer_database_user,
                        "flipbench_guard.insert_events_optimistic(text,text,name,jsonb)",
                    ),
                ).fetchone()[0]
            )
            self.assertTrue(
                admin.execute(
                    "SELECT has_function_privilege(%s, %s, 'EXECUTE')",
                    (
                        settings.writer_database_user,
                        "flipbench_guard.admit_optimistic_batch(text,text,bigint)",
                    ),
                ).fetchone()[0]
            )
            function_definition = admin.execute(
                "SELECT pg_get_functiondef('flipbench_guard.insert_events_optimistic(text,text,name,jsonb)'::regprocedure)"
            ).fetchone()[0]
            self.assertNotIn("partition_write_gates", function_definition)

        session = OptimisticDetachTransactionSession(
            settings.writer_hot_dsn,
            manifest,
            run_id,
            "retiring",
            64,
            gate.ownership_epoch,
            operations_per_batch=len(manifest.tables),
        )
        try:
            self.assertEqual(session.write(2, 1), 1)
        finally:
            session.close()
        with connect(settings.hot_dsn, autocommit=True) as admin:
            counts = tuple(
                admin.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE experiment_run_id=%s").format(
                        sql.Identifier(route.leaf)
                    ),
                    (run_id,),
                ).fetchone()[0]
                for route in manifest.tables
            )
        self.assertEqual(counts, (0, 0, 1, 0, 0))

        row = Jsonb(
            [{
                "id": str(uuid.uuid4()),
                "experiment_run_id": str(uuid.uuid4()),
                "sequence_no": 1,
                "created_at": "2026-07-31T12:00:00+00:00",
                "payload": {"timeslot": "retiring"},
            }]
        )
        with connect(settings.writer_hot_dsn) as writer:
            with self.assertRaisesRegex(Exception, "route is not authorized"):
                writer.execute(
                    "SELECT flipbench_guard.insert_events_optimistic(%s, %s, %s, %s)",
                    (settings.cell, "retiring", "not_allowed", row),
                )

    def test_optimistic_detach_allows_partial_batch_completion(self) -> None:
        from psycopg import sql

        from flipbench.core import build_manifest
        from flipbench.postgres_io import (
            OptimisticDetachTransactionSession,
            connect,
            hot_write_gate_status,
        )
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        detached_route = manifest.tables[1]
        run_id = uuid.uuid4()
        with connect(settings.hot_dsn, autocommit=True) as admin:
            gate = hot_write_gate_status(admin, settings.cell, "retiring")
            try:
                session = OptimisticDetachTransactionSession(
                    settings.writer_hot_dsn,
                    manifest,
                    run_id,
                    "retiring",
                    64,
                    gate.ownership_epoch,
                    operations_per_batch=len(manifest.tables),
                )
                try:
                    self.assertEqual(session.write(0, 1), 1)
                    admin.execute(
                        sql.SQL("ALTER TABLE {} DETACH PARTITION {}").format(
                            sql.Identifier(detached_route.parent),
                            sql.Identifier(detached_route.leaf),
                        )
                    )
                    with self.assertRaises(Exception):
                        session.write(1, 1)
                finally:
                    session.close()
                counts = tuple(
                    admin.execute(
                        sql.SQL("SELECT count(*) FROM {} WHERE experiment_run_id=%s").format(
                            sql.Identifier(route.leaf)
                        ),
                        (run_id,),
                    ).fetchone()[0]
                    for route in manifest.tables
                )
                self.assertEqual(counts, (1, 0, 0, 0, 0))
            finally:
                admin.execute(
                    sql.SQL(
                        "ALTER TABLE {} ATTACH PARTITION {} "
                        "FOR VALUES FROM ('2026-07-31 12:00:00+00') TO ('2026-08-01 00:00:00+00')"
                    ).format(
                        sql.Identifier(detached_route.parent),
                        sql.Identifier(detached_route.leaf),
                    )
                )

    def test_h_state_only_admission_checks_open_state_without_an_epoch(self) -> None:
        from psycopg import sql

        from flipbench.core import build_manifest
        from flipbench.postgres_io import (
            OptimisticDetachTransactionSession,
            connect,
            hot_write_gate_status,
            park_hot_write_gate,
            reopen_hot_write_gate,
        )
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        run_id = uuid.uuid4()
        attempt_id = uuid.uuid4()
        with connect(settings.hot_dsn, autocommit=True) as admin:
            gate = hot_write_gate_status(admin, settings.cell, "retiring")
            session = OptimisticDetachTransactionSession(
                settings.writer_hot_dsn,
                manifest,
                run_id,
                "retiring",
                64,
                expected_ownership_epoch=None,
                operations_per_batch=1,
                admission_check_mode="state_only_v1",
            )
            parked = False
            try:
                self.assertEqual(session.write(0, 1), 1)
                park_hot_write_gate(
                    admin,
                    settings.cell,
                    "retiring",
                    attempt_id,
                    gate.ownership_epoch,
                )
                parked = True
                with self.assertRaisesRegex(RuntimeError, "hot writer parked"):
                    session.write(1, 1)
            finally:
                session.close()
                if parked:
                    reopen_hot_write_gate(
                        admin,
                        settings.cell,
                        "retiring",
                        attempt_id,
                        None,
                    )
                for route in manifest.tables:
                    admin.execute(
                        sql.SQL("DELETE FROM {} WHERE experiment_run_id=%s").format(
                            sql.Identifier(route.leaf)
                        ),
                        (run_id,),
                    )

    def test_startup_reconciliation_refuses_a_live_flip_coordinator(self) -> None:
        from flipbench.core import build_manifest
        from flipbench.lifecycle import lifecycle_lock_name
        from flipbench.postgres_io import connect, reconcile_hot_write_gate
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        lock_name = lifecycle_lock_name(settings.cell, settings.timeslot)
        with connect(settings.hot_dsn, autocommit=True) as coordinator, connect(
            settings.hot_dsn, autocommit=True
        ) as startup, connect(settings.warm_dsn, autocommit=True) as warm:
            acquired = coordinator.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (lock_name,),
            ).fetchone()[0]
            self.assertTrue(acquired)
            try:
                with self.assertRaisesRegex(RuntimeError, "live flip coordinator"):
                    reconcile_hot_write_gate(startup, warm, manifest)
            finally:
                released = coordinator.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                    (lock_name,),
                ).fetchone()[0]
                self.assertTrue(released)

    def test_startup_reconciles_an_orphaned_hot_gate_preparation(self) -> None:
        from flipbench.core import build_manifest
        from flipbench.postgres_io import (
            connect,
            hot_write_gate_status,
            park_hot_write_gate,
            reconcile_hot_write_gate,
        )
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        with connect(settings.hot_dsn, autocommit=True) as hot, connect(
            settings.warm_dsn, autocommit=True
        ) as warm:
            before = hot_write_gate_status(hot, settings.cell, "retiring")
            attempt_id = uuid.uuid4()
            park_hot_write_gate(
                hot,
                settings.cell,
                "retiring",
                attempt_id,
                before.ownership_epoch,
            )
            self.assertEqual(
                reconcile_hot_write_gate(hot, warm, manifest),
                "reopened_orphan_preparation",
            )
            after = hot_write_gate_status(hot, settings.cell, "retiring")
            self.assertEqual(after.state, "open")
            self.assertEqual(after.ownership_epoch, before.ownership_epoch + 1)

    def test_hot_transactional_fence_waits_for_writer_and_is_timeslot_scoped(self) -> None:
        from datetime import datetime, timezone

        from psycopg.types.json import Jsonb

        from flipbench.core import build_manifest
        from flipbench.postgres_io import (
            ACTIVE_START,
            RETIRING_START,
            HotFencedTransactionSession,
            connect,
            hot_write_gate_status,
            park_hot_write_gate,
            reopen_hot_write_gate,
        )
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        attempt_id = uuid.uuid4()
        retiring_run_id = uuid.uuid4()
        active_run_id = uuid.uuid4()
        with connect(settings.hot_dsn, autocommit=True) as admin:
            retiring_gate = hot_write_gate_status(admin, settings.cell, "retiring")
            active_gate = hot_write_gate_status(admin, settings.cell, "active")
            self.assertEqual((retiring_gate.state, active_gate.state), ("open", "open"))
            self.assertFalse(
                admin.execute(
                    "SELECT has_table_privilege(%s, %s, 'INSERT')",
                    (settings.writer_database_user, f"public.{manifest.tables[0].parent}"),
                ).fetchone()[0]
            )
            self.assertTrue(
                admin.execute(
                    "SELECT has_function_privilege(%s, %s, 'EXECUTE')",
                    (
                        settings.writer_database_user,
                        "flipbench_guard.insert_events(text,text,bigint,name,jsonb)",
                    ),
                ).fetchone()[0]
            )
            writer_security = admin.execute(
                """
                SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication,
                       rolbypassrls,
                       (SELECT count(*) FROM pg_auth_members WHERE member=roles.oid)
                FROM pg_roles AS roles WHERE rolname=%s
                """,
                (settings.writer_database_user,),
            ).fetchone()
            self.assertEqual(writer_security, (False, False, False, False, False, 0))

        row = Jsonb(
            [
                {
                    "id": str(uuid.uuid4()),
                    "experiment_run_id": str(retiring_run_id),
                    "sequence_no": 1,
                    "created_at": RETIRING_START.isoformat(),
                    "payload": {"timeslot": "retiring", "at": datetime.now(timezone.utc).isoformat()},
                }
            ]
        )
        with connect(settings.writer_hot_dsn) as writer, connect(
            settings.hot_dsn, autocommit=True
        ) as admin:
            inserted = writer.execute(
                "SELECT flipbench_guard.insert_events(%s, %s, %s, %s, %s)",
                (
                    settings.cell,
                    "retiring",
                    retiring_gate.ownership_epoch,
                    manifest.tables[0].parent,
                    row,
                ),
            ).fetchone()[0]
            self.assertEqual(inserted, 1)
            admin.execute("SET statement_timeout='200ms'")
            with self.assertRaises(Exception):
                park_hot_write_gate(
                    admin,
                    settings.cell,
                    "retiring",
                    attempt_id,
                    retiring_gate.ownership_epoch,
                )
            writer.commit()
            admin.execute("SET statement_timeout='5s'")
            park_hot_write_gate(
                admin,
                settings.cell,
                "retiring",
                attempt_id,
                retiring_gate.ownership_epoch,
            )

            retiring = HotFencedTransactionSession(
                settings.writer_hot_dsn,
                manifest,
                retiring_run_id,
                "retiring",
                64,
                retiring_gate.ownership_epoch,
            )
            active = HotFencedTransactionSession(
                settings.writer_hot_dsn,
                manifest,
                active_run_id,
                "active",
                64,
                active_gate.ownership_epoch,
            )
            try:
                with self.assertRaisesRegex(RuntimeError, "hot writer parked"):
                    retiring.write(0, 1)
                self.assertEqual(active.write(0, 1), 1)
                with connect(settings.writer_hot_dsn) as bypass:
                    with self.assertRaisesRegex(
                        Exception, "does not belong to the selected timeslot"
                    ):
                        bypass.execute(
                            "SELECT flipbench_guard.insert_events(%s, %s, %s, %s, %s)",
                            (
                                settings.cell,
                                "active",
                                active_gate.ownership_epoch,
                                manifest.tables[0].parent,
                                row,
                            ),
                        )
            finally:
                retiring.close()
                active.close()
                reopen_hot_write_gate(
                    admin, settings.cell, "retiring", attempt_id, None
                )
            stale = HotFencedTransactionSession(
                settings.writer_hot_dsn,
                manifest,
                retiring_run_id,
                "retiring",
                64,
                retiring_gate.ownership_epoch,
            )
            try:
                with self.assertRaisesRegex(RuntimeError, "hot writer parked"):
                    stale.write(0, 1)
            finally:
                stale.close()

    def test_hot_and_warm_are_distinct_and_plugins_exist(self) -> None:
        from flipbench.connect_api import ConnectClient
        from flipbench.postgres_io import connect
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        with connect(settings.hot_dsn, autocommit=True) as hot, connect(settings.warm_dsn, autocommit=True) as warm:
            hot_id = hot.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0]
            warm_id = warm.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0]
        self.assertNotEqual(hot_id, warm_id)
        self.assertIn(
            "io.debezium.connector.postgresql.PostgresConnector",
            ConnectClient(settings.source_connect_url).plugins(),
        )
        self.assertIn(
            "io.debezium.connector.jdbc.JdbcSinkConnector",
            ConnectClient(settings.sink_connect_url).plugins(),
        )

    def test_live_connectors_use_leaf_topics_and_one_shared_sink(self) -> None:
        from flipbench.connect_api import ConnectClient
        from flipbench.connector_configs import source_specs
        from flipbench.core import build_manifest
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        client = ConnectClient(settings.source_connect_url)
        sources = source_specs(settings, manifest)
        for spec in sources:
            client.wait_state(spec.connector_name, "RUNNING")
        client.wait_state(manifest.tables[0].sink_connector, "RUNNING")
        source_configs = [client.config(spec.connector_name) for spec in sources]
        sink = client.config(manifest.tables[0].sink_connector)
        self.assertTrue(
            all(config["publish.via.partition.root"] == "false" for config in source_configs)
        )
        self.assertTrue(
            any("bench_table_01_p_retiring" in config["table.include.list"] for config in source_configs)
        )
        self.assertTrue(
            any("bench_table_01_p_active" in config["table.include.list"] for config in source_configs)
        )
        self.assertEqual(
            sink["topics.regex"],
            r"^cards\.cell01\.public\.(?:bench_table_01|bench_table_02|bench_table_03|bench_table_04|bench_table_05)_p_(retiring|active)$",
        )
        self.assertEqual(sink["transforms.route.replacement"], r"public.$1")

    def test_live_publications_match_contract_and_detect_transactional_drift(self) -> None:
        from psycopg import sql

        from flipbench.connector_configs import source_specs
        from flipbench.core import build_manifest
        from flipbench.postgres_io import connect, verify_source_publication
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        specs = source_specs(settings, manifest)
        with connect(settings.hot_dsn, autocommit=True) as hot:
            for spec in specs:
                verify_source_publication(hot, manifest, spec)
            fence_source = next(
                (spec for spec in specs if spec.lane == "migration"), specs[0]
            )
            hot.execute("BEGIN")
            try:
                hot.execute(
                    sql.SQL("ALTER TABLE {} DETACH PARTITION {}").format(
                        sql.Identifier(manifest.tables[0].parent),
                        sql.Identifier(manifest.tables[0].leaf),
                    )
                )
                verify_source_publication(hot, manifest, fence_source)
            finally:
                hot.execute("ROLLBACK")
            verify_source_publication(hot, manifest, fence_source)
            if settings.source_topology == "isolated":
                migration = fence_source
                with self.assertRaisesRegex(RuntimeError, "membership mismatch"):
                    hot.execute("BEGIN")
                    hot.execute(
                        sql.SQL("ALTER PUBLICATION {} DROP TABLE {}").format(
                            sql.Identifier(migration.publication_name),
                            sql.Identifier(manifest.tables[0].leaf),
                        )
                    )
                    try:
                        verify_source_publication(hot, manifest, migration)
                    finally:
                        hot.execute("ROLLBACK")
            verify_source_publication(hot, manifest, fence_source)

    def test_leaf_topics_have_exact_rf3_durability(self) -> None:
        from flipbench.connector_configs import topic_names
        from flipbench.core import build_manifest
        from flipbench.kafka_io import KafkaControl, TopicSpec
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        control = KafkaControl(settings.kafka_bootstrap)
        control.validate_topics(
            tuple(
                TopicSpec(
                    name,
                    partitions=1,
                    replication_factor=settings.kafka_topic_replication_factor,
                    min_insync_replicas=settings.kafka_min_insync_replicas,
                )
                for name in topic_names(manifest, settings)
            )
        )

    def test_connect_internal_topics_have_exact_rf3_durability(self) -> None:
        from flipbench.kafka_io import KafkaControl, TopicSpec
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        control = KafkaControl(settings.kafka_bootstrap)
        control.validate_topics(
            (
                TopicSpec("flipbench-connect-configs", 1, 3, 2),
                TopicSpec("flipbench-connect-offsets", 25, 3, 2),
                TopicSpec("flipbench-connect-status", 5, 3, 2),
            )
        )

    def test_active_and_retiring_leaf_events_route_to_warm_parents(self) -> None:
        from psycopg import sql

        from flipbench.core import build_manifest
        from flipbench.postgres_io import connect, guarded_insert_events
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        run_id = uuid.uuid4()
        guarded_insert_events(
            settings.hot_dsn, settings.warm_dsn, manifest, run_id, 1, "retiring", 64
        )
        guarded_insert_events(
            settings.hot_dsn, settings.warm_dsn, manifest, run_id, 1, "active", 64
        )
        deadline = time.monotonic() + 30
        observed = ()
        with connect(settings.warm_dsn, autocommit=True) as warm:
            while time.monotonic() < deadline:
                observed = tuple(
                    warm.execute(
                        sql.SQL("SELECT count(*) FROM {} WHERE experiment_run_id=%s").format(
                            sql.Identifier(route.parent)
                        ),
                        (run_id,),
                    ).fetchone()[0]
                    for route in manifest.tables
                )
                if observed == (2, 2, 2, 2, 2):
                    break
                time.sleep(0.1)
        self.assertEqual(observed, (2, 2, 2, 2, 2))


if __name__ == "__main__":
    unittest.main()
