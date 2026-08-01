import os
import time
import unittest
import uuid


@unittest.skipUnless(os.environ.get("FLIPBENCH_INTEGRATION") == "1", "requires the running Compose stack")
class StackContractTests(unittest.TestCase):
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
        from flipbench.core import build_manifest
        from flipbench.settings import Settings

        settings = Settings.from_env(5)
        manifest = build_manifest(5, settings.cell, settings.timeslot)
        client = ConnectClient(settings.source_connect_url)
        client.wait_state(settings.source_connector, "RUNNING")
        client.wait_state(manifest.tables[0].sink_connector, "RUNNING")
        source = client.config(settings.source_connector)
        sink = client.config(manifest.tables[0].sink_connector)
        self.assertEqual(source["publish.via.partition.root"], "false")
        self.assertIn("bench_table_01_p_retiring", source["table.include.list"])
        self.assertIn("bench_table_01_p_active", source["table.include.list"])
        self.assertEqual(
            sink["topics.regex"],
            r"^cards\.cell01\.public\.(?:bench_table_01|bench_table_02|bench_table_03|bench_table_04|bench_table_05)_p_(retiring|active)$",
        )
        self.assertEqual(sink["transforms.route.replacement"], r"public.$1")

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
                for name in topic_names(manifest)
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
