import os
import ast
import re
import unittest
from pathlib import Path
from unittest.mock import patch

from flipbench.connector_configs import (
    FENCE_HEADER_NAME,
    FENCE_HEADER_VALUE,
    fence_source_spec,
    source_specs,
    shared_sink_config,
    source_config,
    topic_names,
    warm_collection_for_topic,
)
from flipbench.core import ManifestError, build_manifest
from flipbench.settings import Settings


def settings() -> Settings:
    return Settings(
        hot_dsn="postgresql://hot/cards",
        warm_dsn="postgresql://warm/cards",
        kafka_bootstrap="kafka:19092",
        source_connect_url="http://source:8083",
        sink_connect_url="http://sink:8083",
        postgres_password="test-only",
        table_count=5,
        results_dir=Path("results"),
    )


class ConnectorConfigTests(unittest.TestCase):
    def test_source_is_manual_leaf_publication_with_durable_producer(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        config = source_config(settings(), manifest)
        self.assertEqual(config["publication.autocreate.mode"], "disabled")
        self.assertEqual(config["publish.via.partition.root"], "false")
        self.assertEqual(config["lsn.flush.mode"], "connector")
        self.assertEqual(config["producer.override.acks"], "all")
        self.assertEqual(config["decimal.handling.mode"], "precise")
        self.assertEqual(config["errors.tolerance"], "none")
        self.assertEqual(config["tasks.max"], "1")
        self.assertEqual(config["database.user"], "flipbench_cdc")
        self.assertIn(r"public\.bench_table_01_p_retiring", config["table.include.list"])

    def test_manifest_keeps_unique_leaf_topics_but_uses_one_shared_sink_identity(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        self.assertEqual(len({route.topic for route in manifest.tables}), 5)
        self.assertEqual({route.partition for route in manifest.tables}, {0})
        self.assertEqual({route.sink_connector for route in manifest.tables}, {"flipbench-sink"})
        self.assertEqual({route.sink_group for route in manifest.tables}, {"connect-flipbench-sink"})

    def test_shared_sink_uses_anchored_leaf_topic_regex_and_router(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        config = shared_sink_config(settings(), manifest)
        self.assertNotIn("topics", config)
        self.assertEqual(
            config["topics.regex"],
            r"^cards\.cell01\.public\.(?:bench_table_01|bench_table_02|bench_table_03|bench_table_04|bench_table_05)_p_(retiring|active)$",
        )
        self.assertEqual(config["transforms.route.type"], "org.apache.kafka.connect.transforms.RegexRouter")
        self.assertEqual(config["transforms.route.replacement"], r"public.$1")
        self.assertEqual(config["collection.name.format"], "${topic}")
        self.assertEqual(
            config["collection.naming.strategy"],
            "io.debezium.sink.naming.PassthroughCollectionNamingStrategy",
        )
        self.assertEqual(config["primary.key.mode"], "record_key")
        self.assertEqual(config["errors.tolerance"], "none")
        self.assertEqual(config["tasks.max"], "1")
        self.assertEqual(config["batch.size"], "500")
        self.assertEqual(config["connection.username"], "flipbench_sink")
        self.assertIsNotNone(re.fullmatch(config["topics.regex"], manifest.tables[0].topic))
        self.assertIsNone(
            re.fullmatch(config["topics.regex"], "cards.cell01.public.bench_table_06_p_retiring")
        )
        self.assertIsNone(
            re.fullmatch(config["topics.regex"], "cards.cell01.public.bench_table_99_p_active")
        )

    def test_leaf_topic_routing_maps_both_timeslots_to_correct_warm_parent(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        for route in manifest.tables:
            self.assertEqual(warm_collection_for_topic(manifest, route.topic), f"public.{route.parent}")
            active = f"cards.cell01.public.{route.parent}_p_active"
            self.assertEqual(warm_collection_for_topic(manifest, active), f"public.{route.parent}")
        for invalid in (
            "cards.cell02.public.bench_table_01_p_retiring",
            "cards.cell01.public.bench_table_01",
            "cards.cell01.public.dbz_heartbeat",
        ):
            with self.subTest(topic=invalid), self.assertRaises(ManifestError):
                warm_collection_for_topic(manifest, invalid)

    def test_topic_manifest_precreates_current_next_and_heartbeats(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        topics = topic_names(manifest)
        self.assertEqual(len(topics), 12)
        self.assertEqual(len(set(topics)), len(topics))

    def test_isolated_topology_uses_disjoint_active_and_migration_sources(self) -> None:
        configured = settings()
        object.__setattr__(configured, "source_topology", "isolated")
        manifest = build_manifest(5, "cell01", "retiring")
        specs = source_specs(configured, manifest)

        self.assertEqual([spec.lane for spec in specs], ["active", "migration"])
        self.assertEqual(len({spec.connector_name for spec in specs}), 2)
        self.assertEqual(len({spec.slot_name for spec in specs}), 2)
        self.assertEqual(len({spec.publication_name for spec in specs}), 2)
        self.assertEqual(len({spec.topic_prefix for spec in specs}), 2)
        active, migration = specs
        self.assertIn(r"public\.bench_table_01_p_active", active.config["table.include.list"])
        self.assertNotIn(r"public\.bench_table_01_p_retiring", active.config["table.include.list"])
        self.assertIn(r"public\.bench_table_01_p_retiring", migration.config["table.include.list"])
        self.assertNotIn(r"public\.bench_table_01_p_active", migration.config["table.include.list"])
        self.assertEqual(fence_source_spec(configured, manifest), migration)
        self.assertEqual(migration.config["transforms.route.replacement"], "cards.cell01.public.$1")

    def test_migration_source_routes_allowlisted_leaf_markers_with_a_control_header(self) -> None:
        configured = settings()
        object.__setattr__(configured, "source_topology", "isolated")
        manifest = build_manifest(5, "cell01", "retiring")
        active, migration = source_specs(configured, manifest)

        self.assertNotIn(r"flipbench_fence\.bench_table_01_p_retiring", active.config["table.include.list"])
        self.assertIn(r"flipbench_fence\.bench_table_01_p_retiring", migration.config["table.include.list"])
        self.assertEqual(migration.config["exactly.once.support"], "required")
        self.assertEqual(migration.config["transaction.boundary"], "poll")
        self.assertEqual(migration.config["transforms"], "markFence,routeFence,route")
        self.assertEqual(
            migration.config["transforms.markFence.type"],
            "org.apache.kafka.connect.transforms.InsertHeader",
        )
        self.assertEqual(migration.config["transforms.markFence.header"], FENCE_HEADER_NAME)
        self.assertEqual(migration.config["transforms.markFence.value.literal"], FENCE_HEADER_VALUE)
        self.assertEqual(migration.config["transforms.markFence.predicate"], "isFenceTopic")
        self.assertEqual(
            migration.config["predicates.isFenceTopic.type"],
            "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
        )
        self.assertNotIn(".*", migration.config["predicates.isFenceTopic.pattern"])
        self.assertEqual(
            migration.config["transforms.routeFence.replacement"],
            "cards.cell01.migration.public.$1",
        )

    def test_sink_routes_markers_to_control_receipts_before_business_routing(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        config = shared_sink_config(settings(), manifest)

        self.assertEqual(config["consumer.override.isolation.level"], "read_committed")
        self.assertEqual(config["transforms"], "routeFence,route")
        self.assertEqual(
            config["predicates.isFenceRecord.type"],
            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
        )
        self.assertEqual(config["predicates.isFenceRecord.name"], FENCE_HEADER_NAME)
        self.assertEqual(config["transforms.routeFence.predicate"], "isFenceRecord")
        self.assertEqual(
            config["transforms.routeFence.replacement"],
            "public.flipbench_fence_receipts",
        )

    def test_isolated_topology_keeps_canonical_leaf_topics_and_unique_heartbeats(self) -> None:
        configured = settings()
        object.__setattr__(configured, "source_topology", "isolated")
        manifest = build_manifest(5, "cell01", "retiring")
        topics = topic_names(manifest, configured)

        self.assertIn("cards.cell01.public.bench_table_01_p_active", topics)
        self.assertIn("cards.cell01.public.bench_table_01_p_retiring", topics)
        self.assertIn("__debezium-heartbeat.cards.cell01.active", topics)
        self.assertIn("__debezium-heartbeat.cards.cell01.migration", topics)
        self.assertEqual(len(topics), len(set(topics)))


class SettingsTests(unittest.TestCase):
    def test_reads_and_validates_source_topology(self) -> None:
        environment = {
            "HOT_DSN": "postgresql://hot/cards",
            "WARM_DSN": "postgresql://warm/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://source:8083",
            "SINK_CONNECT_URL": "http://sink:8083",
            "POSTGRES_PASSWORD": "test-only",
            "SOURCE_TOPOLOGY": "isolated",
        }
        with patch.dict(os.environ, environment, clear=True):
            configured = Settings.from_env(5)
        self.assertEqual(configured.source_topology, "isolated")

        environment["SOURCE_TOPOLOGY"] = "per-table"
        with patch.dict(os.environ, environment, clear=True), self.assertRaisesRegex(
            ManifestError, "SOURCE_TOPOLOGY"
        ):
            Settings.from_env(5)

    def test_connector_passwords_are_distinct_from_admin_and_each_other(self) -> None:
        configured = settings()
        self.assertNotEqual(configured.source_database_password, configured.postgres_password)
        self.assertNotEqual(configured.sink_database_password, configured.postgres_password)
        self.assertNotEqual(configured.source_database_password, configured.sink_database_password)

    def test_role_bootstrap_accepts_separate_source_and_sink_passwords(self) -> None:
        module = ast.parse(Path("src/flipbench/postgres_io.py").read_text(encoding="utf-8"))
        function = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == "_ensure_connector_roles"
        )
        parameters = {argument.arg for argument in function.args.args}
        self.assertIn("source_password", parameters)
        self.assertIn("sink_password", parameters)

        login_function = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == "_ensure_login_role"
        )
        login_parameters = {argument.arg for argument in login_function.args.args}
        self.assertEqual(login_parameters, {"connection", "role", "password"})

    def test_rejects_explicit_connector_password_reuse(self) -> None:
        environment = {
            "HOT_DSN": "postgresql://hot/cards",
            "WARM_DSN": "postgresql://warm/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://source:8083",
            "SINK_CONNECT_URL": "http://sink:8083",
            "POSTGRES_PASSWORD": "admin-test-only",
            "CDC_PASSWORD": "shared-test-only",
            "SINK_PASSWORD": "shared-test-only",
        }
        with patch.dict(os.environ, environment, clear=True), self.assertRaisesRegex(
            ManifestError, "must be distinct"
        ):
            Settings.from_env(5)

    def test_allows_source_and_sink_connectors_on_one_distributed_cluster(self) -> None:
        environment = {
            "HOT_DSN": "postgresql://hot/cards",
            "WARM_DSN": "postgresql://warm/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://connect:8083",
            "SINK_CONNECT_URL": "http://connect:8083",
            "POSTGRES_PASSWORD": "test-only",
        }
        with patch.dict(os.environ, environment, clear=True):
            configured = Settings.from_env(5)
        self.assertEqual(configured.source_connect_url, configured.sink_connect_url)

    def test_reads_shared_distributed_connect_worker_settings(self) -> None:
        environment = {
            "HOT_DSN": "postgresql://hot/cards",
            "WARM_DSN": "postgresql://warm/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://source:8083",
            "SINK_CONNECT_URL": "http://sink:8083",
            "POSTGRES_PASSWORD": "test-only",
            "CONNECT_OFFSET_FLUSH_INTERVAL_MS": "250",
            "CONNECT_OFFSET_FLUSH_TIMEOUT_MS": "4000",
            "CONNECT_INTERNAL_TOPIC_REPLICATION_FACTOR": "1",
            "CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS": "30000",
            "CONNECT_SESSION_TIMEOUT_MS": "10000",
            "CONNECT_HEARTBEAT_INTERVAL_MS": "3000",
            "CONNECT_WORKER_HEAP_OPTS": "-Xms256M -Xmx768M",
            "KAFKA_BROKER_HEAP_OPTS": "-Xms256M -Xmx512M",
            "SINK_TASKS_MAX": "2",
            "SINK_BATCH_SIZE": "750",
            "SINK_POOL_MIN_SIZE": "2",
            "SINK_POOL_MAX_SIZE": "4",
            "KAFKA_TOPIC_REPLICATION_FACTOR": "3",
            "KAFKA_MIN_INSYNC_REPLICAS": "2",
        }
        with patch.dict(os.environ, environment, clear=True):
            configured = Settings.from_env(5)
        self.assertEqual(configured.connect_offset_flush_interval_ms, 250)
        self.assertEqual(configured.connect_offset_flush_timeout_ms, 4000)
        self.assertEqual(configured.connect_internal_topic_replication_factor, 1)
        self.assertEqual(configured.connect_scheduled_rebalance_max_delay_ms, 30000)
        self.assertEqual(configured.connect_session_timeout_ms, 10000)
        self.assertEqual(configured.connect_heartbeat_interval_ms, 3000)
        self.assertEqual(configured.connect_worker_heap_opts, "-Xms256M -Xmx768M")
        self.assertEqual(configured.kafka_broker_heap_opts, "-Xms256M -Xmx512M")
        self.assertEqual(configured.sink_tasks_max, 2)
        self.assertEqual(configured.sink_batch_size, 750)
        self.assertEqual(configured.sink_pool_min_size, 2)
        self.assertEqual(configured.sink_pool_max_size, 4)
        self.assertEqual(configured.kafka_topic_replication_factor, 3)
        self.assertEqual(configured.kafka_min_insync_replicas, 2)

    def test_rejects_non_positive_connect_worker_offset_settings(self) -> None:
        environment = {
            "HOT_DSN": "postgresql://hot/cards",
            "WARM_DSN": "postgresql://warm/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://source:8083",
            "SINK_CONNECT_URL": "http://sink:8083",
            "POSTGRES_PASSWORD": "test-only",
            "CONNECT_OFFSET_FLUSH_INTERVAL_MS": "0",
        }
        with patch.dict(os.environ, environment, clear=True), self.assertRaises(ManifestError):
            Settings.from_env(5)

    def test_rejects_invalid_sink_pool_and_kafka_durability_settings(self) -> None:
        base = {
            "HOT_DSN": "postgresql://hot/cards",
            "WARM_DSN": "postgresql://warm/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://connect:8083",
            "SINK_CONNECT_URL": "http://connect:8083",
            "POSTGRES_PASSWORD": "test-only",
        }
        with patch.dict(
            os.environ,
            {**base, "SINK_POOL_MIN_SIZE": "4", "SINK_POOL_MAX_SIZE": "2"},
            clear=True,
        ), self.assertRaises(ManifestError):
            Settings.from_env(5)
        with patch.dict(
            os.environ,
            {**base, "KAFKA_TOPIC_REPLICATION_FACTOR": "2", "KAFKA_MIN_INSYNC_REPLICAS": "3"},
            clear=True,
        ), self.assertRaises(ManifestError):
            Settings.from_env(5)
        with patch.dict(
            os.environ,
            {**base, "CONNECT_SESSION_TIMEOUT_MS": "3000", "CONNECT_HEARTBEAT_INTERVAL_MS": "3000"},
            clear=True,
        ), self.assertRaises(ManifestError):
            Settings.from_env(5)

    def test_rejects_same_hot_and_warm_endpoint(self) -> None:
        environment = {
            "HOT_DSN": "postgresql://same/cards",
            "WARM_DSN": "postgresql://same/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://source:8083",
            "SINK_CONNECT_URL": "http://sink:8083",
            "POSTGRES_PASSWORD": "test-only",
        }
        with patch.dict(os.environ, environment, clear=True), self.assertRaises(ManifestError):
            Settings.from_env(5)

    def test_rejects_example_password(self) -> None:
        environment = {
            "HOT_DSN": "postgresql://hot/cards",
            "WARM_DSN": "postgresql://warm/cards",
            "KAFKA_BOOTSTRAP": "kafka:19092",
            "SOURCE_CONNECT_URL": "http://source:8083",
            "SINK_CONNECT_URL": "http://sink:8083",
            "POSTGRES_PASSWORD": "replace-with-local-only-password",
        }
        with patch.dict(os.environ, environment, clear=True), self.assertRaises(ManifestError):
            Settings.from_env(5)


if __name__ == "__main__":
    unittest.main()
