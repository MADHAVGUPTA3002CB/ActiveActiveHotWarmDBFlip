from __future__ import annotations

import re

from .core import BenchmarkManifest
from .settings import Settings


POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector"
JDBC_CONNECTOR = "io.debezium.connector.jdbc.JdbcSinkConnector"


def active_topic(manifest: BenchmarkManifest, parent: str) -> str:
    return f"{manifest.topic_prefix}.public.{parent}_p_active"


def topic_names(manifest: BenchmarkManifest) -> tuple[str, ...]:
    data_topics = tuple(
        topic
        for route in manifest.tables
        for topic in (route.topic, active_topic(manifest, route.parent))
    )
    return data_topics + (
        f"{manifest.topic_prefix}.public.dbz_heartbeat",
        f"__debezium-heartbeat.{manifest.topic_prefix}",
    )


def source_config(settings: Settings, manifest: BenchmarkManifest) -> dict[str, str]:
    captured_tables = [r"public\.dbz_heartbeat"]
    captured_tables.extend(rf"public\.{route.leaf}" for route in manifest.tables)
    captured_tables.extend(rf"public\.{route.parent}_p_active" for route in manifest.tables)
    return {
        "connector.class": POSTGRES_CONNECTOR,
        "tasks.max": "1",
        "topic.prefix": manifest.topic_prefix,
        "database.hostname": "hot",
        "database.port": "5432",
        "database.user": settings.source_database_user,
        "database.password": settings.source_database_password,
        "database.dbname": "cards",
        "database.sslmode": "disable",
        "plugin.name": "pgoutput",
        "slot.name": settings.slot_name,
        "publication.name": settings.publication_name,
        "publication.autocreate.mode": "disabled",
        "publish.via.partition.root": "false",
        "snapshot.mode": "no_data",
        "table.include.list": ",".join(captured_tables),
        "include.schema.changes": "false",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": "250",
        "heartbeat.action.query": "UPDATE public.dbz_heartbeat SET touched_at = clock_timestamp() WHERE id = 1",
        "poll.interval.ms": "100",
        "lsn.flush.mode": "connector",
        "decimal.handling.mode": "precise",
        "producer.override.acks": "all",
        "producer.override.enable.idempotence": "true",
        "errors.tolerance": "none",
    }


def _leaf_topic_regex(manifest: BenchmarkManifest) -> str:
    parents = "|".join(re.escape(route.parent) for route in manifest.tables)
    return rf"^{re.escape(manifest.topic_prefix)}\.public\.(?:{parents})_p_({re.escape(manifest.timeslot)}|active)$"


def _leaf_topic_router_regex(manifest: BenchmarkManifest) -> str:
    parents = "|".join(re.escape(route.parent) for route in manifest.tables)
    return rf"^{re.escape(manifest.topic_prefix)}\.public\.({parents})_p_({re.escape(manifest.timeslot)}|active)$"


def warm_collection_for_topic(manifest: BenchmarkManifest, topic: str) -> str:
    match = re.fullmatch(_leaf_topic_router_regex(manifest), topic)
    allowed = {route.parent for route in manifest.tables}
    if match is None or match.group(1) not in allowed:
        from .core import ManifestError

        raise ManifestError(f"topic does not map to an allowlisted warm table: {topic!r}")
    return f"public.{match.group(1)}"


def shared_sink_config(settings: Settings, manifest: BenchmarkManifest) -> dict[str, str]:
    return {
        "connector.class": JDBC_CONNECTOR,
        "tasks.max": str(settings.sink_tasks_max),
        "topics.regex": _leaf_topic_regex(manifest),
        "connection.url": "jdbc:postgresql://warm:5432/cards",
        "connection.username": settings.sink_database_user,
        "connection.password": settings.sink_database_password,
        "insert.mode": "upsert",
        "primary.key.mode": "record_key",
        "delete.enabled": "false",
        "schema.evolution": "none",
        "collection.name.format": "${topic}",
        "collection.naming.strategy": "io.debezium.sink.naming.PassthroughCollectionNamingStrategy",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": _leaf_topic_router_regex(manifest),
        "transforms.route.replacement": "public.$1",
        "quote.identifiers": "true",
        "batch.size": str(settings.sink_batch_size),
        "connection.pool.min_size": str(settings.sink_pool_min_size),
        "connection.pool.max_size": str(settings.sink_pool_max_size),
        "errors.tolerance": "none",
        "errors.log.enable": "true",
        "consumer.override.auto.offset.reset": "earliest",
    }


def sink_config(settings: Settings, manifest: BenchmarkManifest, index: int | None = None) -> dict[str, str]:
    """Compatibility wrapper for callers migrating from per-table sink configs."""
    return shared_sink_config(settings, manifest)
