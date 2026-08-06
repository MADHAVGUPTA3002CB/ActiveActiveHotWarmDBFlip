from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from .core import BenchmarkManifest
from .settings import Settings


POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector"
JDBC_CONNECTOR = "io.debezium.connector.jdbc.JdbcSinkConnector"
FENCE_SCHEMA = "flipbench_fence"
FENCE_RECEIPT_TABLE = "flipbench_fence_receipts"
FENCE_HEADER_NAME = "flipbench-control"
FENCE_HEADER_VALUE = "leaf-fence-v1"

# H-DD-Prod generation-pinned connector lanes. Each lane is a persistent connector,
# slot, and publication; a generation is pinned to exactly one lane for life, and
# publication membership (not the include pattern) decides which lane emits a leaf.
LANE_NAMES = ("lane_a", "lane_b")
_GENERATION_SUFFIX_REGEX = r"g[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}"
_GENERATION_LEAF_REGEX = rf"bench_table_[0-9]{{2}}_p_{_GENERATION_SUFFIX_REGEX}"


@dataclass(frozen=True, slots=True)
class SourceConnectorSpec:
    lane: str
    connector_name: str
    slot_name: str
    publication_name: str
    topic_prefix: str
    heartbeat_table: str
    captured_timeslots: tuple[str, ...]
    config: Mapping[str, str]


def active_topic(manifest: BenchmarkManifest, parent: str) -> str:
    return f"{manifest.topic_prefix}.public.{parent}_p_active"


def topic_names(
    manifest: BenchmarkManifest, settings: Settings | None = None
) -> tuple[str, ...]:
    data_topics = tuple(
        topic
        for route in manifest.tables
        for topic in (route.topic, active_topic(manifest, route.parent))
    )
    if settings is None or settings.source_topology == "shared":
        heartbeat_topics = (
            f"{manifest.topic_prefix}.public.dbz_heartbeat",
            f"__debezium-heartbeat.{manifest.topic_prefix}",
        )
    elif settings.source_topology == "lanes":
        return tuple(
            topic
            for lane in LANE_NAMES
            for topic in (
                f"{manifest.topic_prefix}.public.dbz_heartbeat_{lane}",
                f"__debezium-heartbeat.{manifest.topic_prefix}.{lane}",
            )
        )
    else:
        heartbeat_topics = tuple(
            topic
            for lane in ("active", "migration")
            for topic in (
                f"{manifest.topic_prefix}.public.dbz_heartbeat_{lane}",
                f"__debezium-heartbeat.{manifest.topic_prefix}.{lane}",
            )
        )
    return data_topics + heartbeat_topics


def source_config(settings: Settings, manifest: BenchmarkManifest) -> dict[str, str]:
    """Return the fence-source config; retained for shared-topology callers."""
    return dict(fence_source_spec(settings, manifest).config)


def _source_spec(
    settings: Settings,
    manifest: BenchmarkManifest,
    *,
    lane: str,
    connector_name: str,
    slot_name: str,
    publication_name: str,
    topic_prefix: str,
    heartbeat_table: str,
    captured_timeslots: tuple[str, ...],
) -> SourceConnectorSpec:
    captured_tables = [rf"public\.{heartbeat_table}"]
    captures_retiring = manifest.timeslot in captured_timeslots
    for route in manifest.tables:
        if captures_retiring:
            captured_tables.append(rf"public\.{route.leaf}")
            captured_tables.append(rf"{FENCE_SCHEMA}\.{route.leaf}")
        if "active" in captured_timeslots:
            captured_tables.append(rf"public\.{route.parent}_p_active")
    config = {
        "connector.class": POSTGRES_CONNECTOR,
        "tasks.max": "1",
        "topic.prefix": topic_prefix,
        "database.hostname": "hot",
        "database.port": "5432",
        "database.user": settings.source_database_user,
        "database.password": settings.source_database_password,
        "database.dbname": "cards",
        "database.sslmode": "disable",
        "plugin.name": "pgoutput",
        "slot.name": slot_name,
        "publication.name": publication_name,
        "publication.autocreate.mode": "disabled",
        "publish.via.partition.root": "false",
        "snapshot.mode": "no_data",
        "table.include.list": ",".join(captured_tables),
        "include.schema.changes": "false",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": "250",
        "heartbeat.action.query": f"UPDATE public.{heartbeat_table} SET touched_at = clock_timestamp() WHERE id = 1",
        "poll.interval.ms": "100",
        "lsn.flush.mode": "connector",
        "decimal.handling.mode": "precise",
        "producer.override.acks": "all",
        "producer.override.enable.idempotence": "true",
        "errors.tolerance": "none",
        "exactly.once.support": "required",
        "transaction.boundary": "poll",
    }
    transforms: list[str] = []
    if captures_retiring:
        leaves = "|".join(re.escape(route.leaf) for route in manifest.tables)
        marker_topic_pattern = rf"^{re.escape(topic_prefix)}\.{FENCE_SCHEMA}\.({leaves})$"
        transforms.extend(("markFence", "routeFence"))
        config.update(
            {
                "predicates": "isFenceTopic",
                "predicates.isFenceTopic.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
                "predicates.isFenceTopic.pattern": marker_topic_pattern,
                "transforms.markFence.type": "org.apache.kafka.connect.transforms.InsertHeader",
                "transforms.markFence.predicate": "isFenceTopic",
                "transforms.markFence.header": FENCE_HEADER_NAME,
                "transforms.markFence.value.literal": FENCE_HEADER_VALUE,
                "transforms.routeFence.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.routeFence.predicate": "isFenceTopic",
                "transforms.routeFence.regex": marker_topic_pattern,
                "transforms.routeFence.replacement": f"{topic_prefix}.public.$1",
            }
        )
    if topic_prefix != manifest.topic_prefix:
        transforms.append("route")
        config.update(
            {
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": rf"^{re.escape(topic_prefix)}\.public\.(.+)$",
                "transforms.route.replacement": f"{manifest.topic_prefix}.public.$1",
            }
        )
    if transforms:
        config["transforms"] = ",".join(transforms)
    return SourceConnectorSpec(
        lane,
        connector_name,
        slot_name,
        publication_name,
        topic_prefix,
        heartbeat_table,
        captured_timeslots,
        MappingProxyType(config),
    )


def _lane_source_spec(
    settings: Settings,
    manifest: BenchmarkManifest,
    lane: str,
) -> SourceConnectorSpec:
    if lane not in LANE_NAMES:
        raise ValueError(f"unknown connector lane: {lane!r}")
    topic_prefix = f"{manifest.topic_prefix}.{lane}"
    heartbeat_table = f"dbz_heartbeat_{lane}"
    marker_topic_pattern = (
        rf"^{re.escape(topic_prefix)}\.{FENCE_SCHEMA}\.({_GENERATION_LEAF_REGEX})$"
    )
    config = {
        "connector.class": POSTGRES_CONNECTOR,
        "tasks.max": "1",
        "topic.prefix": topic_prefix,
        "database.hostname": "hot",
        "database.port": "5432",
        "database.user": settings.source_database_user,
        "database.password": settings.source_database_password,
        "database.dbname": "cards",
        "database.sslmode": "disable",
        "plugin.name": "pgoutput",
        "slot.name": f"{settings.slot_name}_{lane}",
        "publication.name": f"{settings.publication_name}_{lane}",
        "publication.autocreate.mode": "disabled",
        "publish.via.partition.root": "false",
        "snapshot.mode": "no_data",
        # Generation-independent patterns: creating a new generation must never
        # require a connector change. The lane publication is the authoritative
        # filter; these patterns are a superset of every generation's relations.
        "table.include.list": ",".join(
            (
                rf"public\.{_GENERATION_LEAF_REGEX}",
                rf"{FENCE_SCHEMA}\.{_GENERATION_LEAF_REGEX}",
                rf"public\.{heartbeat_table}",
            )
        ),
        "include.schema.changes": "false",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": "250",
        "heartbeat.action.query": (
            f"UPDATE public.{heartbeat_table} SET touched_at = clock_timestamp() WHERE id = 1"
        ),
        "poll.interval.ms": "100",
        "lsn.flush.mode": "connector",
        "decimal.handling.mode": "precise",
        "producer.override.acks": "all",
        "producer.override.enable.idempotence": "true",
        "errors.tolerance": "none",
        "exactly.once.support": "required",
        "transaction.boundary": "poll",
        "predicates": "isFenceTopic",
        "predicates.isFenceTopic.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
        "predicates.isFenceTopic.pattern": marker_topic_pattern,
        "transforms": "markFence,routeFence,route",
        "transforms.markFence.type": "org.apache.kafka.connect.transforms.InsertHeader",
        "transforms.markFence.predicate": "isFenceTopic",
        "transforms.markFence.header": FENCE_HEADER_NAME,
        "transforms.markFence.value.literal": FENCE_HEADER_VALUE,
        "transforms.routeFence.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.routeFence.predicate": "isFenceTopic",
        "transforms.routeFence.regex": marker_topic_pattern,
        "transforms.routeFence.replacement": f"{manifest.topic_prefix}.public.$1",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": rf"^{re.escape(topic_prefix)}\.public\.(.+)$",
        "transforms.route.replacement": f"{manifest.topic_prefix}.public.$1",
    }
    return SourceConnectorSpec(
        lane,
        f"{settings.source_connector}-{lane.replace('_', '-')}",
        f"{settings.slot_name}_{lane}",
        f"{settings.publication_name}_{lane}",
        topic_prefix,
        heartbeat_table,
        (),
        MappingProxyType(config),
    )


def lane_source_specs(
    settings: Settings, manifest: BenchmarkManifest
) -> tuple[SourceConnectorSpec, ...]:
    return tuple(_lane_source_spec(settings, manifest, lane) for lane in LANE_NAMES)


def generation_leaf_topic_regex(manifest: BenchmarkManifest) -> str:
    return rf"^{re.escape(manifest.topic_prefix)}\.public\.{_GENERATION_LEAF_REGEX}$"


def lanes_sink_config(settings: Settings, manifest: BenchmarkManifest) -> dict[str, str]:
    """Generation-independent sink: one config covers every rolling generation."""
    router_regex = (
        rf"^{re.escape(manifest.topic_prefix)}\.public\."
        rf"(bench_table_[0-9]{{2}})_p_{_GENERATION_SUFFIX_REGEX}$"
    )
    config = shared_sink_config(settings, manifest)
    config.update(
        {
            "topics.regex": generation_leaf_topic_regex(manifest),
            "transforms.routeFence.regex": router_regex,
            "transforms.route.regex": router_regex,
            # New generation topics must be discovered quickly without a restart.
            "consumer.override.metadata.max.age.ms": "5000",
        }
    )
    return config


def source_specs(settings: Settings, manifest: BenchmarkManifest) -> tuple[SourceConnectorSpec, ...]:
    if settings.source_topology == "lanes":
        return lane_source_specs(settings, manifest)
    if settings.source_topology == "shared":
        return (
            _source_spec(
                settings,
                manifest,
                lane="shared",
                connector_name=settings.source_connector,
                slot_name=settings.slot_name,
                publication_name=settings.publication_name,
                topic_prefix=manifest.topic_prefix,
                heartbeat_table="dbz_heartbeat",
                captured_timeslots=("active", manifest.timeslot),
            ),
        )
    if settings.source_topology != "isolated":
        from .core import ManifestError

        raise ManifestError("SOURCE_TOPOLOGY must be shared or isolated")
    return (
        _source_spec(
            settings,
            manifest,
            lane="active",
            connector_name=f"{settings.source_connector}-active",
            slot_name=f"{settings.slot_name}_active",
            publication_name=f"{settings.publication_name}_active",
            topic_prefix=f"{manifest.topic_prefix}.active",
            heartbeat_table="dbz_heartbeat_active",
            captured_timeslots=("active",),
        ),
        _source_spec(
            settings,
            manifest,
            lane="migration",
            connector_name=f"{settings.source_connector}-migration",
            slot_name=f"{settings.slot_name}_migration",
            publication_name=f"{settings.publication_name}_migration",
            topic_prefix=f"{manifest.topic_prefix}.migration",
            heartbeat_table="dbz_heartbeat_migration",
            captured_timeslots=(manifest.timeslot,),
        ),
    )


def fence_source_spec(settings: Settings, manifest: BenchmarkManifest) -> SourceConnectorSpec:
    specs = source_specs(settings, manifest)
    return next((spec for spec in specs if spec.lane == "migration"), specs[0])


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
        "predicates": "isFenceRecord",
        "predicates.isFenceRecord.type": "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
        "predicates.isFenceRecord.name": FENCE_HEADER_NAME,
        "transforms": "routeFence,route",
        "transforms.routeFence.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.routeFence.predicate": "isFenceRecord",
        "transforms.routeFence.regex": _leaf_topic_router_regex(manifest),
        "transforms.routeFence.replacement": f"public.{FENCE_RECEIPT_TABLE}",
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
        "consumer.override.isolation.level": "read_committed",
    }


def sink_config(settings: Settings, manifest: BenchmarkManifest, index: int | None = None) -> dict[str, str]:
    """Compatibility wrapper for callers migrating from per-table sink configs."""
    return shared_sink_config(settings, manifest)
