from __future__ import annotations

from .connect_api import ConnectClient
from .connector_configs import (
    JDBC_CONNECTOR,
    POSTGRES_CONNECTOR,
    shared_sink_config,
    source_specs,
    topic_names,
)
from .core import BenchmarkManifest, build_manifest
from .kafka_io import KafkaControl, TopicSpec
from .postgres_io import bootstrap_databases
from .settings import Settings


def all_topic_specs(manifest: BenchmarkManifest, settings: Settings) -> tuple[TopicSpec, ...]:
    return tuple(
        TopicSpec(
            name,
            replication_factor=settings.kafka_topic_replication_factor,
            min_insync_replicas=settings.kafka_min_insync_replicas,
        )
        for name in topic_names(manifest, settings)
    )


def bootstrap(settings: Settings) -> BenchmarkManifest:
    manifest = build_manifest(settings.table_count, settings.cell, settings.timeslot)
    source = ConnectClient(settings.source_connect_url)
    sink = ConnectClient(settings.sink_connect_url)
    sources = source_specs(settings, manifest)
    bootstrap_databases(
        settings.hot_dsn,
        settings.warm_dsn,
        manifest,
        sources,
        settings.source_database_password,
        settings.sink_database_password,
        settings.source_database_user,
        settings.sink_database_user,
        settings.writer_database_user,
        settings.writer_database_password,
    )
    KafkaControl(settings.kafka_bootstrap).ensure_topics(all_topic_specs(manifest, settings))

    source_plugins = source.plugins()
    sink_plugins = sink.plugins()
    if POSTGRES_CONNECTOR not in source_plugins:
        raise RuntimeError(f"source worker does not contain {POSTGRES_CONNECTOR}; plugins={source_plugins}")
    if JDBC_CONNECTOR not in sink_plugins:
        raise RuntimeError(f"sink worker does not contain {JDBC_CONNECTOR}; plugins={sink_plugins}")

    for spec in sources:
        source.put_config(spec.connector_name, dict(spec.config))
    for spec in sources:
        source.wait_state(spec.connector_name, "RUNNING", 90)
    sink_connector = manifest.tables[0].sink_connector
    sink.put_config(sink_connector, shared_sink_config(settings, manifest))
    sink.wait_state(sink_connector, "RUNNING", 90)
    return manifest
