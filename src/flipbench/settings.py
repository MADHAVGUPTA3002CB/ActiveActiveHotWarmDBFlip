from __future__ import annotations

import os
import hashlib
import hmac
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

from .core import ManifestError


@dataclass(frozen=True, slots=True)
class Settings:
    hot_dsn: str
    warm_dsn: str
    kafka_bootstrap: str
    source_connect_url: str
    sink_connect_url: str
    postgres_password: str = field(repr=False)
    table_count: int
    cdc_password: str | None = field(default=None, repr=False)
    sink_password: str | None = field(default=None, repr=False)
    cell: str = "cell01"
    timeslot: str = "retiring"
    slot_name: str = "flipbench_slot"
    publication_name: str = "flipbench_pub"
    source_connector: str = "flipbench-source"
    source_topology: str = "shared"
    source_database_user: str = "flipbench_cdc"
    sink_database_user: str = "flipbench_sink"
    writer_database_user: str = "flipbench_writer"
    results_dir: Path = Path("results")
    connect_offset_flush_interval_ms: int = 1000
    connect_offset_flush_timeout_ms: int = 5000
    connect_internal_topic_replication_factor: int = 1
    connect_scheduled_rebalance_max_delay_ms: int = 30000
    connect_session_timeout_ms: int = 10000
    connect_heartbeat_interval_ms: int = 3000
    connect_worker_heap_opts: str = "-Xms256M -Xmx768M"
    kafka_broker_heap_opts: str = "-Xms256M -Xmx512M"
    sink_tasks_max: int = 1
    sink_batch_size: int = 500
    sink_pool_min_size: int = 1
    sink_pool_max_size: int = 4
    kafka_topic_replication_factor: int = 1
    kafka_min_insync_replicas: int = 1

    @staticmethod
    def _local_role_password(admin_password: str, role: str) -> str:
        return hmac.new(
            admin_password.encode("utf-8"),
            f"flipbench-local-{role}-v1".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    @property
    def source_database_password(self) -> str:
        return self.cdc_password or self._local_role_password(
            self.postgres_password, self.source_database_user
        )

    @property
    def sink_database_password(self) -> str:
        return self.sink_password or self._local_role_password(
            self.postgres_password, self.sink_database_user
        )

    @property
    def writer_database_password(self) -> str:
        return self._local_role_password(self.postgres_password, self.writer_database_user)

    @property
    def writer_hot_dsn(self) -> str:
        parsed = urlsplit(self.hot_dsn)
        if parsed.scheme not in ("postgres", "postgresql") or parsed.hostname is None:
            raise ManifestError("HOT_DSN must be a PostgreSQL URI to derive the local writer DSN")
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = "" if parsed.port is None else f":{parsed.port}"
        credentials = (
            f"{quote(self.writer_database_user, safe='')}:{quote(self.writer_database_password, safe='')}@"
        )
        return urlunsplit(
            (parsed.scheme, f"{credentials}{host}{port}", parsed.path, parsed.query, parsed.fragment)
        )

    @classmethod
    def from_env(cls, table_count: int | None = None) -> "Settings":
        required = {
            "HOT_DSN": os.environ.get("HOT_DSN"),
            "WARM_DSN": os.environ.get("WARM_DSN"),
            "KAFKA_BOOTSTRAP": os.environ.get("KAFKA_BOOTSTRAP"),
            "SOURCE_CONNECT_URL": os.environ.get("SOURCE_CONNECT_URL"),
            "SINK_CONNECT_URL": os.environ.get("SINK_CONNECT_URL"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD"),
        }
        missing = tuple(name for name, value in required.items() if not value)
        if missing:
            raise ManifestError(f"missing required environment variables: {', '.join(missing)}")
        if required["HOT_DSN"] == required["WARM_DSN"]:
            raise ManifestError("HOT_DSN and WARM_DSN must identify different PostgreSQL instances")
        if required["POSTGRES_PASSWORD"] == "replace-with-local-only-password":
            raise ManifestError("replace the example POSTGRES_PASSWORD before starting")
        resolved_count = table_count if table_count is not None else int(os.environ.get("TABLE_COUNT", "5"))
        if resolved_count not in (5, 10, 15, 20):
            raise ManifestError("TABLE_COUNT must be one of 5, 10, 15, or 20")
        source_topology = os.environ.get("SOURCE_TOPOLOGY", "shared")
        if source_topology not in ("shared", "isolated", "lanes"):
            raise ManifestError("SOURCE_TOPOLOGY must be shared, isolated, or lanes")

        def positive_int(name: str, default: int) -> int:
            try:
                value = int(os.environ.get(name, str(default)))
            except ValueError as error:
                raise ManifestError(f"{name} must be a positive integer") from error
            if value <= 0:
                raise ManifestError(f"{name} must be a positive integer")
            return value

        sink_pool_min = positive_int("SINK_POOL_MIN_SIZE", 1)
        sink_pool_max = positive_int("SINK_POOL_MAX_SIZE", 4)
        if sink_pool_max < sink_pool_min:
            raise ManifestError("SINK_POOL_MAX_SIZE must be greater than or equal to SINK_POOL_MIN_SIZE")
        topic_replication = positive_int("KAFKA_TOPIC_REPLICATION_FACTOR", 1)
        min_insync = positive_int("KAFKA_MIN_INSYNC_REPLICAS", 1)
        if min_insync > topic_replication:
            raise ManifestError("KAFKA_MIN_INSYNC_REPLICAS cannot exceed topic replication factor")
        connect_session_timeout = positive_int("CONNECT_SESSION_TIMEOUT_MS", 10000)
        connect_heartbeat_interval = positive_int("CONNECT_HEARTBEAT_INTERVAL_MS", 3000)
        if connect_heartbeat_interval >= connect_session_timeout:
            raise ManifestError("CONNECT_HEARTBEAT_INTERVAL_MS must be less than CONNECT_SESSION_TIMEOUT_MS")
        configured = cls(
            hot_dsn=str(required["HOT_DSN"]),
            warm_dsn=str(required["WARM_DSN"]),
            kafka_bootstrap=str(required["KAFKA_BOOTSTRAP"]),
            source_connect_url=str(required["SOURCE_CONNECT_URL"]).rstrip("/"),
            sink_connect_url=str(required["SINK_CONNECT_URL"]).rstrip("/"),
            postgres_password=str(required["POSTGRES_PASSWORD"]),
            table_count=resolved_count,
            cdc_password=os.environ.get("CDC_PASSWORD") or None,
            sink_password=os.environ.get("SINK_PASSWORD") or None,
            results_dir=Path(os.environ.get("RESULTS_DIR", "results")),
            source_topology=source_topology,
            connect_offset_flush_interval_ms=positive_int("CONNECT_OFFSET_FLUSH_INTERVAL_MS", 1000),
            connect_offset_flush_timeout_ms=positive_int("CONNECT_OFFSET_FLUSH_TIMEOUT_MS", 5000),
            connect_internal_topic_replication_factor=positive_int(
                "CONNECT_INTERNAL_TOPIC_REPLICATION_FACTOR", 1
            ),
            connect_scheduled_rebalance_max_delay_ms=positive_int(
                "CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS", 30000
            ),
            connect_session_timeout_ms=connect_session_timeout,
            connect_heartbeat_interval_ms=connect_heartbeat_interval,
            connect_worker_heap_opts=os.environ.get(
                "CONNECT_WORKER_HEAP_OPTS", "-Xms256M -Xmx768M"
            ),
            kafka_broker_heap_opts=os.environ.get(
                "KAFKA_BROKER_HEAP_OPTS", "-Xms256M -Xmx512M"
            ),
            sink_tasks_max=positive_int("SINK_TASKS_MAX", 1),
            sink_batch_size=positive_int("SINK_BATCH_SIZE", 500),
            sink_pool_min_size=sink_pool_min,
            sink_pool_max_size=sink_pool_max,
            kafka_topic_replication_factor=topic_replication,
            kafka_min_insync_replicas=min_insync,
        )
        passwords = {
            configured.postgres_password,
            configured.source_database_password,
            configured.sink_database_password,
            configured.writer_database_password,
        }
        if len(passwords) != 4:
            raise ManifestError(
                "POSTGRES_PASSWORD and derived CDC, sink and writer passwords must be distinct"
            )
        return configured
