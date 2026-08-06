"""H-DD-Prod rolling generations on persistent connector lanes.

A generation is one 12-hour timeslot pinned to one connector lane for its complete
hot lifetime. This module owns the hot/warm DDL that makes generations dynamic:

- the window-driven rewrite of the ``flipbench_guard`` admission functions, so new
  timeslots are data (rows in ``timeslot_windows``) instead of hard-coded names;
- the one-time lane bootstrap (parents, roles, empty per-lane publications,
  heartbeat tables, lane connectors are configured by the caller);
- per-generation provisioning: leaves, marker tables, topics, publication
  membership, routes, gate, tracker row, and the end-to-end canary proof.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from psycopg import sql

from .connect_api import ConnectClient
from .connector_configs import (
    FENCE_RECEIPT_TABLE,
    FENCE_SCHEMA,
    JDBC_CONNECTOR,
    LANE_NAMES,
    POSTGRES_CONNECTOR,
    SourceConnectorSpec,
    lane_source_specs,
    lanes_sink_config,
    topic_names,
)
from .core import BenchmarkManifest, TopicPartition, build_leaf_fence_markers, build_manifest
from .kafka_io import KafkaControl, TopicSpec
from .lifecycle import is_generation_timeslot
from .postgres_io import (
    ACTIVE_END,
    _create_leaf,
    _create_warm_table,
    _ensure_login_role,
    _verify_environment_guard,
    connect,
    emit_leaf_fence_markers,
    observed_leaf_fence_receipts,
)
from .settings import Settings

GENERATION_INTERVAL = timedelta(hours=12)
GENERATION_BASE = ACTIVE_END


def generation_timeslot(index: int) -> str:
    if not isinstance(index, int) or isinstance(index, bool) or index < 0:
        raise ValueError("generation index must be a non-negative integer")
    start = GENERATION_BASE + GENERATION_INTERVAL * index
    timeslot = f"g{start:%Y_%m_%d_%H}"
    if not is_generation_timeslot(timeslot):
        raise ValueError(f"generated timeslot is not a valid generation id: {timeslot!r}")
    return timeslot


def generation_window(index: int) -> tuple[datetime, datetime]:
    start = GENERATION_BASE + GENERATION_INTERVAL * index
    return start, start + GENERATION_INTERVAL


def lane_for_generation(index: int) -> str:
    if not isinstance(index, int) or isinstance(index, bool) or index < 0:
        raise ValueError("generation index must be a non-negative integer")
    return LANE_NAMES[index % len(LANE_NAMES)]


@dataclass(frozen=True, slots=True)
class GenerationSpec:
    index: int
    timeslot: str
    lane: str
    window_start: datetime
    window_end: datetime
    manifest: BenchmarkManifest

    @classmethod
    def build(cls, settings: Settings, index: int) -> "GenerationSpec":
        timeslot = generation_timeslot(index)
        start, end = generation_window(index)
        manifest = build_manifest(settings.table_count, settings.cell, timeslot)
        return cls(index, timeslot, lane_for_generation(index), start, end, manifest)


_WINDOW_LOOKUP = """
    SELECT win.window_start, win.window_end
    INTO window_start, window_end
    FROM flipbench_guard.timeslot_windows AS win
    WHERE win.cell = p_cell AND win.timeslot = p_timeslot;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'unknown hot write timeslot';
    END IF;
"""


_GUARD_FUNCTIONS = (
    (
        "insert_events(text, text, bigint, name, jsonb)",
        """
CREATE OR REPLACE FUNCTION flipbench_guard.insert_events(
    p_cell text,
    p_timeslot text,
    p_expected_ownership_epoch bigint,
    p_parent_name name,
    p_rows jsonb
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    window_start timestamptz;
    window_end timestamptz;
    gate_state text;
    gate_epoch bigint;
    inserted_count integer;
BEGIN
    {window_lookup}
    IF jsonb_typeof(p_rows) <> 'array' OR jsonb_array_length(p_rows) < 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'guarded insert rows must be a non-empty JSON array';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_rows) AS item
        WHERE (item->>'created_at')::timestamptz < window_start
           OR (item->>'created_at')::timestamptz >= window_end
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'guarded insert row does not belong to the selected timeslot';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flipbench_guard.write_routes AS route
        WHERE route.cell = p_cell
          AND route.timeslot = p_timeslot
          AND route.parent_name = p_parent_name
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'guarded insert route is not authorized';
    END IF;

    SELECT gate.state, gate.ownership_epoch
    INTO gate_state, gate_epoch
    FROM flipbench_guard.partition_write_gates AS gate
    WHERE gate.cell = p_cell AND gate.timeslot = p_timeslot
    FOR SHARE;

    IF NOT FOUND
       OR gate_state <> 'open'
       OR gate_epoch <> p_expected_ownership_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: ownership gate is not open for the expected epoch';
    END IF;

    EXECUTE format(
        'INSERT INTO public.%I (id, experiment_run_id, sequence_no, created_at, payload) '
        'SELECT (item->>''id'')::uuid, (item->>''experiment_run_id'')::uuid, '
        '(item->>''sequence_no'')::bigint, (item->>''created_at'')::timestamptz, item->''payload'' '
        'FROM jsonb_array_elements($1) AS item',
        p_parent_name
    ) USING p_rows;
    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$
""",
    ),
    (
        "admit_optimistic_batch(text, text, bigint)",
        """
CREATE OR REPLACE FUNCTION flipbench_guard.admit_optimistic_batch(
    p_cell text,
    p_timeslot text,
    p_expected_ownership_epoch bigint
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    gate_state text;
    gate_epoch bigint;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM flipbench_guard.timeslot_windows AS win
        WHERE win.cell = p_cell AND win.timeslot = p_timeslot
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'unknown optimistic batch timeslot';
    END IF;
    SELECT gate.state, gate.ownership_epoch
    INTO gate_state, gate_epoch
    FROM flipbench_guard.partition_write_gates AS gate
    WHERE gate.cell = p_cell AND gate.timeslot = p_timeslot;

    IF NOT FOUND
       OR gate_state <> 'open'
       OR gate_epoch <> p_expected_ownership_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: optimistic batch admission rejected';
    END IF;
    RETURN gate_epoch;
END;
$$
""",
    ),
    (
        "admit_optimistic_batch_state_only(text, text)",
        """
CREATE OR REPLACE FUNCTION flipbench_guard.admit_optimistic_batch_state_only(
    p_cell text,
    p_timeslot text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    gate_state text;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM flipbench_guard.timeslot_windows AS win
        WHERE win.cell = p_cell AND win.timeslot = p_timeslot
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'unknown optimistic batch timeslot';
    END IF;
    SELECT gate.state
    INTO gate_state
    FROM flipbench_guard.partition_write_gates AS gate
    WHERE gate.cell = p_cell AND gate.timeslot = p_timeslot;

    IF NOT FOUND OR gate_state <> 'open' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: optimistic state-only batch admission rejected';
    END IF;
    RETURN true;
END;
$$
""",
    ),
    (
        "insert_events_optimistic(text, text, name, jsonb)",
        """
CREATE OR REPLACE FUNCTION flipbench_guard.insert_events_optimistic(
    p_cell text,
    p_timeslot text,
    p_parent_name name,
    p_rows jsonb
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    window_start timestamptz;
    window_end timestamptz;
    inserted_count integer;
BEGIN
    {window_lookup}
    IF jsonb_typeof(p_rows) <> 'array'
       OR jsonb_array_length(p_rows) < 1
       OR jsonb_array_length(p_rows) > 100000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic write batch must contain 1..100000 rows';
    END IF;
    IF pg_column_size(p_rows) > 67108864 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic write payload exceeds the 64 MiB safety limit';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flipbench_guard.write_routes AS route
        WHERE route.cell = p_cell
          AND route.timeslot = p_timeslot
          AND route.parent_name = p_parent_name
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'optimistic write route is not authorized';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_rows) AS row_item
        WHERE (row_item->>'created_at')::timestamptz < window_start
           OR (row_item->>'created_at')::timestamptz >= window_end
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic write row does not belong to the selected timeslot';
    END IF;

    BEGIN
        EXECUTE format(
            'INSERT INTO public.%I (id, experiment_run_id, sequence_no, created_at, payload) '
            'SELECT (item->>''id'')::uuid, (item->>''experiment_run_id'')::uuid, '
            '(item->>''sequence_no'')::bigint, (item->>''created_at'')::timestamptz, item->''payload'' '
            'FROM jsonb_array_elements($1) AS item',
            p_parent_name
        ) USING p_rows;
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
    EXCEPTION
        WHEN check_violation OR lock_not_available OR query_canceled THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: optimistic detach race aborted one batch operation';
    END;
    RETURN inserted_count;
END;
$$
""",
    ),
    (
        "update_events_optimistic(text, text, name, jsonb)",
        """
CREATE OR REPLACE FUNCTION flipbench_guard.update_events_optimistic(
    p_cell text,
    p_timeslot text,
    p_parent_name name,
    p_rows jsonb
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    window_start timestamptz;
    window_end timestamptz;
    updated_count integer;
    expected_count integer;
BEGIN
    {window_lookup}
    IF jsonb_typeof(p_rows) <> 'array'
       OR jsonb_array_length(p_rows) < 1
       OR jsonb_array_length(p_rows) > 100000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic update batch must contain 1..100000 rows';
    END IF;
    IF pg_column_size(p_rows) > 67108864 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic update payload exceeds the 64 MiB safety limit';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flipbench_guard.write_routes AS route
        WHERE route.cell = p_cell
          AND route.timeslot = p_timeslot
          AND route.parent_name = p_parent_name
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'optimistic update route is not authorized';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_rows) AS row_item
        WHERE row_item->>'id' IS NULL
           OR row_item->>'created_at' IS NULL
           OR row_item->'payload' IS NULL
           OR (row_item->>'created_at')::timestamptz < window_start
           OR (row_item->>'created_at')::timestamptz >= window_end
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic update row is invalid or outside the selected timeslot';
    END IF;

    expected_count := jsonb_array_length(p_rows);
    BEGIN
        EXECUTE format(
            'WITH input_rows AS ('
            '  SELECT (item->>''id'')::uuid AS id, '
            '         (item->>''created_at'')::timestamptz AS created_at, '
            '         item->''payload'' AS payload '
            '  FROM jsonb_array_elements($1) AS item'
            ') '
            'UPDATE public.%I AS target '
            'SET payload = input_rows.payload, updated_at = clock_timestamp() '
            'FROM input_rows '
            'WHERE target.id = input_rows.id '
            '  AND target.created_at = input_rows.created_at',
            p_parent_name
        ) USING p_rows;
        GET DIAGNOSTICS updated_count = ROW_COUNT;
    EXCEPTION
        WHEN check_violation OR lock_not_available OR query_canceled THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: optimistic detach race aborted one update operation';
    END;
    IF updated_count <> expected_count THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: optimistic update target is no longer attached';
    END IF;
    RETURN updated_count;
END;
$$
""",
    ),
    (
        "update_events(text, text, bigint, name, jsonb)",
        """
CREATE OR REPLACE FUNCTION flipbench_guard.update_events(
    p_cell text,
    p_timeslot text,
    p_expected_ownership_epoch bigint,
    p_parent_name name,
    p_rows jsonb
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    gate_state text;
    gate_epoch bigint;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM flipbench_guard.timeslot_windows AS win
        WHERE win.cell = p_cell AND win.timeslot = p_timeslot
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'unknown guarded update timeslot';
    END IF;

    SELECT gate.state, gate.ownership_epoch
    INTO gate_state, gate_epoch
    FROM flipbench_guard.partition_write_gates AS gate
    WHERE gate.cell = p_cell AND gate.timeslot = p_timeslot
    FOR SHARE;

    IF NOT FOUND
       OR gate_state <> 'open'
       OR gate_epoch <> p_expected_ownership_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: ownership gate is not open for the expected epoch';
    END IF;

    RETURN flipbench_guard.update_events_optimistic(
        p_cell,
        p_timeslot,
        p_parent_name,
        p_rows
    );
END;
$$
""",
    ),
)


def ensure_generation_guard_objects(
    hot: Any,
    cell: str,
    writer_role: str,
) -> None:
    """Make the hot admission layer timeslot-window driven (idempotent)."""
    hot.execute(
        "ALTER TABLE flipbench_guard.partition_write_gates "
        "DROP CONSTRAINT IF EXISTS partition_write_gates_timeslot_check"
    )
    hot.execute(
        "ALTER TABLE flipbench_guard.write_routes "
        "DROP CONSTRAINT IF EXISTS write_routes_timeslot_check"
    )
    hot.execute(
        """
        CREATE TABLE IF NOT EXISTS flipbench_guard.timeslot_windows (
            cell text NOT NULL,
            timeslot text NOT NULL CHECK (timeslot ~ '^[a-z][a-z0-9_]{0,62}$'),
            window_start timestamptz NOT NULL,
            window_end timestamptz NOT NULL,
            PRIMARY KEY (cell, timeslot),
            CHECK (window_start < window_end)
        )
        """
    )
    hot.execute(
        "ALTER TABLE flipbench_guard.timeslot_windows OWNER TO flipbench_guard_owner"
    )
    hot.execute("REVOKE ALL ON TABLE flipbench_guard.timeslot_windows FROM PUBLIC")
    hot.execute(
        """
        INSERT INTO flipbench_guard.timeslot_windows (cell, timeslot, window_start, window_end)
        VALUES
            (%s, 'retiring', timestamptz '2026-07-31 12:00:00+00', timestamptz '2026-08-01 00:00:00+00'),
            (%s, 'active', timestamptz '2026-08-01 00:00:00+00', timestamptz '2026-08-01 12:00:00+00')
        ON CONFLICT (cell, timeslot) DO NOTHING
        """,
        (cell, cell),
    )
    for signature, body in _GUARD_FUNCTIONS:
        hot.execute(body.format(window_lookup=_WINDOW_LOOKUP))
        hot.execute(
            sql.SQL("ALTER FUNCTION flipbench_guard.{} OWNER TO flipbench_guard_owner").format(
                sql.SQL(signature)
            )
        )
        hot.execute(
            sql.SQL("REVOKE ALL ON FUNCTION flipbench_guard.{} FROM PUBLIC").format(
                sql.SQL(signature)
            )
        )
        hot.execute(
            sql.SQL("GRANT EXECUTE ON FUNCTION flipbench_guard.{} TO {}").format(
                sql.SQL(signature), sql.Identifier(writer_role)
            )
        )


def register_timeslot_window(
    hot: Any,
    cell: str,
    timeslot: str,
    window_start: datetime,
    window_end: datetime,
) -> None:
    hot.execute(
        """
        INSERT INTO flipbench_guard.timeslot_windows (cell, timeslot, window_start, window_end)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (cell, timeslot) DO NOTHING
        """,
        (cell, timeslot, window_start, window_end),
    )
    observed = hot.execute(
        "SELECT window_start, window_end FROM flipbench_guard.timeslot_windows "
        "WHERE cell=%s AND timeslot=%s",
        (cell, timeslot),
    ).fetchone()
    if observed != (window_start, window_end):
        raise RuntimeError(f"conflicting timeslot window already registered for {timeslot}")


_MARKER_COLUMNS = sql.SQL(
    """
    marker_schema_version smallint NOT NULL CHECK (marker_schema_version = 1),
    marker_id uuid NOT NULL UNIQUE,
    attempt_id uuid NOT NULL,
    attempt_epoch bigint NOT NULL CHECK (attempt_epoch > 0),
    ownership_epoch bigint NOT NULL CHECK (ownership_epoch > 0),
    cell text NOT NULL,
    timeslot text NOT NULL,
    parent_name text NOT NULL,
    leaf_name text NOT NULL,
    emitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (attempt_id, leaf_name)
    """
)


def bootstrap_lanes_databases(settings: Settings) -> BenchmarkManifest:
    """One-time hot/warm DDL for the lanes topology: parents, roles, empty lane
    publications, heartbeat tables, fence schema, and the window-driven guard."""
    if settings.source_topology != "lanes":
        raise RuntimeError("lane bootstrap requires SOURCE_TOPOLOGY=lanes")
    base_manifest = build_manifest(settings.table_count, settings.cell, "retiring")
    specs = lane_source_specs(settings, base_manifest)
    with connect(settings.hot_dsn, autocommit=True) as hot, connect(
        settings.warm_dsn, autocommit=True
    ) as warm:
        _verify_environment_guard(hot, "hot")
        _verify_environment_guard(warm, "warm")
        existing = hot.execute(
            "SELECT to_regclass(%s)", (f"public.{base_manifest.tables[0].parent}",)
        ).fetchone()[0]
        if existing is not None:
            raise RuntimeError("prototype tables already exist; use a clean, scoped Compose volume set")
        for spec in specs:
            hot.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS public.{} (
                        id integer PRIMARY KEY,
                        touched_at timestamptz NOT NULL
                    )
                    """
                ).format(sql.Identifier(spec.heartbeat_table))
            )
            hot.execute(
                sql.SQL(
                    "INSERT INTO public.{} (id, touched_at) VALUES (1, clock_timestamp()) "
                    "ON CONFLICT (id) DO NOTHING"
                ).format(sql.Identifier(spec.heartbeat_table))
            )
        for route in base_manifest.tables:
            hot.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        id uuid NOT NULL,
                        experiment_run_id uuid NOT NULL,
                        sequence_no bigint NOT NULL,
                        created_at timestamptz NOT NULL,
                        payload jsonb NOT NULL,
                        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                        PRIMARY KEY (id, created_at)
                    ) PARTITION BY RANGE (created_at)
                    """
                ).format(sql.Identifier(route.parent))
            )
            trigger = f"{route.parent}_immutable_record_key"
            hot.execute(
                sql.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                    sql.Identifier(trigger), sql.Identifier(route.parent)
                )
            )
            hot.execute(
                sql.SQL(
                    "CREATE TRIGGER {} BEFORE UPDATE OF id, created_at ON {} "
                    "FOR EACH ROW EXECUTE FUNCTION public.reject_record_key_change()"
                ).format(sql.Identifier(trigger), sql.Identifier(route.parent))
            )
            _create_warm_table(warm, route)
        hot.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(FENCE_SCHEMA)))
        hot.execute(sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(FENCE_SCHEMA)))
        warm.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS public.{} (
                    marker_schema_version smallint NOT NULL CHECK (marker_schema_version = 1),
                    marker_id uuid NOT NULL UNIQUE,
                    attempt_id uuid NOT NULL,
                    attempt_epoch bigint NOT NULL CHECK (attempt_epoch > 0),
                    ownership_epoch bigint NOT NULL CHECK (ownership_epoch > 0),
                    cell text NOT NULL,
                    timeslot text NOT NULL,
                    parent_name text NOT NULL,
                    leaf_name text NOT NULL,
                    emitted_at timestamptz NOT NULL,
                    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                    PRIMARY KEY (attempt_id, leaf_name)
                )
                """
            ).format(sql.Identifier(FENCE_RECEIPT_TABLE))
        )
        warm.execute(
            sql.SQL("REVOKE ALL ON TABLE public.{} FROM PUBLIC").format(
                sql.Identifier(FENCE_RECEIPT_TABLE)
            )
        )
        _ensure_lane_roles(hot, warm, base_manifest, specs, settings)
        ensure_generation_guard_objects(hot, settings.cell, settings.writer_database_user)
        for spec in specs:
            hot.execute(
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                    sql.Identifier(spec.publication_name)
                )
            )
            hot.execute(
                sql.SQL(
                    "CREATE PUBLICATION {} FOR TABLE public.{} "
                    "WITH (publish = 'insert, update', publish_via_partition_root = false)"
                ).format(
                    sql.Identifier(spec.publication_name),
                    sql.Identifier(spec.heartbeat_table),
                )
            )
    return base_manifest


def _ensure_lane_roles(
    hot: Any,
    warm: Any,
    manifest: BenchmarkManifest,
    specs: tuple[SourceConnectorSpec, ...],
    settings: Settings,
) -> None:
    _ensure_login_role(hot, settings.source_database_user, settings.source_database_password, replication=True)
    _ensure_login_role(warm, settings.sink_database_user, settings.sink_database_password, replication=False)
    _ensure_login_role(hot, settings.writer_database_user, settings.writer_database_password, replication=False)
    source = sql.Identifier(settings.source_database_user)
    sink = sql.Identifier(settings.sink_database_user)
    writer = sql.Identifier(settings.writer_database_user)
    hot.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(source))
    hot.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(source))
    hot.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(FENCE_SCHEMA), source)
    )
    hot.execute(
        sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
            sql.SQL(", ").join(sql.Identifier(route.parent) for route in manifest.tables),
            source,
        )
    )
    heartbeat_identifiers = sql.SQL(", ").join(
        sql.Identifier(spec.heartbeat_table) for spec in specs
    )
    hot.execute(sql.SQL("GRANT SELECT, UPDATE ON TABLE {} TO {}").format(heartbeat_identifiers, source))
    warm.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(sink))
    warm.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sink))
    warm.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {} TO {}").format(
            sql.SQL(", ").join(
                [sql.Identifier(route.parent) for route in manifest.tables]
                + [sql.Identifier(FENCE_RECEIPT_TABLE)]
            ),
            sink,
        )
    )
    hot.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(writer))
    hot.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON TABLE {} FROM {}").format(
            sql.SQL(", ").join(sql.Identifier(route.parent) for route in manifest.tables),
            writer,
        )
    )
    hot.execute(sql.SQL("REVOKE CREATE ON SCHEMA public FROM {}").format(writer))
    hot.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA flipbench_guard FROM {}").format(writer)
    )
    hot.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} FROM {}").format(
            sql.Identifier(FENCE_SCHEMA), writer
        )
    )
    hot.execute(sql.SQL("GRANT USAGE ON SCHEMA flipbench_guard TO {}").format(writer))
    hot.execute(
        sql.SQL("GRANT INSERT, SELECT, UPDATE ON TABLE {} TO flipbench_guard_owner").format(
            sql.SQL(", ").join(sql.Identifier(route.parent) for route in manifest.tables)
        )
    )


def bootstrap_lanes(settings: Settings) -> BenchmarkManifest:
    """Databases + heartbeat topics + lane connectors + generation-independent sink."""
    base_manifest = bootstrap_lanes_databases(settings)
    KafkaControl(settings.kafka_bootstrap).ensure_topics(
        tuple(
            TopicSpec(
                name,
                replication_factor=settings.kafka_topic_replication_factor,
                min_insync_replicas=settings.kafka_min_insync_replicas,
            )
            for name in topic_names(base_manifest, settings)
        )
    )
    source = ConnectClient(settings.source_connect_url)
    sink = ConnectClient(settings.sink_connect_url)
    if POSTGRES_CONNECTOR not in source.plugins():
        raise RuntimeError(f"source worker does not contain {POSTGRES_CONNECTOR}")
    if JDBC_CONNECTOR not in sink.plugins():
        raise RuntimeError(f"sink worker does not contain {JDBC_CONNECTOR}")
    specs = lane_source_specs(settings, base_manifest)
    for spec in specs:
        source.put_config(spec.connector_name, dict(spec.config))
    for spec in specs:
        source.wait_state(spec.connector_name, "RUNNING", 90)
    sink_connector = base_manifest.tables[0].sink_connector
    sink.put_config(sink_connector, lanes_sink_config(settings, base_manifest))
    sink.wait_state(sink_connector, "RUNNING", 90)
    return base_manifest


def verify_lane_publication_membership(
    hot: Any,
    settings: Settings,
    generation: GenerationSpec,
) -> None:
    """The generation's relations must all live in its own lane publication and in
    no other lane publication (invariant: one producer per leaf topic)."""
    expected = {
        item
        for route in generation.manifest.tables
        for item in (("public", route.leaf), (FENCE_SCHEMA, route.leaf))
    }
    for lane in LANE_NAMES:
        publication = f"{settings.publication_name}_{lane}"
        observed = {
            (schema, table)
            for schema, table in hot.execute(
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname=%s",
                (publication,),
            ).fetchall()
        }
        if lane == generation.lane:
            missing = expected - observed
            if missing:
                raise RuntimeError(
                    f"lane publication {publication} is missing generation relations: {sorted(missing)}"
                )
        else:
            overlap = expected & observed
            if overlap:
                raise RuntimeError(
                    f"generation relations appear in the wrong lane publication {publication}: {sorted(overlap)}"
                )


def provision_generation(
    settings: Settings,
    generation: GenerationSpec,
    kafka: KafkaControl,
    *,
    canary_timeout_seconds: float = 60.0,
) -> dict[str, Any]:
    """Create everything one generation needs and prove the CDC path end to end."""
    started_ns = time.perf_counter_ns()
    manifest = generation.manifest
    publication = f"{settings.publication_name}_{generation.lane}"
    kafka.ensure_topics(
        tuple(
            TopicSpec(
                route.topic,
                replication_factor=settings.kafka_topic_replication_factor,
                min_insync_replicas=settings.kafka_min_insync_replicas,
            )
            for route in manifest.tables
        )
    )
    with connect(settings.hot_dsn, autocommit=True) as hot, connect(
        settings.warm_dsn, autocommit=True
    ) as warm:
        _verify_environment_guard(hot, "hot")
        _verify_environment_guard(warm, "warm")
        source = sql.Identifier(settings.source_database_user)
        for route in manifest.tables:
            _create_leaf(hot, route.leaf, route.parent, generation.window_start, generation.window_end)
            hot.execute(
                sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(sql.Identifier(route.leaf), source)
            )
            hot.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                    sql.Identifier(FENCE_SCHEMA, route.leaf), _MARKER_COLUMNS
                )
            )
            hot.execute(
                sql.SQL("ALTER TABLE {} ADD CHECK (parent_name = {} AND leaf_name = {})").format(
                    sql.Identifier(FENCE_SCHEMA, route.leaf),
                    sql.Literal(route.parent),
                    sql.Literal(route.leaf),
                )
            )
            hot.execute(
                sql.SQL("REVOKE ALL ON TABLE {} FROM PUBLIC").format(
                    sql.Identifier(FENCE_SCHEMA, route.leaf)
                )
            )
            hot.execute(
                sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
                    sql.Identifier(FENCE_SCHEMA, route.leaf), source
                )
            )
        relations = sql.SQL(", ").join(
            item
            for route in manifest.tables
            for item in (
                sql.Identifier("public", route.leaf),
                sql.Identifier(FENCE_SCHEMA, route.leaf),
            )
        )
        hot.execute(
            sql.SQL("ALTER PUBLICATION {} ADD TABLE {}").format(
                sql.Identifier(publication), relations
            )
        )
        register_timeslot_window(
            hot, settings.cell, generation.timeslot, generation.window_start, generation.window_end
        )
        with hot.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO flipbench_guard.write_routes (cell, timeslot, parent_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (cell, timeslot, parent_name) DO NOTHING
                """,
                ((settings.cell, generation.timeslot, route.parent) for route in manifest.tables),
            )
        hot.execute(
            """
            INSERT INTO flipbench_guard.partition_write_gates (cell, timeslot, ownership_epoch, state)
            VALUES (%s, %s, 1, 'open')
            ON CONFLICT (cell, timeslot) DO NOTHING
            """,
            (settings.cell, generation.timeslot),
        )
        warm.execute(
            """
            INSERT INTO public.partition_tracker (cell, timeslot, state)
            VALUES (%s, %s, 'hot_primary')
            ON CONFLICT (cell, timeslot) DO NOTHING
            """,
            (settings.cell, generation.timeslot),
        )
        verify_lane_publication_membership(hot, settings, generation)

        canary_attempt = uuid.uuid4()
        markers = build_leaf_fence_markers(manifest, canary_attempt, 1)
        partitions = tuple(marker.partition for marker in markers)
        baselines = kafka.end_offsets(partitions)
        canary_started_ns = time.perf_counter_ns()
        emit_leaf_fence_markers(hot, markers, 1)
        marker_offsets = kafka.wait_leaf_fence_markers(markers, dict(baselines), canary_timeout_seconds)
        deadline = time.monotonic() + canary_timeout_seconds
        while True:
            observed = observed_leaf_fence_receipts(warm, markers, 1)
            if observed == frozenset(partitions):
                break
            if time.monotonic() >= deadline:
                missing = sorted(p.key for p in set(partitions) - observed)
                raise TimeoutError(f"provisioning canary receipts missing on warm: {missing}")
            time.sleep(0.25)
        canary_ns = time.perf_counter_ns() - canary_started_ns
    return {
        "generation": generation.timeslot,
        "lane": generation.lane,
        "window_start": generation.window_start.isoformat(),
        "window_end": generation.window_end.isoformat(),
        "provision_ns": time.perf_counter_ns() - started_ns,
        "canary_ns": canary_ns,
        "canary_attempt_id": str(canary_attempt),
        "canary_marker_offsets": {
            partition.key: offset for partition, offset in marker_offsets.items()
        },
    }
