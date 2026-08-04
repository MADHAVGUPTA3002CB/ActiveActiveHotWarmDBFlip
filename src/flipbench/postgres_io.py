from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from .core import BenchmarkManifest, HotSourceIdentity, LeafFenceMarker, TableRoute, TopicPartition
from .connector_configs import FENCE_RECEIPT_TABLE, FENCE_SCHEMA, SourceConnectorSpec
from .lifecycle import lifecycle_lock_name, validate_timeslot
from .traffic import CommittedTrafficWorkerError, FatalTrafficWorkerError


RETIRING_START = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
ACTIVE_START = datetime(2026, 8, 1, tzinfo=timezone.utc)
ACTIVE_END = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
ENVIRONMENT_MARKERS = {
    "hot": "2bbd8f35-7fa3-4a48-91ce-1d79df58d68d",
    "warm": "ca093fd2-6f08-44d1-a114-3494b55ca6bf",
}


@dataclass(frozen=True, slots=True)
class SlotStatus:
    identity: HotSourceIdentity
    confirmed_lsn: str
    restart_lsn: str | None
    lag_bytes: int
    active: bool


@dataclass(frozen=True, slots=True)
class HotWriteGateStatus:
    cell: str
    timeslot: str
    ownership_epoch: int
    state: str
    park_attempt_id: str | None
    attempt_epoch: int | None
    version: int


def hot_write_gate_status(
    connection: psycopg.Connection,
    cell: str,
    timeslot: str,
) -> HotWriteGateStatus:
    validated_timeslot = validate_timeslot(timeslot)
    row = connection.execute(
        """
        SELECT cell, timeslot, ownership_epoch, state, park_attempt_id::text,
               attempt_epoch, version
        FROM flipbench_guard.partition_write_gates
        WHERE cell=%s AND timeslot=%s
        """,
        (cell, validated_timeslot),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"hot write gate is missing for {cell}/{validated_timeslot}")
    return HotWriteGateStatus(*row)


def park_hot_write_gate(
    connection: psycopg.Connection,
    cell: str,
    timeslot: str,
    attempt_id: uuid.UUID,
    expected_ownership_epoch: int,
) -> int:
    validated_timeslot = validate_timeslot(timeslot)
    row = connection.execute(
        """
        UPDATE flipbench_guard.partition_write_gates
        SET state='parked', park_attempt_id=%s, version=version+1,
            updated_at=clock_timestamp()
        WHERE cell=%s AND timeslot=%s AND state='open'
          AND ownership_epoch=%s
          AND park_attempt_id IS NULL AND attempt_epoch IS NULL
        RETURNING ownership_epoch
        """,
        (attempt_id, cell, validated_timeslot, expected_ownership_epoch),
    ).fetchone()
    if row is None:
        raise RuntimeError("hot write gate park CAS failed")
    return int(row[0])


def bind_hot_write_gate_attempt(
    connection: psycopg.Connection,
    cell: str,
    timeslot: str,
    attempt_id: uuid.UUID,
    attempt_epoch: int,
) -> None:
    validated_timeslot = validate_timeslot(timeslot)
    updated = connection.execute(
        """
        UPDATE flipbench_guard.partition_write_gates
        SET attempt_epoch=%s, version=version+1, updated_at=clock_timestamp()
        WHERE cell=%s AND timeslot=%s AND state='parked'
          AND park_attempt_id=%s AND attempt_epoch IS NULL
        """,
        (attempt_epoch, cell, validated_timeslot, attempt_id),
    ).rowcount
    if updated != 1:
        raise RuntimeError("hot write gate attempt bind CAS failed")


def reopen_hot_write_gate(
    connection: psycopg.Connection,
    cell: str,
    timeslot: str,
    attempt_id: uuid.UUID,
    attempt_epoch: int | None,
) -> int:
    validated_timeslot = validate_timeslot(timeslot)
    row = connection.execute(
        """
        UPDATE flipbench_guard.partition_write_gates
        SET state='open', ownership_epoch=ownership_epoch+1,
            park_attempt_id=NULL, attempt_epoch=NULL, version=version+1,
            updated_at=clock_timestamp()
        WHERE cell=%s AND timeslot=%s AND state='parked'
          AND park_attempt_id=%s AND attempt_epoch IS NOT DISTINCT FROM %s
        RETURNING ownership_epoch
        """,
        (cell, validated_timeslot, attempt_id, attempt_epoch),
    ).fetchone()
    if row is None:
        raise RuntimeError("hot write gate reopen CAS failed")
    return int(row[0])


def _all_retiring_leaves_attached(
    hot: psycopg.Connection,
    manifest: BenchmarkManifest,
) -> bool:
    return all(
        hot.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_inherits
                WHERE inhparent=%s::regclass AND inhrelid=%s::regclass
                  AND NOT inhdetachpending
            )
            """,
            (f"public.{route.parent}", f"public.{route.leaf}"),
        ).fetchone()
        == (True,)
        for route in manifest.tables
    )


def _reconcile_hot_write_gate_locked(
    hot: psycopg.Connection,
    warm: psycopg.Connection,
    manifest: BenchmarkManifest,
    timeout_seconds: float = 30.0,
) -> str:
    """Idempotently reconcile fail-closed hot-gate states during API startup."""
    active = hot_write_gate_status(hot, manifest.cell, "active")
    if active.state != "open":
        raise RuntimeError("active hot write gate is not open during startup reconciliation")
    gate = hot_write_gate_status(hot, manifest.cell, manifest.timeslot)
    tracker = warm.execute(
        "SELECT state, attempt_epoch FROM public.partition_tracker WHERE cell=%s AND timeslot=%s",
        (manifest.cell, manifest.timeslot),
    ).fetchone()
    if tracker is None:
        raise RuntimeError("retiring warm ownership tracker is missing")

    if gate.state == "open":
        if tracker == ("hot_primary", None):
            return "open_consistent"
        raise RuntimeError(
            "hot write gate is open while warm ownership is not hot_primary; refusing startup"
        )

    attempt = warm.execute(
        """
        SELECT attempt_epoch, attempt_id::text, state, write_fence_mode,
               hot_ownership_epoch, hot_gate_version
        FROM public.flip_attempts
        WHERE attempt_id=%s
        """,
        (gate.park_attempt_id,),
    ).fetchone()
    if attempt is None:
        if gate.attempt_epoch is None and tracker == ("hot_primary", None):
            reopen_hot_write_gate(
                hot,
                manifest.cell,
                manifest.timeslot,
                uuid.UUID(str(gate.park_attempt_id)),
                None,
            )
            return "reopened_orphan_preparation"
        raise RuntimeError("parked hot gate has no matching warm attempt")

    attempt_epoch, attempt_id_text, attempt_state, mode, expected_epoch, gate_version = attempt
    if (
        mode not in ("hot_transactional_v1", "optimistic_detach_v1")
        or attempt_id_text != gate.park_attempt_id
        or expected_epoch != gate.ownership_epoch
        or not isinstance(gate_version, int)
        or gate.version < gate_version + 1
    ):
        raise RuntimeError("parked hot gate disagrees with its durable warm attempt")
    attempt_id = uuid.UUID(attempt_id_text)
    if gate.attempt_epoch is None:
        bind_hot_write_gate_attempt(
            hot, manifest.cell, manifest.timeslot, attempt_id, attempt_epoch
        )
        gate = hot_write_gate_status(hot, manifest.cell, manifest.timeslot)
    if gate.attempt_epoch != attempt_epoch or tracker[1] != attempt_epoch:
        raise RuntimeError("hot gate and warm tracker attempt epochs disagree")

    if attempt_state == "warm_primary" and tracker[0] == "warm_primary":
        return "warm_primary_parked"
    if attempt_state == "reverted" and tracker[0] == "hot_primary":
        if not _all_retiring_leaves_attached(hot, manifest):
            raise RuntimeError("reverted attempt has detached retiring leaves")
        reopen_hot_write_gate(
            hot, manifest.cell, manifest.timeslot, attempt_id, attempt_epoch
        )
        return "reopened_after_revert"
    if attempt_state in ("locked", "drained", "recovering") and tracker[0] in (
        "locked",
        "drained",
        "recovering",
    ):
        from .recovery import revert_to_hot

        revert_to_hot(
            hot,
            warm,
            manifest,
            attempt_epoch,
            timeout_seconds=timeout_seconds,
        )
        reopen_hot_write_gate(
            hot, manifest.cell, manifest.timeslot, attempt_id, attempt_epoch
        )
        return "reverted_nonterminal_attempt"
    raise RuntimeError("unrecognized hot/warm ownership combination; refusing startup")


def reconcile_hot_write_gate(
    hot: psycopg.Connection,
    warm: psycopg.Connection,
    manifest: BenchmarkManifest,
    timeout_seconds: float = 30.0,
) -> str:
    """Reconcile only when no live flip coordinator owns the hot session lock."""
    lock_name = lifecycle_lock_name(manifest.cell, manifest.timeslot)
    acquired = bool(
        hot.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
            (lock_name,),
        ).fetchone()[0]
    )
    if not acquired:
        raise RuntimeError(
            "startup reconciliation refused because a live flip coordinator owns the hot gate"
        )
    try:
        return _reconcile_hot_write_gate_locked(
            hot,
            warm,
            manifest,
            timeout_seconds=timeout_seconds,
        )
    finally:
        released = bool(
            hot.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (lock_name,),
            ).fetchone()[0]
        )
        if not released:
            raise RuntimeError("startup reconciliation lost its hot coordinator lock")


def trigger_source_heartbeat(
    connection: psycopg.Connection,
    spec: SourceConnectorSpec,
) -> int:
    """Commit one source-lane heartbeat without changing connector configuration."""
    updated = connection.execute(
        sql.SQL(
            "UPDATE public.{} SET touched_at = clock_timestamp() WHERE id = %s"
        ).format(sql.Identifier(spec.heartbeat_table)),
        (1,),
    ).rowcount
    if updated != 1:
        raise RuntimeError(
            f"heartbeat table {spec.heartbeat_table!r} must update exactly one row; updated={updated}"
        )
    return updated


def current_source_wal_flush_lsn(connection: psycopg.Connection) -> str:
    """Observe the source WAL position after a separately confirmed commit."""
    row = connection.execute("SELECT pg_current_wal_flush_lsn()::text").fetchone()
    if row is None or not isinstance(row[0], str):
        raise RuntimeError("could not observe the post-heartbeat WAL position")
    return row[0]


def expected_source_publication_tables(
    manifest: BenchmarkManifest, spec: SourceConnectorSpec
) -> frozenset[tuple[str, str]]:
    """Return the exact logical leaf coverage PostgreSQL must expose for a source lane."""
    business_tables = {
        name
        for route in manifest.tables
        for timeslot, name in (
            (manifest.timeslot, route.leaf),
            ("active", f"{route.parent}_p_active"),
        )
        if timeslot in spec.captured_timeslots
    }
    result = {("public", table) for table in business_tables}
    result.add(("public", spec.heartbeat_table))
    if manifest.timeslot in spec.captured_timeslots:
        result.update((FENCE_SCHEMA, route.leaf) for route in manifest.tables)
    return frozenset(result)


def verify_source_publication(
    connection: psycopg.Connection,
    manifest: BenchmarkManifest,
    spec: SourceConnectorSpec,
) -> None:
    """Fail closed if a live publication no longer matches the connector contract."""
    settings = connection.execute(
        """
        SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
        FROM pg_publication
        WHERE pubname=%s
        """,
        (spec.publication_name,),
    ).fetchone()
    expected_settings = (True, True, False, False, False)
    if settings != expected_settings:
        raise RuntimeError(
            f"publication {spec.publication_name!r} options mismatch: "
            f"expected={expected_settings}, observed={settings}"
        )
    observed = frozenset(
        (schema, table)
        for schema, table in connection.execute(
            "SELECT schemaname, tablename FROM pg_publication_tables "
            "WHERE pubname=%s",
            (spec.publication_name,),
        ).fetchall()
    )
    expected = expected_source_publication_tables(manifest, spec)
    if observed != expected:
        raise RuntimeError(
            f"publication {spec.publication_name!r} membership mismatch: "
            f"expected={sorted(expected)}, observed={sorted(observed)}"
        )


def connect(
    dsn: str,
    *,
    autocommit: bool = False,
    options: str | None = None,
) -> psycopg.Connection:
    if options is None:
        return psycopg.connect(dsn, autocommit=autocommit, connect_timeout=10)
    return psycopg.connect(
        dsn,
        autocommit=autocommit,
        connect_timeout=10,
        options=options,
    )


def bootstrap_databases(
    hot_dsn: str,
    warm_dsn: str,
    manifest: BenchmarkManifest,
    source_specs: tuple[SourceConnectorSpec, ...],
    source_database_password: str,
    sink_database_password: str,
    source_database_user: str,
    sink_database_user: str,
    writer_database_user: str,
    writer_database_password: str,
) -> None:
    with connect(hot_dsn) as hot, connect(warm_dsn) as warm:
        _verify_environment_guard(hot, "hot")
        _verify_environment_guard(warm, "warm")
        existing = hot.execute("SELECT to_regclass(%s)", (f"public.{manifest.tables[0].parent}",)).fetchone()[0]
        if existing is not None:
            raise RuntimeError("prototype tables already exist; use a clean, scoped Compose volume set")
        for route in manifest.tables:
            _create_hot_route(hot, route)
            _create_warm_table(warm, route)
        _create_leaf_fence_tables(hot, warm, manifest)
        _ensure_connector_roles(
            hot,
            warm,
            manifest,
            source_database_password,
            sink_database_password,
            source_database_user,
            sink_database_user,
            writer_database_user,
            writer_database_password,
        )
        hot.execute(
            """
            INSERT INTO flipbench_guard.partition_write_gates
                (cell, timeslot, ownership_epoch, state)
            VALUES (%s, 'retiring', 1, 'open'), (%s, 'active', 1, 'open')
            ON CONFLICT (cell, timeslot) DO NOTHING
            """,
            (manifest.cell, manifest.cell),
        )
        with hot.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO flipbench_guard.write_routes (cell, timeslot, parent_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (cell, timeslot, parent_name) DO NOTHING
                """,
                (
                    (manifest.cell, timeslot, route.parent)
                    for timeslot in ("retiring", "active")
                    for route in manifest.tables
                ),
            )
        warm.execute(
            """
            INSERT INTO public.partition_tracker (cell, timeslot, state)
            VALUES (%s, %s, 'hot_primary')
            ON CONFLICT (cell, timeslot) DO NOTHING
            """,
            (manifest.cell, manifest.timeslot),
        )
        warm.execute(
            """
            INSERT INTO public.partition_tracker (cell, timeslot, state)
            VALUES (%s, 'active', 'hot_primary')
            ON CONFLICT (cell, timeslot) DO NOTHING
            """,
            (manifest.cell,),
        )
        expected_business = {
            *(("public", route.leaf) for route in manifest.tables),
            *(("public", f"{route.parent}_p_active") for route in manifest.tables),
        }
        assigned_business: set[tuple[str, str]] = set()
        for spec in source_specs:
            expected_catalog_membership = expected_source_publication_tables(manifest, spec)
            covered_business = {
                item
                for item in expected_catalog_membership
                if item[0] == "public" and item[1] != spec.heartbeat_table
            }
            overlap = assigned_business.intersection(covered_business)
            if overlap:
                raise RuntimeError(f"source publications overlap on business leaves: {sorted(overlap)}")
            assigned_business.update(covered_business)
            expected = set(expected_catalog_membership)
            hot.execute(
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                    sql.Identifier(spec.publication_name)
                )
            )
            hot.execute(
                sql.SQL(
                    "CREATE PUBLICATION {} FOR TABLE {} "
                    "WITH (publish = 'insert, update', publish_via_partition_root = false)"
                ).format(
                    sql.Identifier(spec.publication_name),
                    sql.SQL(", ").join(
                        sql.Identifier(schema, table) for schema, table in sorted(expected)
                    ),
                )
            )
            verify_source_publication(hot, manifest, spec)
        if assigned_business != expected_business:
            raise RuntimeError("source publications do not exhaust active and retiring business leaves")


def _verify_environment_guard(connection: psycopg.Connection, expected_role: str) -> None:
    row = connection.execute(
        "SELECT current_database(), role, marker::text FROM public.flipbench_environment_guard WHERE role=%s",
        (expected_role,),
    ).fetchone()
    if row != ("cards", expected_role, ENVIRONMENT_MARKERS.get(expected_role)):
        raise RuntimeError(f"refusing DDL: expected local cards/{expected_role} environment guard")


def _ensure_login_role(
    connection: psycopg.Connection,
    role: str,
    password: str,
    *,
    replication: bool,
) -> None:
    exists = connection.execute("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s)", (role,)).fetchone()[0]
    attributes = sql.SQL("REPLICATION") if replication else sql.SQL("NOREPLICATION")
    if exists:
        connection.execute(
            sql.SQL(
                "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS {} PASSWORD {}"
            ).format(sql.Identifier(role), attributes, sql.Literal(password))
        )
    else:
        connection.execute(
            sql.SQL(
                "CREATE ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS {} PASSWORD {}"
            ).format(sql.Identifier(role), attributes, sql.Literal(password))
        )


def _ensure_connector_roles(
    hot: psycopg.Connection,
    warm: psycopg.Connection,
    manifest: BenchmarkManifest,
    source_password: str,
    sink_password: str,
    source_role: str,
    sink_role: str,
    writer_role: str,
    writer_password: str,
) -> None:
    _ensure_login_role(hot, source_role, source_password, replication=True)
    _ensure_login_role(warm, sink_role, sink_password, replication=False)
    _ensure_login_role(hot, writer_role, writer_password, replication=False)
    memberships = hot.execute(
        """
        SELECT granted.rolname
        FROM pg_auth_members AS membership
        JOIN pg_roles AS member ON member.oid=membership.member
        JOIN pg_roles AS granted ON granted.oid=membership.roleid
        WHERE member.rolname=%s
        """,
        (writer_role,),
    ).fetchall()
    for (granted_role,) in memberships:
        hot.execute(
            sql.SQL("REVOKE {} FROM {}").format(
                sql.Identifier(granted_role), sql.Identifier(writer_role)
            )
        )
    hot.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(sql.Identifier(source_role)))
    hot.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(source_role)))
    hot.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(FENCE_SCHEMA), sql.Identifier(source_role)
        )
    )
    published_tables = [sql.Identifier(route.parent) for route in manifest.tables]
    published_tables.extend(sql.Identifier(route.leaf) for route in manifest.tables)
    published_tables.extend(
        sql.Identifier(f"{route.parent}_p_active") for route in manifest.tables
    )
    heartbeat_tables = (
        "dbz_heartbeat",
        "dbz_heartbeat_active",
        "dbz_heartbeat_migration",
    )
    published_tables.extend(sql.Identifier(table) for table in heartbeat_tables)
    hot.execute(
        sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
            sql.SQL(", ").join(published_tables), sql.Identifier(source_role)
        )
    )
    hot.execute(
        sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
            sql.SQL(", ").join(
                sql.Identifier(FENCE_SCHEMA, route.leaf) for route in manifest.tables
            ),
            sql.Identifier(source_role),
        )
    )
    hot.execute(
        sql.SQL("GRANT UPDATE ON TABLE {} TO {}").format(
            sql.SQL(", ").join(sql.Identifier(table) for table in heartbeat_tables),
            sql.Identifier(source_role),
        )
    )
    warm.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(sql.Identifier(sink_role)))
    warm.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(sink_role)))
    warm.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {} TO {}").format(
            sql.SQL(", ").join(
                [sql.Identifier(route.parent) for route in manifest.tables]
                + [sql.Identifier(FENCE_RECEIPT_TABLE)]
            ),
            sql.Identifier(sink_role),
        )
    )
    hot.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(sql.Identifier(writer_role)))
    writer_tables = [sql.Identifier(route.parent) for route in manifest.tables]
    writer_tables.extend(sql.Identifier(route.leaf) for route in manifest.tables)
    writer_tables.extend(
        sql.Identifier(f"{route.parent}_p_active") for route in manifest.tables
    )
    hot.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON TABLE {} FROM {}").format(
            sql.SQL(", ").join(writer_tables), sql.Identifier(writer_role)
        )
    )
    hot.execute(
        sql.SQL("REVOKE CREATE ON SCHEMA public FROM {}").format(
            sql.Identifier(writer_role)
        )
    )
    hot.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA flipbench_guard FROM {}").format(
            sql.Identifier(writer_role)
        )
    )
    hot.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} FROM {}").format(
            sql.Identifier(FENCE_SCHEMA), sql.Identifier(writer_role)
        )
    )
    hot.execute(
        sql.SQL("GRANT USAGE ON SCHEMA flipbench_guard TO {}").format(
            sql.Identifier(writer_role)
        )
    )
    hot.execute(
        sql.SQL(
            "GRANT EXECUTE ON FUNCTION flipbench_guard.insert_events(text, text, bigint, name, jsonb) TO {}"
        ).format(sql.Identifier(writer_role))
    )
    hot.execute(
        sql.SQL(
            "GRANT EXECUTE ON FUNCTION flipbench_guard.admit_optimistic_batch(text, text, bigint) TO {}"
        ).format(sql.Identifier(writer_role))
    )
    hot.execute(
        sql.SQL(
            "GRANT EXECUTE ON FUNCTION flipbench_guard.insert_events_optimistic(text, text, name, jsonb) TO {}"
        ).format(sql.Identifier(writer_role))
    )
    hot.execute(
        sql.SQL("GRANT INSERT ON TABLE {} TO flipbench_guard_owner").format(
            sql.SQL(", ").join(sql.Identifier(route.parent) for route in manifest.tables)
        )
    )


def _create_leaf_fence_tables(
    hot: psycopg.Connection,
    warm: psycopg.Connection,
    manifest: BenchmarkManifest,
) -> None:
    hot.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(FENCE_SCHEMA)))
    hot.execute(sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(FENCE_SCHEMA)))
    marker_columns = sql.SQL(
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
    for route in manifest.tables:
        hot.execute(
            sql.SQL("CREATE TABLE {} ({})").format(
                sql.Identifier(FENCE_SCHEMA, route.leaf), marker_columns
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
    warm.execute(
        sql.SQL(
            """
            CREATE TABLE public.{} (
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
        """
        CREATE TABLE public.flip_leaf_fence_intents (
            attempt_epoch bigint NOT NULL REFERENCES public.flip_attempts(attempt_epoch),
            marker_id uuid NOT NULL UNIQUE,
            parent_name text NOT NULL,
            leaf_name text NOT NULL,
            topic text NOT NULL,
            partition_id integer NOT NULL CHECK (partition_id >= 0),
            scan_start_offset bigint NOT NULL CHECK (scan_start_offset >= 0),
            marker_next_offset bigint CHECK (marker_next_offset >= 0),
            observed_at timestamptz,
            PRIMARY KEY (attempt_epoch, leaf_name),
            UNIQUE (attempt_epoch, topic, partition_id),
            CHECK ((marker_next_offset IS NULL) = (observed_at IS NULL))
        )
        """
    )
    warm.execute(
        sql.SQL("REVOKE ALL ON TABLE public.{} FROM PUBLIC").format(
            sql.Identifier(FENCE_RECEIPT_TABLE)
        )
    )


def emit_leaf_fence_markers(
    connection: psycopg.Connection,
    markers: tuple[LeafFenceMarker, ...],
    ownership_epoch: int,
) -> None:
    if not markers or len({marker.leaf for marker in markers}) != len(markers):
        raise ValueError("leaf fence marker plan must be non-empty and unique")
    if (
        not isinstance(ownership_epoch, int)
        or isinstance(ownership_epoch, bool)
        or ownership_epoch <= 0
    ):
        raise ValueError("leaf fence ownership_epoch must be positive")
    with connection.transaction():
        for marker in markers:
            connection.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (
                        marker_schema_version, marker_id, attempt_id, attempt_epoch,
                        ownership_epoch, cell, timeslot, parent_name, leaf_name
                    )
                    VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (attempt_id, leaf_name) DO NOTHING
                    """
                ).format(sql.Identifier(FENCE_SCHEMA, marker.leaf)),
                (
                    marker.marker_id,
                    marker.attempt_id,
                    marker.attempt_epoch,
                    ownership_epoch,
                    marker.cell,
                    marker.timeslot,
                    marker.parent,
                    marker.leaf,
                ),
            )
            observed = connection.execute(
                sql.SQL(
                    """
                    SELECT marker_id, attempt_epoch, ownership_epoch, cell, timeslot,
                           parent_name, leaf_name
                    FROM {}
                    WHERE attempt_id=%s AND leaf_name=%s
                    """
                ).format(sql.Identifier(FENCE_SCHEMA, marker.leaf)),
                (marker.attempt_id, marker.leaf),
            ).fetchone()
            expected = (
                marker.marker_id,
                marker.attempt_epoch,
                ownership_epoch,
                marker.cell,
                marker.timeslot,
                marker.parent,
                marker.leaf,
            )
            if observed != expected:
                raise RuntimeError(f"conflicting durable leaf fence marker for {marker.leaf}")


def atomic_detach_and_emit_leaf_fence_marker(
    connection: psycopg.Connection,
    marker: LeafFenceMarker,
    ownership_epoch: int,
) -> None:
    """Detach one leaf and commit its CDC marker in the same transaction."""
    if not isinstance(marker, LeafFenceMarker):
        raise ValueError("atomic detach requires one validated leaf fence marker")
    if (
        not isinstance(ownership_epoch, int)
        or isinstance(ownership_epoch, bool)
        or ownership_epoch <= 0
    ):
        raise ValueError("leaf fence ownership_epoch must be positive")
    with connection.transaction():
        connection.execute(
            sql.SQL("ALTER TABLE {} DETACH PARTITION {}").format(
                sql.Identifier(marker.parent),
                sql.Identifier(marker.leaf),
            )
        )
        connection.execute(
            sql.SQL(
                """
                INSERT INTO {} (
                    marker_schema_version, marker_id, attempt_id, attempt_epoch,
                    ownership_epoch, cell, timeslot, parent_name, leaf_name
                )
                VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (attempt_id, leaf_name) DO NOTHING
                """
            ).format(sql.Identifier(FENCE_SCHEMA, marker.leaf)),
            (
                marker.marker_id,
                marker.attempt_id,
                marker.attempt_epoch,
                ownership_epoch,
                marker.cell,
                marker.timeslot,
                marker.parent,
                marker.leaf,
            ),
        )
        observed = connection.execute(
            sql.SQL(
                """
                SELECT marker_id, attempt_epoch, ownership_epoch, cell, timeslot,
                       parent_name, leaf_name
                FROM {}
                WHERE attempt_id=%s AND leaf_name=%s
                """
            ).format(sql.Identifier(FENCE_SCHEMA, marker.leaf)),
            (marker.attempt_id, marker.leaf),
        ).fetchone()
        expected = (
            marker.marker_id,
            marker.attempt_epoch,
            ownership_epoch,
            marker.cell,
            marker.timeslot,
            marker.parent,
            marker.leaf,
        )
        if observed != expected:
            raise RuntimeError(
                f"conflicting durable atomic detach marker for {marker.leaf}"
            )


def observed_leaf_fence_receipts(
    connection: psycopg.Connection,
    markers: tuple[LeafFenceMarker, ...],
    ownership_epoch: int,
) -> frozenset[TopicPartition]:
    if not markers:
        raise ValueError("leaf fence receipt query requires markers")
    rows = connection.execute(
        sql.SQL(
            """
            SELECT marker_id, attempt_id, attempt_epoch, ownership_epoch, cell, timeslot,
                   parent_name, leaf_name
            FROM public.{}
            WHERE attempt_id=%s
            """
        ).format(sql.Identifier(FENCE_RECEIPT_TABLE)),
        (markers[0].attempt_id,),
    ).fetchall()
    expected_by_leaf = {marker.leaf: marker for marker in markers}
    observed: set[TopicPartition] = set()
    for row in rows:
        marker = expected_by_leaf.get(str(row[7]))
        if marker is None:
            raise RuntimeError("warm leaf fence receipt contains an unexpected leaf")
        expected = (
            marker.marker_id,
            marker.attempt_id,
            marker.attempt_epoch,
            ownership_epoch,
            marker.cell,
            marker.timeslot,
            marker.parent,
            marker.leaf,
        )
        if row != expected:
            raise RuntimeError(f"warm leaf fence receipt conflicts for {marker.leaf}")
        observed.add(marker.partition)
    return frozenset(observed)


def _create_hot_route(connection: psycopg.Connection, route: TableRoute) -> None:
    connection.execute(
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
    _create_leaf(connection, route.leaf, route.parent, RETIRING_START, ACTIVE_START)
    active_leaf = f"{route.parent}_p_active"
    _create_leaf(connection, active_leaf, route.parent, ACTIVE_START, ACTIVE_END)
    trigger = f"{route.parent}_immutable_record_key"
    connection.execute(sql.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(sql.Identifier(trigger), sql.Identifier(route.parent)))
    connection.execute(
        sql.SQL(
            "CREATE TRIGGER {} BEFORE UPDATE OF id, created_at ON {} FOR EACH ROW EXECUTE FUNCTION public.reject_record_key_change()"
        ).format(sql.Identifier(trigger), sql.Identifier(route.parent))
    )


def _create_leaf(
    connection: psycopg.Connection,
    leaf: str,
    parent: str,
    start: datetime,
    end: datetime,
) -> None:
    connection.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})").format(
            sql.Identifier(leaf),
            sql.Identifier(parent),
            sql.Literal(start),
            sql.Literal(end),
        ),
    )
    constraint = f"{leaf}_bound_check"
    connection.execute(
        sql.SQL("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}").format(
            sql.Identifier(leaf), sql.Identifier(constraint)
        )
    )
    connection.execute(
        sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} CHECK (created_at >= {} AND created_at < {})").format(
            sql.Identifier(leaf),
            sql.Identifier(constraint),
            sql.Literal(start),
            sql.Literal(end),
        )
    )
    connection.execute(sql.SQL("REVOKE ALL ON {} FROM PUBLIC").format(sql.Identifier(leaf)))


def _create_warm_table(connection: psycopg.Connection, route: TableRoute) -> None:
    connection.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                id uuid NOT NULL,
                experiment_run_id uuid NOT NULL,
                sequence_no bigint NOT NULL,
                created_at timestamptz NOT NULL,
                payload jsonb NOT NULL,
                updated_at timestamptz NOT NULL,
                PRIMARY KEY (id, created_at)
            )
            """
        ).format(sql.Identifier(route.parent))
    )


def _insert_events_unchecked(
    hot_dsn: str,
    manifest: BenchmarkManifest,
    run_id: uuid.UUID,
    events_per_table: int,
    timeslot: str,
    payload_bytes: int,
) -> int:
    if not 0 < events_per_table <= 1_000_000 or not 16 <= payload_bytes <= 1_048_576:
        raise ValueError("events_per_table must be 1..1,000,000 and payload_bytes must be 16..1,048,576")
    validated_timeslot = validate_timeslot(timeslot)
    created_at = RETIRING_START if validated_timeslot == "retiring" else ACTIVE_START
    payload = {"padding": "x" * (payload_bytes - 15), "timeslot": timeslot}
    inserted = 0
    with connect(hot_dsn) as connection:
        with connection.cursor() as cursor:
            for table_index, route in enumerate(manifest.tables, start=1):
                for batch_start in range(0, events_per_table, 1_000):
                    batch_end = min(batch_start + 1_000, events_per_table)
                    rows = tuple(
                        (
                            uuid.uuid4(),
                            run_id,
                            table_index * 1_000_000_000 + sequence,
                            created_at,
                            psycopg.types.json.Jsonb(payload),
                        )
                        for sequence in range(batch_start, batch_end)
                    )
                    cursor.executemany(
                        sql.SQL("INSERT INTO {} (id, experiment_run_id, sequence_no, created_at, payload) VALUES (%s, %s, %s, %s, %s)").format(
                            sql.Identifier(route.parent)
                        ),
                        rows,
                    )
                    inserted += len(rows)
    return inserted


def guarded_insert_events(
    hot_dsn: str,
    warm_dsn: str,
    manifest: BenchmarkManifest,
    run_id: uuid.UUID,
    events_per_table: int,
    timeslot: str,
    payload_bytes: int,
) -> int:
    validated_timeslot = validate_timeslot(timeslot)
    lock_name = lifecycle_lock_name(manifest.cell, validated_timeslot)
    with connect(warm_dsn, autocommit=True) as tracker:
        acquired = tracker.execute(
            "SELECT pg_try_advisory_lock_shared(hashtextextended(%s, 0))",
            (lock_name,),
        ).fetchone()[0]
        if not acquired:
            raise RuntimeError("hot writer parked: lifecycle lock is exclusive")
        try:
            state = tracker.execute(
                "SELECT state FROM public.partition_tracker WHERE cell=%s AND timeslot=%s",
                (manifest.cell, validated_timeslot),
            ).fetchone()
            if state != ("hot_primary",):
                raise RuntimeError(f"hot writer parked: ownership state is {None if state is None else state[0]}")
            return _insert_events_unchecked(
                hot_dsn, manifest, run_id, events_per_table, timeslot, payload_bytes
            )
        finally:
            tracker.execute("SELECT pg_advisory_unlock_shared(hashtextextended(%s, 0))", (lock_name,))


class GuardedTransactionSession:
    """Persistent worker session where one write call equals one hot DB commit."""

    def __init__(
        self,
        hot_dsn: str,
        warm_dsn: str,
        manifest: BenchmarkManifest,
        run_id: uuid.UUID,
        timeslot: str,
        payload_bytes: int,
    ) -> None:
        if not 16 <= payload_bytes <= 1_048_576:
            raise ValueError("payload_bytes must be 16..1,048,576")
        self._manifest = manifest
        self._run_id = run_id
        self._timeslot = validate_timeslot(timeslot)
        self._created_at = RETIRING_START if self._timeslot == "retiring" else ACTIVE_START
        self._payload = psycopg.types.json.Jsonb(
            {"padding": "x" * (payload_bytes - 15), "timeslot": self._timeslot}
        )
        self._lock_name = lifecycle_lock_name(manifest.cell, self._timeslot)
        self._sequence_prefix = (uuid.uuid4().int % 1_000_000) * 1_000_000_000_000
        self._sequence = 0
        self._closed = False
        timeout_options = "-c statement_timeout=3000 -c lock_timeout=1000"
        self._warm = connect(warm_dsn, autocommit=True, options=timeout_options)
        try:
            self._hot = connect(hot_dsn, options=timeout_options)
        except BaseException:
            self._warm.close()
            raise

    def _acquire_guard(self) -> None:
        acquired = self._warm.execute(
            "SELECT pg_try_advisory_lock_shared(hashtextextended(%s, 0))",
            (self._lock_name,),
        ).fetchone()
        if acquired != (True,):
            raise RuntimeError("hot writer parked: lifecycle lock is exclusive")
        try:
            state = self._warm.execute(
                "SELECT state FROM public.partition_tracker WHERE cell=%s AND timeslot=%s",
                (self._manifest.cell, self._timeslot),
            ).fetchone()
        except BaseException:
            self._release_guard()
            raise
        if state != ("hot_primary",):
            self._release_guard()
            value = None if state is None else state[0]
            raise RuntimeError(f"hot writer parked: ownership state is {value}")

    def _release_guard(self) -> None:
        try:
            released = self._warm.execute(
                "SELECT pg_advisory_unlock_shared(hashtextextended(%s, 0))",
                (self._lock_name,),
            ).fetchone()
        except BaseException as error:
            self._invalidate()
            raise FatalTrafficWorkerError(
                "lifecycle guard release failed; worker session was closed"
            ) from error
        if released != (True,):
            self._invalidate()
            raise FatalTrafficWorkerError(
                "lifecycle guard release was not confirmed; worker session was closed"
            )

    def _invalidate(self) -> None:
        if self._closed:
            return
        self._closed = True
        for connection in (self._hot, self._warm):
            try:
                connection.close()
            except BaseException:
                pass

    def _event_rows(self, rows: int) -> tuple[tuple[object, ...], ...]:
        if not isinstance(rows, int) or isinstance(rows, bool) or not 1 <= rows <= 100_000:
            raise ValueError("rows must be an integer between 1 and 100000")
        start = self._sequence
        self._sequence += rows
        return tuple(
            (
                uuid.uuid4(),
                self._run_id,
                self._sequence_prefix + sequence,
                self._created_at,
                self._payload,
            )
            for sequence in range(start, self._sequence)
        )

    def write(self, table_index: int, rows: int) -> int:
        if self._closed:
            raise RuntimeError("transaction session is closed")
        if (
            not isinstance(table_index, int)
            or isinstance(table_index, bool)
            or not 0 <= table_index < len(self._manifest.tables)
        ):
            raise ValueError("table_index is outside the benchmark manifest")
        event_rows = self._event_rows(rows)
        self._acquire_guard()
        try:
            with self._hot.cursor() as cursor:
                cursor.executemany(
                    sql.SQL(
                        "INSERT INTO {} (id, experiment_run_id, sequence_no, created_at, payload) "
                        "VALUES (%s, %s, %s, %s, %s)"
                    ).format(sql.Identifier(self._manifest.tables[table_index].parent)),
                    event_rows,
                )
            self._hot.commit()
        except BaseException:
            try:
                self._hot.rollback()
            finally:
                self._release_guard()
            raise
        try:
            self._release_guard()
        except FatalTrafficWorkerError as error:
            raise CommittedTrafficWorkerError(len(event_rows), str(error)) from error
        return len(event_rows)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        for connection in (self._hot, self._warm):
            try:
                connection.close()
            except BaseException as error:
                errors.append(error)
        if errors:
            raise RuntimeError("; ".join(str(error) for error in errors))


class HotFencedTransactionSession:
    """Hot-only worker; one function call and one commit form one guarded transaction."""

    def __init__(
        self,
        hot_dsn: str,
        manifest: BenchmarkManifest,
        run_id: uuid.UUID,
        timeslot: str,
        payload_bytes: int,
        expected_ownership_epoch: int,
    ) -> None:
        if not 16 <= payload_bytes <= 1_048_576:
            raise ValueError("payload_bytes must be 16..1,048,576")
        if (
            not isinstance(expected_ownership_epoch, int)
            or isinstance(expected_ownership_epoch, bool)
            or expected_ownership_epoch <= 0
        ):
            raise ValueError("expected_ownership_epoch must be a positive integer")
        self._manifest = manifest
        self._run_id = run_id
        self._timeslot = validate_timeslot(timeslot)
        self._created_at = RETIRING_START if self._timeslot == "retiring" else ACTIVE_START
        self._payload = {
            "padding": "x" * (payload_bytes - 15),
            "timeslot": self._timeslot,
        }
        self._expected_ownership_epoch = expected_ownership_epoch
        self._sequence_prefix = (uuid.uuid4().int % 1_000_000) * 1_000_000_000_000
        self._sequence = 0
        self._closed = False
        timeout_options = "-c statement_timeout=3000 -c lock_timeout=1000"
        self._hot = connect(hot_dsn, options=timeout_options)

    def _event_payload(self, rows: int) -> list[dict[str, object]]:
        if not isinstance(rows, int) or isinstance(rows, bool) or not 1 <= rows <= 100_000:
            raise ValueError("rows must be an integer between 1 and 100000")
        start = self._sequence
        self._sequence += rows
        return [
            {
                "id": str(uuid.uuid4()),
                "experiment_run_id": str(self._run_id),
                "sequence_no": self._sequence_prefix + sequence,
                "created_at": self._created_at.isoformat(),
                "payload": self._payload,
            }
            for sequence in range(start, self._sequence)
        ]

    def write(self, table_index: int, rows: int) -> int:
        if self._closed:
            raise RuntimeError("transaction session is closed")
        if (
            not isinstance(table_index, int)
            or isinstance(table_index, bool)
            or not 0 <= table_index < len(self._manifest.tables)
        ):
            raise ValueError("table_index is outside the benchmark manifest")
        payload = self._event_payload(rows)
        try:
            result = self._hot.execute(
                """
                /* guarded hot transaction */
                SELECT flipbench_guard.insert_events(%s, %s, %s, %s, %s)::integer
                """,
                (
                    self._manifest.cell,
                    self._timeslot,
                    self._expected_ownership_epoch,
                    self._manifest.tables[table_index].parent,
                    Jsonb(payload),
                ),
            ).fetchone()
            if result is None or result[0] != rows:
                raise RuntimeError(
                    f"guarded database operation inserted {None if result is None else result[0]} rows; expected {rows}"
                )
            self._hot.commit()
        except Exception as error:
            self._hot.rollback()
            if "hot writer parked:" in str(error):
                raise RuntimeError(str(error)) from error
            raise
        return rows

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._hot.close()


class OptimisticDetachTransactionSession:
    """One ownership admission per bounded batch, then independent table commits."""

    def __init__(
        self,
        hot_dsn: str,
        manifest: BenchmarkManifest,
        run_id: uuid.UUID,
        timeslot: str,
        payload_bytes: int,
        expected_ownership_epoch: int,
        operations_per_batch: int,
    ) -> None:
        if not 16 <= payload_bytes <= 1_048_576:
            raise ValueError("payload_bytes must be 16..1,048,576")
        if (
            not isinstance(expected_ownership_epoch, int)
            or isinstance(expected_ownership_epoch, bool)
            or expected_ownership_epoch <= 0
        ):
            raise ValueError("expected_ownership_epoch must be a positive integer")
        if (
            not isinstance(operations_per_batch, int)
            or isinstance(operations_per_batch, bool)
            or not 1 <= operations_per_batch <= 1_000
        ):
            raise ValueError("operations_per_batch must be an integer between 1 and 1000")
        self._manifest = manifest
        self._run_id = run_id
        self._timeslot = validate_timeslot(timeslot)
        self._created_at = RETIRING_START if self._timeslot == "retiring" else ACTIVE_START
        self._payload = {
            "padding": "x" * (payload_bytes - 15),
            "timeslot": self._timeslot,
        }
        self._expected_ownership_epoch = expected_ownership_epoch
        self._operations_per_batch = operations_per_batch
        self._remaining_batch_operations = 0
        self._sequence_prefix = (uuid.uuid4().int % 1_000_000) * 1_000_000_000_000
        self._sequence = 0
        self._closed = False
        timeout_options = "-c statement_timeout=3000 -c lock_timeout=1000"
        self._hot = connect(hot_dsn, options=timeout_options)

    def _event_payload(self, rows: int) -> list[dict[str, object]]:
        if not isinstance(rows, int) or isinstance(rows, bool) or not 1 <= rows <= 100_000:
            raise ValueError("rows must be an integer between 1 and 100000")
        start = self._sequence
        self._sequence += rows
        return [
            {
                "id": str(uuid.uuid4()),
                "experiment_run_id": str(self._run_id),
                "sequence_no": self._sequence_prefix + sequence,
                "created_at": self._created_at.isoformat(),
                "payload": self._payload,
            }
            for sequence in range(start, self._sequence)
        ]

    def write(self, table_index: int, rows: int) -> int:
        if self._closed:
            raise RuntimeError("transaction session is closed")
        if (
            not isinstance(table_index, int)
            or isinstance(table_index, bool)
            or not 0 <= table_index < len(self._manifest.tables)
        ):
            raise ValueError("table_index is outside the benchmark manifest")
        payload = self._event_payload(rows)
        starts_batch = self._remaining_batch_operations == 0
        if starts_batch:
            statement = """
                WITH admission AS MATERIALIZED (
                    SELECT flipbench_guard.admit_optimistic_batch(%s, %s, %s)::bigint
                )
                SELECT flipbench_guard.insert_events_optimistic(%s, %s, %s, %s)::integer
                FROM admission
                """
            parameters = (
                self._manifest.cell,
                self._timeslot,
                self._expected_ownership_epoch,
                self._manifest.cell,
                self._timeslot,
                self._manifest.tables[table_index].parent,
                Jsonb(payload),
            )
        else:
            statement = """
                SELECT flipbench_guard.insert_events_optimistic(%s, %s, %s, %s)::integer
                """
            parameters = (
                self._manifest.cell,
                self._timeslot,
                self._manifest.tables[table_index].parent,
                Jsonb(payload),
            )
        try:
            result = self._hot.execute(statement, parameters).fetchone()
            if result is None or result[0] != rows:
                raise RuntimeError(
                    "optimistic database operation inserted "
                    f"{None if result is None else result[0]} rows; expected {rows}"
                )
            self._hot.commit()
            self._remaining_batch_operations = (
                self._operations_per_batch - 1
                if starts_batch
                else self._remaining_batch_operations - 1
            )
        except Exception as error:
            self._hot.rollback()
            self._remaining_batch_operations = 0
            if "hot writer parked:" in str(error):
                raise RuntimeError(str(error)) from error
            raise
        return rows

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._hot.close()


def hot_identity(connection: psycopg.Connection, cell: str, slot: str) -> tuple[HotSourceIdentity, str]:
    system_identifier, database, lsn = connection.execute(
        "SELECT system_identifier::text, current_database(), pg_current_wal_flush_lsn()::text FROM pg_control_system()"
    ).fetchone()
    return HotSourceIdentity(cell, system_identifier, database, slot), lsn


def slot_status(connection: psycopg.Connection, cell: str, slot: str) -> SlotStatus:
    row = connection.execute(
        """
        SELECT c.system_identifier::text,
               current_database(),
               s.confirmed_flush_lsn::text,
               s.restart_lsn::text,
               pg_wal_lsn_diff(pg_current_wal_flush_lsn(), s.confirmed_flush_lsn)::bigint,
               s.active
        FROM pg_control_system() c
        CROSS JOIN pg_replication_slots s
        WHERE s.slot_name = %s AND s.database = current_database()
        """,
        (slot,),
    ).fetchone()
    if row is None or row[2] is None:
        raise RuntimeError(f"logical slot {slot!r} is missing or has no confirmed_flush_lsn")
    return SlotStatus(
        HotSourceIdentity(cell, row[0], row[1], slot),
        row[2],
        row[3],
        row[4],
        row[5],
    )


def wait_slot_lsn(
    connection: psycopg.Connection,
    cell: str,
    slot: str,
    target_lsn: str,
    timeout_seconds: float,
    poll_seconds: float,
) -> SlotStatus:
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        timeout_ms = max(1, int(remaining * 1000))
        connection.execute(
            "SELECT set_config('statement_timeout', %s, false)",
            (f"{timeout_ms}ms",),
        )
        status = slot_status(connection, cell, slot)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        timeout_ms = max(1, int(remaining * 1000))
        connection.execute(
            "SELECT set_config('statement_timeout', %s, false)",
            (f"{timeout_ms}ms",),
        )
        reached = connection.execute(
            "SELECT %s::pg_lsn >= %s::pg_lsn",
            (status.confirmed_lsn, target_lsn),
        ).fetchone()[0]
        if reached:
            return status
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
    raise TimeoutError(f"slot {slot} did not confirm fence {target_lsn} within {timeout_seconds}s")


def checksum_for_run(connection: psycopg.Connection, table: str, run_id: uuid.UUID) -> tuple[int, int]:
    count, checksum = connection.execute(
        sql.SQL(
            """
            SELECT count(*)::bigint,
                   COALESCE(sum(hashtextextended(concat_ws('|',
                       id::text,
                       experiment_run_id::text,
                       sequence_no::text,
                       (extract(epoch FROM created_at) * 1000000)::bigint::text,
                       payload::text,
                       (extract(epoch FROM updated_at) * 1000000)::bigint::text
                   ), 0)::numeric), 0)::numeric
            FROM {}
            WHERE experiment_run_id = %s
            """
        ).format(sql.Identifier(table)),
        (run_id,),
    ).fetchone()
    return int(count), int(checksum)


def parity_for_run(
    hot: psycopg.Connection,
    warm: psycopg.Connection,
    manifest: BenchmarkManifest,
    run_id: uuid.UUID,
) -> tuple[bool, tuple[dict[str, object], ...]]:
    rows = tuple(
        {
            "parent": route.parent,
            "hot": checksum_for_run(hot, route.leaf, run_id),
            "warm": checksum_for_run(warm, route.parent, run_id),
        }
        for route in manifest.tables
    )
    return all(row["hot"] == row["warm"] for row in rows), rows
