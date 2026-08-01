from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import psycopg
from psycopg import sql

from .core import BenchmarkManifest, HotSourceIdentity, TableRoute
from .lifecycle import lifecycle_lock_name, validate_timeslot


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


def connect(dsn: str, *, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=autocommit, connect_timeout=10)


def bootstrap_databases(
    hot_dsn: str,
    warm_dsn: str,
    manifest: BenchmarkManifest,
    publication: str,
    source_database_password: str,
    sink_database_password: str,
    source_database_user: str,
    sink_database_user: str,
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
        _ensure_connector_roles(
            hot,
            warm,
            manifest,
            source_database_password,
            sink_database_password,
            source_database_user,
            sink_database_user,
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
        hot.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(publication)))
        published = [sql.Identifier("public", route.parent) for route in manifest.tables]
        published.append(sql.Identifier("public", "dbz_heartbeat"))
        hot.execute(
            sql.SQL("CREATE PUBLICATION {} FOR TABLE {} WITH (publish = 'insert, update', publish_via_partition_root = false)").format(
                sql.Identifier(publication), sql.SQL(", ").join(published)
            )
        )


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
                "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE {} PASSWORD {}"
            ).format(sql.Identifier(role), attributes, sql.Literal(password))
        )
    else:
        connection.execute(
            sql.SQL(
                "CREATE ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE {} PASSWORD {}"
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
) -> None:
    _ensure_login_role(hot, source_role, source_password, replication=True)
    _ensure_login_role(warm, sink_role, sink_password, replication=False)
    hot.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(sql.Identifier(source_role)))
    hot.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(source_role)))
    published_tables = [sql.Identifier(route.parent) for route in manifest.tables]
    published_tables.append(sql.Identifier("dbz_heartbeat"))
    hot.execute(
        sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
            sql.SQL(", ").join(published_tables), sql.Identifier(source_role)
        )
    )
    hot.execute(
        sql.SQL("GRANT UPDATE ON TABLE {} TO {}").format(
            sql.Identifier("dbz_heartbeat"), sql.Identifier(source_role)
        )
    )
    warm.execute(sql.SQL("GRANT CONNECT ON DATABASE cards TO {}").format(sql.Identifier(sink_role)))
    warm.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(sink_role)))
    warm.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {} TO {}").format(
            sql.SQL(", ").join(sql.Identifier(route.parent) for route in manifest.tables),
            sql.Identifier(sink_role),
        )
    )


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
    while time.monotonic() < deadline:
        status = slot_status(connection, cell, slot)
        reached = connection.execute("SELECT %s::pg_lsn >= %s::pg_lsn", (status.confirmed_lsn, target_lsn)).fetchone()[0]
        if reached:
            return status
        time.sleep(poll_seconds)
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
