from __future__ import annotations

import time
from enum import StrEnum
from typing import Any

from .core import BenchmarkManifest


class RecoveryError(RuntimeError):
    pass


class CatalogLeafState(StrEnum):
    ATTACHED = "attached"
    PENDING_FINALIZE = "pending_finalize"
    DETACHED = "detached"


def _remaining_ms(deadline: float) -> int:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RecoveryError("recovery deadline expired")
    return max(1, int(remaining * 1000))


def _refresh_statement_timeout(connection: Any, deadline: float) -> None:
    connection.execute(
        "SELECT set_config('statement_timeout', %s, false)",
        (f"{_remaining_ms(deadline)}ms",),
    )


def recovery_steps(state: CatalogLeafState) -> tuple[str, ...]:
    if state is CatalogLeafState.ATTACHED:
        return ()
    if state is CatalogLeafState.PENDING_FINALIZE:
        return ("finalize", "attach", "verify")
    if state is CatalogLeafState.DETACHED:
        return ("attach", "verify")
    raise RecoveryError(f"unknown catalog leaf state: {state!r}")


def _catalog_state(hot: Any, parent: str, leaf: str) -> CatalogLeafState:
    row = hot.execute(
        """
        SELECT i.inhdetachpending
        FROM pg_inherits i
        WHERE i.inhparent=%s::regclass AND i.inhrelid=%s::regclass
        """,
        (f"public.{parent}", f"public.{leaf}"),
    ).fetchone()
    if row is None:
        return CatalogLeafState.DETACHED
    return CatalogLeafState.PENDING_FINALIZE if row[0] else CatalogLeafState.ATTACHED


def revert_to_hot(
    hot: Any,
    warm: Any,
    manifest: BenchmarkManifest,
    attempt_epoch: int,
    timeout_seconds: float,
    window: tuple[Any, Any] | None = None,
) -> tuple[dict[str, str], ...]:
    if timeout_seconds <= 0:
        raise RecoveryError("recovery timeout must be positive")
    if window is None and manifest.timeslot != "retiring":
        raise RecoveryError(
            "automatic revert requires the retiring timeslot or an explicit partition window"
        )
    deadline = time.monotonic() + timeout_seconds
    _refresh_statement_timeout(warm, deadline)
    ownership = warm.execute(
        """
        SELECT a.state::text, p.state::text, p.attempt_epoch
        FROM public.flip_attempts a
        JOIN public.partition_tracker p
          ON p.cell=a.cell AND p.timeslot=a.timeslot
        WHERE a.attempt_epoch=%s AND p.cell=%s AND p.timeslot=%s
        """,
        (attempt_epoch, manifest.cell, manifest.timeslot),
    ).fetchone()
    recoverable = {"locked", "drained", "recovering"}
    if (
        ownership is None
        or ownership[0] not in recoverable
        or ownership[1] not in recoverable
        or ownership[2] != attempt_epoch
    ):
        raise RecoveryError("attempt/tracker ownership is not recoverable; refusing hot DDL")

    from psycopg import sql

    from .postgres_io import ACTIVE_START, RETIRING_START

    if window is None:
        window = (RETIRING_START, ACTIVE_START)
    window_start, window_end = window

    recovered: list[dict[str, str]] = []
    for route in manifest.tables:
        if time.monotonic() >= deadline:
            raise RecoveryError("recovery deadline expired before every leaf was attached")
        state = _catalog_state(hot, route.parent, route.leaf)
        for step in recovery_steps(state):
            remaining_ms = _remaining_ms(deadline)
            hot.execute("SELECT set_config('statement_timeout', %s, false)", (f"{remaining_ms}ms",))
            hot.execute("SELECT set_config('lock_timeout', %s, false)", (f"{remaining_ms}ms",))
            if step == "finalize":
                hot.execute(
                    sql.SQL("ALTER TABLE {} DETACH PARTITION {} FINALIZE").format(
                        sql.Identifier(route.parent), sql.Identifier(route.leaf)
                    )
                )
            elif step == "attach":
                hot.execute(
                    sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES FROM ({}) TO ({})").format(
                        sql.Identifier(route.parent),
                        sql.Identifier(route.leaf),
                        sql.Literal(window_start),
                        sql.Literal(window_end),
                    )
                )
            elif step == "verify" and _catalog_state(hot, route.parent, route.leaf) is not CatalogLeafState.ATTACHED:
                raise RecoveryError(f"leaf did not reattach: {route.leaf}")
        if _catalog_state(hot, route.parent, route.leaf) is not CatalogLeafState.ATTACHED:
            raise RecoveryError(f"leaf remains detached after recovery: {route.leaf}")
        recovered.append({"parent": route.parent, "leaf": route.leaf, "initial_state": state.value})

    with warm.transaction():
        _refresh_statement_timeout(warm, deadline)
        table_count = warm.execute(
            """
            UPDATE public.flip_table_states
            SET state='reattached', detach_finished_at=COALESCE(detach_finished_at, clock_timestamp())
            WHERE attempt_epoch=%s
            """,
            (attempt_epoch,),
        ).rowcount
        _refresh_statement_timeout(warm, deadline)
        attempt_count = warm.execute(
            """
            UPDATE public.flip_attempts
            SET state='reverted', updated_at=clock_timestamp()
            WHERE attempt_epoch=%s AND state IN ('locked', 'drained', 'recovering')
            """,
            (attempt_epoch,),
        ).rowcount
        _refresh_statement_timeout(warm, deadline)
        tracker_count = warm.execute(
            """
            UPDATE public.partition_tracker
            SET state='hot_primary', attempt_epoch=NULL, version=version+1, updated_at=clock_timestamp()
            WHERE cell=%s AND timeslot=%s AND attempt_epoch=%s AND state IN ('locked', 'drained', 'recovering')
            """,
            (manifest.cell, manifest.timeslot, attempt_epoch),
        ).rowcount
        if table_count != len(manifest.tables) or attempt_count != 1 or tracker_count != 1:
            raise RecoveryError(
                "revert CAS failed; refusing to claim hot_primary without exact attempt ownership"
            )
        _refresh_statement_timeout(warm, deadline)
    _remaining_ms(deadline)
    return tuple(recovered)
