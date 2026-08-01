from __future__ import annotations

from .core import ManifestError


_TIMESLOTS = frozenset(("retiring", "active"))


def validate_timeslot(timeslot: str) -> str:
    if timeslot not in _TIMESLOTS:
        raise ManifestError(f"unknown benchmark timeslot: {timeslot!r}")
    return timeslot


def lifecycle_lock_name(cell: str, timeslot: str) -> str:
    if not cell:
        raise ManifestError("cell is required for a lifecycle lock")
    return f"flipbench:{cell}:{validate_timeslot(timeslot)}"
