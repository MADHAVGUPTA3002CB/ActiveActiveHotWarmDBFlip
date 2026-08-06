from __future__ import annotations

import re

from .core import ManifestError


_TIMESLOTS = frozenset(("retiring", "active"))

# H-DD-Prod rolling generations: one 12-hour timeslot per generation, named by its
# UTC window start, e.g. "g2026_08_01_12". The pattern is deliberately exact so a
# generation identifier can never collide with the fixed benchmark timeslots or
# escape PostgreSQL identifier safety.
GENERATION_TIMESLOT_PATTERN = re.compile(r"^g[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}$")


def is_generation_timeslot(timeslot: str) -> bool:
    return isinstance(timeslot, str) and GENERATION_TIMESLOT_PATTERN.fullmatch(timeslot) is not None


def validate_timeslot(timeslot: str) -> str:
    if timeslot in _TIMESLOTS or is_generation_timeslot(timeslot):
        return timeslot
    raise ManifestError(f"unknown benchmark timeslot: {timeslot!r}")


def lifecycle_lock_name(cell: str, timeslot: str) -> str:
    if not cell:
        raise ManifestError("cell is required for a lifecycle lock")
    return f"flipbench:{cell}:{validate_timeslot(timeslot)}"
