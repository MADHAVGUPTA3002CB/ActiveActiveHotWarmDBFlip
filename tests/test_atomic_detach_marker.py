from __future__ import annotations

import contextlib
import importlib.util
import unittest
import uuid

from flipbench.core import build_leaf_fence_markers, build_manifest


PSYCOPG_AVAILABLE = importlib.util.find_spec("psycopg") is not None


class _Result:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _RecordingConnection:
    def __init__(self, expected_marker: object, ownership_epoch: int) -> None:
        self.expected_marker = expected_marker
        self.ownership_epoch = ownership_epoch
        self.events: list[str] = []

    @contextlib.contextmanager
    def transaction(self):
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")

    def execute(self, query: object, params: object = None) -> _Result:
        rendered = str(query)
        if "DETACH" in rendered:
            self.events.append("detach")
            return _Result()
        if "INSERT" in rendered:
            self.events.append("marker_insert")
            return _Result()
        if "SELECT" in rendered:
            self.events.append("marker_verify")
            marker = self.expected_marker
            return _Result(
                (
                    marker.marker_id,
                    marker.attempt_epoch,
                    self.ownership_epoch,
                    marker.cell,
                    marker.timeslot,
                    marker.parent,
                    marker.leaf,
                )
            )
        raise AssertionError(f"unexpected SQL: {rendered}")


@unittest.skipUnless(PSYCOPG_AVAILABLE, "psycopg is installed in the runner image")
class AtomicDetachMarkerTests(unittest.TestCase):
    def test_detach_and_marker_are_ordered_in_one_transaction(self) -> None:
        from flipbench.postgres_io import atomic_detach_and_emit_leaf_fence_marker

        manifest = build_manifest(5, "cell01", "retiring")
        marker = build_leaf_fence_markers(manifest, uuid.uuid4(), 7)[0]
        connection = _RecordingConnection(marker, ownership_epoch=3)

        atomic_detach_and_emit_leaf_fence_marker(
            connection,
            marker,
            ownership_epoch=3,
        )

        self.assertEqual(
            connection.events,
            ["begin", "detach", "marker_insert", "marker_verify", "commit"],
        )

    def test_invalid_epoch_fails_before_opening_transaction(self) -> None:
        from flipbench.postgres_io import atomic_detach_and_emit_leaf_fence_marker

        manifest = build_manifest(5, "cell01", "retiring")
        marker = build_leaf_fence_markers(manifest, uuid.uuid4(), 7)[0]
        connection = _RecordingConnection(marker, ownership_epoch=1)

        with self.assertRaisesRegex(ValueError, "ownership_epoch"):
            atomic_detach_and_emit_leaf_fence_marker(
                connection,
                marker,
                ownership_epoch=0,
            )

        self.assertEqual(connection.events, [])


if __name__ == "__main__":
    unittest.main()
