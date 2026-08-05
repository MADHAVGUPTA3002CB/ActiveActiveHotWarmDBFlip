import unittest
import uuid
from importlib.util import find_spec
from unittest.mock import patch

from flipbench.core import build_manifest
from flipbench.lifecycle import lifecycle_lock_name, validate_timeslot

if find_spec("psycopg") is not None:
    import flipbench.postgres_io as postgres_io

    GuardedTransactionSession = postgres_io.GuardedTransactionSession
    HotFencedTransactionSession = getattr(
        postgres_io, "HotFencedTransactionSession", None
    )
    OptimisticDetachTransactionSession = getattr(
        postgres_io, "OptimisticDetachTransactionSession", None
    )
    UpdateTarget = getattr(postgres_io, "UpdateTarget", None)
else:
    GuardedTransactionSession = None
    HotFencedTransactionSession = None
    OptimisticDetachTransactionSession = None
    UpdateTarget = None


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Cursor:
    def __init__(self) -> None:
        self.rows = ()
        self.statement = None
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def executemany(self, statement, rows) -> None:
        self.statement = statement
        self.rows = tuple(rows)
        self.rowcount = len(self.rows)


class _Connection:
    def __init__(
        self,
        warm: bool,
        *,
        fail_state_query: bool = False,
        unlock_result: tuple[bool] = (True,),
    ) -> None:
        self.warm = warm
        self.fail_state_query = fail_state_query
        self.unlock_result = unlock_result
        self.cursor_instance = _Cursor()
        self.commits = 0
        self.rollbacks = 0
        self.unlocks = 0
        self.closed = False

    def execute(self, statement, _params=()):
        text = str(statement)
        if "pg_try_advisory_lock_shared" in text:
            return _Result((True,))
        if "SELECT state" in text:
            if self.fail_state_query:
                raise RuntimeError("tracker unavailable")
            return _Result(("hot_primary",))
        if "pg_advisory_unlock_shared" in text:
            self.unlocks += 1
            return _Result(self.unlock_result)
        return _Result((True,))

    def cursor(self):
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class _DatabaseFenceError(Exception):
    sqlstate = "P0001"


class _HotFencedConnection:
    def __init__(self, *, fence_error: BaseException | None = None) -> None:
        self.fence_error = fence_error
        self.operations: list[tuple[object, tuple[object, ...]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, statement, params=()):
        self.operations.append((statement, tuple(params)))
        if self.fence_error is not None:
            raise self.fence_error
        rows = params[-1] if params and isinstance(params[-1], int) else 3
        return _Result((rows,))

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class WriterScopeTests(unittest.TestCase):
    def test_lifecycle_lock_key_is_timeslot_scoped(self) -> None:
        retiring = lifecycle_lock_name("cell01", "retiring")
        active = lifecycle_lock_name("cell01", "active")
        self.assertEqual(retiring, "flipbench:cell01:retiring")
        self.assertEqual(active, "flipbench:cell01:active")
        self.assertNotEqual(retiring, active)

    def test_unknown_timeslot_fails_before_database_io(self) -> None:
        self.assertEqual(validate_timeslot("retiring"), "retiring")
        self.assertEqual(validate_timeslot("active"), "active")
        with self.assertRaises(ValueError):
            validate_timeslot("typo")

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_hot_fenced_session_opens_only_hot_and_commits_one_guarded_operation(self) -> None:
        hot = _HotFencedConnection()
        manifest = build_manifest(5, "cell01", "retiring")
        self.assertIsNotNone(HotFencedTransactionSession)

        with patch("flipbench.postgres_io.connect", return_value=hot) as connect_mock:
            session = HotFencedTransactionSession(  # type: ignore[misc,operator]
                "hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
            )
            inserted = session.write(2, 3)
            session.close()

        connect_mock.assert_called_once()
        self.assertEqual(connect_mock.call_args.args[0], "hot-dsn")
        self.assertEqual(inserted, 3)
        self.assertEqual(len(hot.operations), 1)
        self.assertIn("guarded", str(hot.operations[0][0]).lower())
        self.assertEqual(hot.commits, 1)
        self.assertEqual(hot.rollbacks, 0)
        self.assertTrue(hot.closed)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_hot_fenced_session_parks_and_rolls_back_on_database_fence_error(self) -> None:
        hot = _HotFencedConnection(
            fence_error=_DatabaseFenceError("hot writer parked: ownership gate is parked")
        )
        manifest = build_manifest(5, "cell01", "retiring")
        self.assertIsNotNone(HotFencedTransactionSession)

        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = HotFencedTransactionSession(  # type: ignore[misc,operator]
                "hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
            )
            with self.assertRaisesRegex(RuntimeError, "hot writer parked"):
                session.write(0, 1)
            session.close()

        self.assertEqual(len(hot.operations), 1)
        self.assertEqual(hot.commits, 0)
        self.assertEqual(hot.rollbacks, 1)
        self.assertTrue(hot.closed)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_hot_fenced_session_updates_seeded_rows_with_the_same_epoch_guard(self) -> None:
        hot = _HotFencedConnection()
        manifest = build_manifest(5, "cell01", "retiring")
        targets = tuple(
            tuple(
                UpdateTarget(uuid.uuid4(), postgres_io.RETIRING_START)  # type: ignore[misc,operator]
                for _ in range(6)
            )
            for _ in manifest.tables
        )

        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = HotFencedTransactionSession(  # type: ignore[misc,operator]
                "hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
                update_targets_by_table=targets,
            )
            updated = session.update(2, 3, 0)
            session.close()

        statement, parameters = hot.operations[0]
        self.assertEqual(updated, 3)
        self.assertIn("update_events", str(statement))
        self.assertEqual(parameters[2], 7)
        self.assertEqual(
            [row["id"] for row in parameters[-1].obj],
            [str(target.id) for target in targets[2][:3]],
        )
        self.assertEqual(hot.commits, 1)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_h_state_only_batch_admission_does_not_send_an_epoch(self) -> None:
        hot = _HotFencedConnection()
        manifest = build_manifest(5, "cell01", "retiring")
        self.assertIsNotNone(OptimisticDetachTransactionSession)

        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = OptimisticDetachTransactionSession(  # type: ignore[misc,operator]
                "hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=None,
                operations_per_batch=5,
                admission_check_mode="state_only_v1",
            )
            inserted = session.write(0, 3)
            session.close()

        statement, parameters = hot.operations[0]
        self.assertEqual(inserted, 3)
        self.assertIn("admit_optimistic_batch_state_only", str(statement))
        self.assertEqual(len(parameters), 6)
        self.assertNotIn(7, parameters)
        self.assertEqual(hot.commits, 1)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_h_updates_seeded_rows_and_checks_state_only_once_per_api_batch(self) -> None:
        hot = _HotFencedConnection()
        manifest = build_manifest(5, "cell01", "retiring")
        self.assertIsNotNone(OptimisticDetachTransactionSession)
        self.assertIsNotNone(UpdateTarget)
        targets = tuple(
            tuple(
                UpdateTarget(uuid.uuid4(), postgres_io.RETIRING_START)  # type: ignore[misc,operator]
                for _ in range(6)
            )
            for _ in manifest.tables
        )

        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = OptimisticDetachTransactionSession(  # type: ignore[misc,operator]
                "hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=None,
                operations_per_batch=5,
                admission_check_mode="state_only_v1",
                update_targets_by_table=targets,
            )
            first = session.update(0, 3, 0)
            second = session.update(1, 3, 0)
            session.close()

        first_statement, first_parameters = hot.operations[0]
        second_statement, second_parameters = hot.operations[1]
        self.assertEqual((first, second), (3, 3))
        self.assertIn("admit_optimistic_batch_state_only", str(first_statement))
        self.assertIn("update_events_optimistic", str(first_statement))
        self.assertNotIn("admit_optimistic_batch", str(second_statement))
        self.assertIn("update_events_optimistic", str(second_statement))
        first_rows = first_parameters[-1].obj
        second_rows = second_parameters[-1].obj
        self.assertEqual(
            [row["id"] for row in first_rows],
            [str(target.id) for target in targets[0][:3]],
        )
        self.assertEqual(
            [row["id"] for row in second_rows],
            [str(target.id) for target in targets[1][:3]],
        )
        self.assertEqual(hot.commits, 2)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_h_update_rotation_wraps_around_seed_pool(self) -> None:
        hot = _HotFencedConnection()
        manifest = build_manifest(5, "cell01", "retiring")
        targets = tuple(
            tuple(
                UpdateTarget(uuid.uuid4(), postgres_io.RETIRING_START)  # type: ignore[misc,operator]
                for _ in range(3)
            )
            for _ in manifest.tables
        )

        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = OptimisticDetachTransactionSession(  # type: ignore[misc,operator]
                "hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=None,
                operations_per_batch=5,
                admission_check_mode="state_only_v1",
                update_targets_by_table=targets,
            )
            session.update(0, 3, 1)
            session.close()

        rows = hot.operations[0][1][-1].obj
        self.assertEqual(
            [row["id"] for row in rows],
            [str(targets[0][0].id), str(targets[0][1].id), str(targets[0][2].id)],
        )

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_target_rate_session_commits_one_selected_table_transaction(self) -> None:
        warm = _Connection(warm=True)
        hot = _Connection(warm=False)
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", side_effect=[warm, hot]):
            session = GuardedTransactionSession(  # type: ignore[misc]
                "hot-dsn",
                "warm-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
            )
            inserted = session.write(2, 3)
            session.close()
        self.assertEqual(inserted, 3)
        self.assertEqual(hot.commits, 1)
        self.assertEqual(len(hot.cursor_instance.rows), 3)
        self.assertEqual(warm.unlocks, 1)
        self.assertTrue(hot.closed)
        self.assertTrue(warm.closed)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_warm_tracker_session_updates_seeded_rows_under_the_same_shared_guard(self) -> None:
        warm = _Connection(warm=True)
        hot = _Connection(warm=False)
        manifest = build_manifest(5, "cell01", "retiring")
        targets = tuple(
            tuple(
                UpdateTarget(uuid.uuid4(), postgres_io.RETIRING_START)  # type: ignore[misc,operator]
                for _ in range(4)
            )
            for _ in manifest.tables
        )
        with patch("flipbench.postgres_io.connect", side_effect=[warm, hot]):
            session = GuardedTransactionSession(  # type: ignore[misc,operator]
                "hot-dsn",
                "warm-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                update_targets_by_table=targets,
            )
            updated = session.update(1, 2, 0)
            session.close()

        self.assertEqual(updated, 2)
        self.assertIn("UPDATE", str(hot.cursor_instance.statement))
        self.assertEqual(
            [row[1] for row in hot.cursor_instance.rows],
            [target.id for target in targets[1][:2]],
        )
        self.assertEqual(hot.commits, 1)
        self.assertEqual(warm.unlocks, 1)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_target_rate_session_releases_guard_when_tracker_query_fails(self) -> None:
        warm = _Connection(warm=True, fail_state_query=True)
        hot = _Connection(warm=False)
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", side_effect=[warm, hot]):
            session = GuardedTransactionSession(  # type: ignore[misc]
                "hot-dsn",
                "warm-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
            )
            with self.assertRaisesRegex(RuntimeError, "tracker unavailable"):
                session.write(2, 3)
            session.close()
        self.assertEqual(warm.unlocks, 1)
        self.assertEqual(hot.commits, 0)

    @unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
    def test_target_rate_session_closes_connections_when_unlock_is_false(self) -> None:
        from flipbench.traffic import CommittedTrafficWorkerError

        warm = _Connection(warm=True, unlock_result=(False,))
        hot = _Connection(warm=False)
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", side_effect=[warm, hot]):
            session = GuardedTransactionSession(  # type: ignore[misc]
                "hot-dsn", "warm-dsn", manifest, uuid.uuid4(), "retiring", 256
            )
            with self.assertRaises(CommittedTrafficWorkerError):
                session.write(0, 1)
        self.assertEqual(hot.commits, 1)
        self.assertTrue(hot.closed)
        self.assertTrue(warm.closed)


if __name__ == "__main__":
    unittest.main()
