from __future__ import annotations

import unittest
import uuid
from importlib.util import find_spec
from unittest.mock import patch

from flipbench.core import build_manifest

if find_spec("psycopg") is not None:
    from flipbench.postgres_io import (
        HotFencedTransactionSession,
        OptimisticDetachTransactionSession,
        bind_hot_write_gate_attempt,
        park_hot_write_gate,
        reopen_hot_write_gate,
    )
else:
    HotFencedTransactionSession = None
    OptimisticDetachTransactionSession = None


class _Result:
    def __init__(self, row=(3,), rowcount=1) -> None:
        self._row = row
        self.rowcount = rowcount

    def fetchone(self):
        return self._row


class _HotConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.result = _Result()
        self.results: list[_Result] = []

    def execute(self, statement, params=()):
        self.statements.append((str(statement), tuple(params)))
        return self.results.pop(0) if self.results else self.result

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
class HotFencedTransactionSessionTests(unittest.TestCase):
    def test_one_guarded_database_operation_and_commit_per_table_transaction(self) -> None:
        hot = _HotConnection()
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", return_value=hot) as connect_mock:
            session = HotFencedTransactionSession(  # type: ignore[misc]
                "writer-hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
            )
            inserted = session.write(2, 3)
            session.close()

        connect_mock.assert_called_once()
        self.assertEqual(inserted, 3)
        self.assertEqual(hot.commits, 1)
        self.assertEqual(len(hot.statements), 1)
        statement, parameters = hot.statements[0]
        self.assertIn("flipbench_guard.insert_events", statement)
        self.assertIn("bench_table_03", parameters)
        self.assertEqual(parameters[2], 7)
        self.assertTrue(hot.closed)

    def test_database_fence_rejection_parks_and_rolls_back(self) -> None:
        hot = _HotConnection()
        hot.result = RuntimeError("hot writer parked: gate is parked")

        def execute(statement, params=()):
            raise hot.result

        hot.execute = execute  # type: ignore[method-assign]
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = HotFencedTransactionSession(  # type: ignore[misc]
                "writer-hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
            )
            with self.assertRaisesRegex(RuntimeError, "hot writer parked"):
                session.write(0, 1)

        self.assertEqual(hot.commits, 0)
        self.assertEqual(hot.rollbacks, 1)


@unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
class OptimisticDetachTransactionSessionTests(unittest.TestCase):
    def test_one_admission_read_is_reused_across_separate_selected_table_commits(self) -> None:
        hot = _HotConnection()
        hot.results = [
            _Result(row=(3,)),
            _Result(row=(2,)),
            _Result(row=(1,)),
        ]
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", return_value=hot) as connect_mock:
            session = OptimisticDetachTransactionSession(  # type: ignore[misc]
                "writer-hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
                operations_per_batch=2,
            )
            inserted = (
                session.write(3, 3),
                session.write(4, 2),
                session.write(1, 1),
            )
            session.close()

        connect_mock.assert_called_once()
        self.assertEqual(inserted, (3, 2, 1))
        self.assertEqual(hot.commits, 3)
        self.assertEqual(len(hot.statements), 3)
        admission_statements = [
            item for item in hot.statements if "admit_optimistic_batch" in item[0]
        ]
        self.assertEqual(len(admission_statements), 2)
        self.assertEqual(admission_statements[0][1][:3], ("cell01", "retiring", 7))
        write_statements = [
            item for item in hot.statements if "insert_events_optimistic" in item[0]
        ]
        self.assertEqual(len(write_statements), 3)
        self.assertEqual(write_statements[0][1][5], manifest.tables[3].parent)
        self.assertEqual(len(write_statements[0][1][6].obj), 3)
        self.assertNotIn("partition_write_gates", "".join(item[0] for item in write_statements))
        self.assertTrue(hot.closed)

    def test_detach_race_preserves_earlier_batch_commit_and_aborts_failed_commit(self) -> None:
        hot = _HotConnection()
        call_count = 0

        def execute(statement, params=()):
            nonlocal call_count
            hot.statements.append((str(statement), tuple(params)))
            call_count += 1
            if call_count == 1:
                return _Result(row=(1,))
            raise RuntimeError("hot writer parked: optimistic detach race")

        hot.execute = execute  # type: ignore[method-assign]
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = OptimisticDetachTransactionSession(  # type: ignore[misc]
                "writer-hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
                operations_per_batch=5,
            )
            self.assertEqual(session.write(0, 1), 1)
            with self.assertRaisesRegex(RuntimeError, "optimistic detach race"):
                session.write(1, 1)

        self.assertEqual(hot.commits, 1)
        self.assertEqual(hot.rollbacks, 1)

    def test_parked_batch_admission_prevents_any_write(self) -> None:
        hot = _HotConnection()

        def execute(statement, params=()):
            hot.statements.append((str(statement), tuple(params)))
            raise RuntimeError("hot writer parked: optimistic batch admission rejected")

        hot.execute = execute  # type: ignore[method-assign]
        manifest = build_manifest(5, "cell01", "retiring")
        with patch("flipbench.postgres_io.connect", return_value=hot):
            session = OptimisticDetachTransactionSession(  # type: ignore[misc]
                "writer-hot-dsn",
                manifest,
                uuid.uuid4(),
                "retiring",
                256,
                expected_ownership_epoch=7,
                operations_per_batch=5,
            )
            with self.assertRaisesRegex(RuntimeError, "batch admission"):
                session.write(0, 1)

        self.assertEqual(len(hot.statements), 1)
        self.assertIn("admit_optimistic_batch", hot.statements[0][0])
        self.assertEqual(hot.commits, 0)
        self.assertEqual(hot.rollbacks, 1)


@unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in runner")
class HotFenceCasTests(unittest.TestCase):
    def test_park_bind_and_reopen_use_exact_attempt_identity(self) -> None:
        hot = _HotConnection()
        attempt_id = uuid.uuid4()

        park_hot_write_gate(hot, "cell01", "retiring", attempt_id, 3)  # type: ignore[misc]
        bind_hot_write_gate_attempt(hot, "cell01", "retiring", attempt_id, 9)  # type: ignore[misc]
        reopen_hot_write_gate(hot, "cell01", "retiring", attempt_id, 9)  # type: ignore[misc]

        self.assertEqual(len(hot.statements), 3)
        self.assertIn("state='open'", hot.statements[0][0])
        self.assertIn("park_attempt_id=%s", hot.statements[1][0])
        self.assertIn("ownership_epoch=ownership_epoch+1", hot.statements[2][0])
        self.assertIn(attempt_id, hot.statements[2][1])

    def test_failed_gate_cas_is_rejected(self) -> None:
        hot = _HotConnection()
        hot.result = _Result(row=None, rowcount=0)
        with self.assertRaisesRegex(RuntimeError, "hot write gate park CAS failed"):
            park_hot_write_gate(hot, "cell01", "retiring", uuid.uuid4(), 3)  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
