import threading
import unittest

from flipbench.overload import BackgroundBatchWriter


class BackgroundBatchWriterTests(unittest.TestCase):
    def test_counts_batches_and_stops_cooperatively(self) -> None:
        first_batch = threading.Event()

        def write_batch() -> int:
            first_batch.set()
            threading.Event().wait(0.001)
            return 25

        writer = BackgroundBatchWriter.start(write_batch, max_batches=10_000)
        self.assertTrue(first_batch.wait(1.0))
        inserted = writer.stop_and_join(timeout_seconds=2.0)

        self.assertGreaterEqual(inserted, 25)
        self.assertFalse(writer.is_alive())

    def test_expected_writer_park_is_a_clean_stop(self) -> None:
        def parked() -> int:
            raise RuntimeError("hot writer parked: ownership state is locked")

        writer = BackgroundBatchWriter.start(parked, max_batches=1)
        self.assertEqual(writer.stop_and_join(timeout_seconds=2.0), 0)

    def test_unexpected_writer_error_is_reported(self) -> None:
        def broken() -> int:
            raise RuntimeError("database disconnected")

        writer = BackgroundBatchWriter.start(broken, max_batches=1)
        with self.assertRaisesRegex(RuntimeError, "database disconnected"):
            writer.stop_and_join(timeout_seconds=2.0)

    def test_max_batch_exhaustion_is_not_a_clean_overload_stop(self) -> None:
        writer = BackgroundBatchWriter.start(lambda: 5, max_batches=1)
        waiter = threading.Event()
        for _ in range(1000):
            if not writer.is_alive():
                break
            waiter.wait(0.001)
        with self.assertRaisesRegex(RuntimeError, "exhausted maximum batches"):
            writer.stop_and_join(timeout_seconds=2.0)


if __name__ == "__main__":
    unittest.main()
