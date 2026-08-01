import threading
import unittest

from flipbench.workload import MixedWorkload, WorkloadMix


class WorkloadMixTests(unittest.TestCase):
    def test_accepts_active_heavy_retiring_light_profile(self) -> None:
        mix = WorkloadMix(active_events_per_table=100, retiring_events_per_table=5)
        self.assertEqual(mix.active_events_per_table, 100)
        self.assertEqual(mix.retiring_events_per_table, 5)
        with self.assertRaises(ValueError):
            WorkloadMix(active_events_per_table=5, retiring_events_per_table=10)

    def test_retiring_park_does_not_stop_active_writer(self) -> None:
        active_committed = threading.Event()

        def active_batch() -> int:
            active_committed.set()
            threading.Event().wait(0.001)
            return 10

        def retiring_batch() -> int:
            raise RuntimeError("hot writer parked: ownership state is locked")

        workload = MixedWorkload.start(active_batch, retiring_batch, max_batches=10_000)
        self.assertTrue(active_committed.wait(1.0))
        retiring_total = workload.stop_retiring(2.0)
        self.assertEqual(retiring_total, 0)
        self.assertTrue(workload.active_is_alive())
        self.assertGreaterEqual(workload.stop_active(2.0), 10)


if __name__ == "__main__":
    unittest.main()
