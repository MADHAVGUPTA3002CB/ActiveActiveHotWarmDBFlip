import unittest

from flipbench.lifecycle import lifecycle_lock_name, validate_timeslot


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


if __name__ == "__main__":
    unittest.main()
