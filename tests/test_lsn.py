import unittest

from flipbench.core import HotSourceIdentity, LsnError, format_lsn, parse_lsn, source_fence_satisfied


class LsnTests(unittest.TestCase):
    def test_parses_full_unsigned_range(self) -> None:
        self.assertEqual(parse_lsn("0/0"), 0)
        self.assertEqual(parse_lsn("0/1"), 1)
        self.assertEqual(parse_lsn("1/0"), 1 << 32)
        self.assertEqual(parse_lsn("FFFFFFFF/FFFFFFFF"), (1 << 64) - 1)

    def test_format_round_trips_canonically(self) -> None:
        self.assertEqual(format_lsn(parse_lsn("0000000a/0000000b")), "A/B")

    def test_compares_numerically_and_checks_source_identity(self) -> None:
        expected = HotSourceIdentity("cell01", "hot-system-1", "cards", "flipbench_slot")
        observed = HotSourceIdentity("cell01", "hot-system-1", "cards", "flipbench_slot")
        self.assertFalse(source_fence_satisfied(expected, observed, "0/FFFFFFFF", "1/0"))
        self.assertTrue(source_fence_satisfied(expected, observed, "1/0", "0/FFFFFFFF"))

    def test_rejects_invalid_lsn(self) -> None:
        for value in ("", "0", "0/1/2", "-1/0", "0/G", "100000000/0", " 0/1"):
            with self.subTest(value=value), self.assertRaises(LsnError):
                parse_lsn(value)

    def test_identity_mismatch_fails_closed(self) -> None:
        expected = HotSourceIdentity("cell01", "hot-system-1", "cards", "flipbench_slot")
        observed = HotSourceIdentity("cell01", "warm-system", "cards", "flipbench_slot")
        with self.assertRaises(LsnError):
            source_fence_satisfied(expected, observed, "2/0", "1/0")


if __name__ == "__main__":
    unittest.main()
