import unittest

from flipbench.core import OffsetError, OffsetVector, TopicPartition, offset_gate


class OffsetGateTests(unittest.TestCase):
    def test_requires_every_partition_component_wise(self) -> None:
        a = TopicPartition("leaf_a", 0)
        b = TopicPartition("leaf_b", 0)
        target = OffsetVector("cell01", "slot-a", 4, {a: 10, b: 20})
        current = OffsetVector("cell01", "slot-a", 4, {a: 10, b: 19})

        result = offset_gate((a, b), target, current)

        self.assertFalse(result.ready)
        self.assertEqual(result.behind, {b: 1})

    def test_equal_or_greater_next_offset_passes(self) -> None:
        partition = TopicPartition("leaf", 0)
        target = OffsetVector("cell01", "slot-a", 4, {partition: 10})
        current = OffsetVector("cell01", "slot-a", 4, {partition: 10})
        self.assertTrue(offset_gate((partition,), target, current).ready)

    def test_topics_are_independent_and_manifest_must_be_unique(self) -> None:
        a = TopicPartition("leaf_a", 0)
        b = TopicPartition("leaf_b", 0)
        target = OffsetVector("cell01", "slot-a", 4, {a: 10, b: 20})
        current = OffsetVector("cell01", "slot-a", 4, {a: 10, b: 20, TopicPartition("extra", 0): 99})
        self.assertTrue(offset_gate((a, b), target, current).ready)
        for manifest in ((), (a, a)):
            with self.subTest(manifest=manifest), self.assertRaises(OffsetError):
                offset_gate(manifest, target, current)

    def test_missing_current_is_pending_but_missing_target_is_corrupt(self) -> None:
        partition = TopicPartition("leaf", 0)
        empty = OffsetVector("cell01", "slot-a", 4, {})
        target = OffsetVector("cell01", "slot-a", 4, {partition: 10})
        self.assertEqual(offset_gate((partition,), target, empty).missing, (partition,))
        with self.assertRaises(OffsetError):
            offset_gate((partition,), empty, target)

    def test_empty_topic_with_zero_target_needs_no_committed_offset(self) -> None:
        partition = TopicPartition("empty_leaf", 0)
        target = OffsetVector("cell01", "slot-a", 4, {partition: 0})
        current = OffsetVector("cell01", "slot-a", 4, {})
        self.assertTrue(offset_gate((partition,), target, current).ready)

    def test_attempt_mismatch_fails_closed(self) -> None:
        partition = TopicPartition("leaf", 0)
        target = OffsetVector("cell01", "slot-a", 4, {partition: 10})
        stale = OffsetVector("cell01", "slot-a", 3, {partition: 10})
        with self.assertRaises(OffsetError):
            offset_gate((partition,), target, stale)

    def test_rejects_invalid_partition_or_offset(self) -> None:
        with self.assertRaises(OffsetError):
            TopicPartition("leaf", -1)
        partition = TopicPartition("leaf", 0)
        with self.assertRaises(OffsetError):
            OffsetVector("cell01", "slot-a", 1, {partition: -1})
        with self.assertRaises(OffsetError):
            OffsetVector("cell01", "slot-a", 1, {partition: 1 << 63})


if __name__ == "__main__":
    unittest.main()
