from __future__ import annotations

import unittest
import uuid

from flipbench.core import SourceProofMode, build_leaf_fence_markers, build_manifest


class LeafFenceContractTests(unittest.TestCase):
    def test_marker_plan_is_deterministic_complete_and_leaf_scoped(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        attempt_id = uuid.uuid4()

        first = build_leaf_fence_markers(manifest, attempt_id, 9)
        second = build_leaf_fence_markers(manifest, attempt_id, 9)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 5)
        self.assertEqual({marker.partition.topic for marker in first}, {route.topic for route in manifest.tables})
        self.assertEqual({marker.partition.partition for marker in first}, {0})
        self.assertEqual(len({marker.marker_id for marker in first}), 5)
        self.assertEqual({marker.attempt_epoch for marker in first}, {9})

    def test_source_proof_modes_keep_lsn_control_and_marker_variant_separate(self) -> None:
        self.assertEqual(SourceProofMode.SLOT_LSN.value, "slot_lsn_v1")
        self.assertEqual(SourceProofMode.PER_LEAF_MARKER.value, "per_leaf_marker_v1")
        self.assertEqual(
            SourceProofMode.ATOMIC_DETACH_MARKER.value,
            "atomic_detach_marker_v1",
        )
        self.assertEqual(
            SourceProofMode.PARALLEL_ATOMIC_DETACH_MARKER.value,
            "parallel_atomic_detach_marker_v1",
        )


if __name__ == "__main__":
    unittest.main()
