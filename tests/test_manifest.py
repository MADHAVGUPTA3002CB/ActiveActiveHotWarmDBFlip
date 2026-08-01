import unittest

from flipbench.core import ManifestError, build_manifest, canonical_manifest_json, validate_manifest


class ManifestTests(unittest.TestCase):
    def test_builds_supported_table_counts_deterministically(self) -> None:
        for count in (5, 10, 15, 20):
            with self.subTest(count=count):
                manifest = build_manifest(count, "cell01", "retiring")
                validate_manifest(manifest)
                self.assertEqual(len(manifest.tables), count)
                independent = build_manifest(count, "cell01", "retiring")
                self.assertEqual(canonical_manifest_json(manifest), canonical_manifest_json(independent))

    def test_rejects_unsupported_count(self) -> None:
        with self.assertRaises(ManifestError):
            build_manifest(6, "cell01", "retiring")

    def test_topics_are_leaf_specific_and_single_partition(self) -> None:
        manifest = build_manifest(5, "cell01", "retiring")
        first = manifest.tables[0]
        self.assertEqual(first.parent, "bench_table_01")
        self.assertEqual(first.leaf, "bench_table_01_p_retiring")
        self.assertEqual(first.topic, "cards.cell01.public.bench_table_01_p_retiring")
        self.assertEqual(first.partition, 0)


if __name__ == "__main__":
    unittest.main()
