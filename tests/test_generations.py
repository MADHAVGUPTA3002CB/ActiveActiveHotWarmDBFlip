from __future__ import annotations

import re
import unittest
from datetime import datetime, timedelta, timezone
from importlib.util import find_spec
from pathlib import Path

from flipbench.core import ManifestError
from flipbench.lifecycle import is_generation_timeslot, validate_timeslot
from flipbench.settings import Settings


def lanes_settings() -> Settings:
    return Settings(
        hot_dsn="postgresql://hot/cards",
        warm_dsn="postgresql://warm/cards",
        kafka_bootstrap="kafka:19092",
        source_connect_url="http://source:8083",
        sink_connect_url="http://sink:8083",
        postgres_password="test-only",
        table_count=5,
        results_dir=Path("results"),
        source_topology="lanes",
    )


class GenerationTimeslotTests(unittest.TestCase):
    def test_generation_timeslots_are_accepted_and_bounded(self) -> None:
        self.assertTrue(is_generation_timeslot("g2026_08_01_12"))
        self.assertEqual(validate_timeslot("g2026_08_01_12"), "g2026_08_01_12")
        self.assertEqual(validate_timeslot("retiring"), "retiring")
        self.assertEqual(validate_timeslot("active"), "active")
        for bad in ("g2026_08_01", "G2026_08_01_12", "g2026_08_01_12x", "future", ""):
            with self.assertRaises(ManifestError):
                validate_timeslot(bad)


class LaneSourceSpecTests(unittest.TestCase):
    def test_two_persistent_lanes_with_unique_identities(self) -> None:
        from flipbench.connector_configs import lane_source_specs
        from flipbench.core import build_manifest

        settings = lanes_settings()
        manifest = build_manifest(5, "cell01", "retiring")
        specs = lane_source_specs(settings, manifest)
        self.assertEqual(tuple(spec.lane for spec in specs), ("lane_a", "lane_b"))
        for field in ("connector_name", "slot_name", "publication_name", "topic_prefix", "heartbeat_table"):
            values = [getattr(spec, field) for spec in specs]
            self.assertEqual(len(set(values)), 2, field)
        for spec in specs:
            config = dict(spec.config)
            self.assertEqual(config["publication.autocreate.mode"], "disabled")
            self.assertEqual(config["exactly.once.support"], "required")
            self.assertEqual(config["snapshot.mode"], "no_data")
            include = config["table.include.list"].split(",")
            generation_leaf = "public\\.bench_table_[0-9]{2}_p_g[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}"
            self.assertIn(generation_leaf, include)
            self.assertTrue(re.fullmatch(generation_leaf, "public.bench_table_01_p_g2026_08_01_12"))
            self.assertFalse(re.fullmatch(generation_leaf, "public.bench_table_01_p_retiring"))

    def test_lanes_sink_config_covers_every_generation(self) -> None:
        from flipbench.connector_configs import lanes_sink_config
        from flipbench.core import build_manifest

        settings = lanes_settings()
        manifest = build_manifest(5, "cell01", "retiring")
        config = lanes_sink_config(settings, manifest)
        topic_pattern = re.compile(config["topics.regex"])
        self.assertTrue(topic_pattern.fullmatch("cards.cell01.public.bench_table_03_p_g2026_08_02_00"))
        self.assertFalse(topic_pattern.fullmatch("cards.cell01.public.bench_table_03_p_retiring"))
        self.assertFalse(topic_pattern.fullmatch("cards.cell01.public.dbz_heartbeat_lane_a"))
        router = re.compile(config["transforms.route.regex"])
        match = router.fullmatch("cards.cell01.public.bench_table_03_p_g2026_08_02_00")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "bench_table_03")
        self.assertEqual(config["consumer.override.metadata.max.age.ms"], "5000")

    def test_source_specs_dispatches_lanes_topology(self) -> None:
        from flipbench.connector_configs import source_specs
        from flipbench.core import build_manifest

        specs = source_specs(lanes_settings(), build_manifest(5, "cell01", "retiring"))
        self.assertEqual(tuple(spec.lane for spec in specs), ("lane_a", "lane_b"))


@unittest.skipUnless(find_spec("psycopg") is not None, "psycopg is installed in the runner image")
class GenerationSpecTests(unittest.TestCase):
    def test_generation_math_and_lane_pinning_alternate(self) -> None:
        from flipbench.generations import (
            GENERATION_BASE,
            GenerationSpec,
            generation_timeslot,
            generation_window,
            lane_for_generation,
        )

        self.assertEqual(GENERATION_BASE, datetime(2026, 8, 1, 12, tzinfo=timezone.utc))
        self.assertEqual(generation_timeslot(0), "g2026_08_01_12")
        self.assertEqual(generation_timeslot(1), "g2026_08_02_00")
        start, end = generation_window(2)
        self.assertEqual(start, GENERATION_BASE + timedelta(hours=24))
        self.assertEqual(end - start, timedelta(hours=12))
        self.assertEqual(
            [lane_for_generation(index) for index in range(4)],
            ["lane_a", "lane_b", "lane_a", "lane_b"],
        )

        spec = GenerationSpec.build(lanes_settings(), 1)
        self.assertEqual(spec.timeslot, "g2026_08_02_00")
        self.assertEqual(spec.lane, "lane_b")
        self.assertEqual(
            spec.manifest.tables[0].leaf, "bench_table_01_p_g2026_08_02_00"
        )
        self.assertEqual(
            spec.manifest.tables[0].topic,
            "cards.cell01.public.bench_table_01_p_g2026_08_02_00",
        )

    def test_generation_session_requires_explicit_timestamp(self) -> None:
        from flipbench.core import build_manifest
        import uuid

        from flipbench.postgres_io import OptimisticDetachTransactionSession

        manifest = build_manifest(5, "cell01", "g2026_08_02_00")
        from unittest.mock import patch

        with patch("flipbench.postgres_io.connect") as connect_mock:
            connect_mock.return_value = object()
            with self.assertRaisesRegex(ValueError, "explicit created_at"):
                OptimisticDetachTransactionSession(
                    "postgresql://hot/cards",
                    manifest,
                    uuid.uuid4(),
                    "g2026_08_02_00",
                    256,
                    None,
                    operations_per_batch=5,
                    admission_check_mode="state_only_v1",
                )
            OptimisticDetachTransactionSession(
                "postgresql://hot/cards",
                manifest,
                uuid.uuid4(),
                "g2026_08_02_00",
                256,
                None,
                operations_per_batch=5,
                admission_check_mode="state_only_v1",
                created_at=datetime(2026, 8, 2, 0, tzinfo=timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
