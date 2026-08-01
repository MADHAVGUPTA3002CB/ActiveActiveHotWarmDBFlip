import importlib.util
import unittest
from unittest.mock import Mock, patch

from flipbench.core import OffsetError, TopicPartition


HAS_CONFLUENT_KAFKA = importlib.util.find_spec("confluent_kafka") is not None


@unittest.skipUnless(HAS_CONFLUENT_KAFKA, "confluent-kafka is installed in the runner image")
class KafkaControlTests(unittest.TestCase):
    def test_validate_topics_rejects_wrong_replication_factor_or_min_isr(self) -> None:
        from confluent_kafka.admin import ConfigResource

        from flipbench.kafka_io import KafkaControl, TopicSpec

        partition = Mock(replicas=[1])
        topic = Mock(error=None, partitions={0: partition})
        control = KafkaControl.__new__(KafkaControl)
        control._admin = Mock()
        control._admin.list_topics.return_value = Mock(topics={"topic-a": topic})

        with self.assertRaises(OffsetError):
            control.validate_topics((TopicSpec("topic-a", replication_factor=3),))

        partition.replicas = [1, 2, 3]
        config_entry = Mock(value="1")
        future = Mock()
        future.result.return_value = {"min.insync.replicas": config_entry}
        control._admin.describe_configs.return_value = {
            ConfigResource(ConfigResource.Type.TOPIC, "topic-a"): future
        }
        with self.assertRaises(OffsetError):
            control.validate_topics(
                (TopicSpec("topic-a", replication_factor=3, min_insync_replicas=2),)
            )

    def test_ensure_topics_retries_transient_metadata_gap(self) -> None:
        from flipbench.kafka_io import KafkaControl, TopicSpec

        future = Mock()
        future.result.return_value = None
        control = KafkaControl.__new__(KafkaControl)
        control._admin = Mock()
        control._admin.create_topics.return_value = {"topic-a": future}

        with patch.object(
            KafkaControl,
            "validate_topics",
            side_effect=(OffsetError("topic missing"), None),
        ) as validate, patch("flipbench.kafka_io.time.sleep"):
            control.ensure_topics((TopicSpec("topic-a"),))

        self.assertEqual(validate.call_count, 2)

    def test_committed_offsets_budgets_timeout_across_worker_waves(self) -> None:
        from flipbench.kafka_io import KafkaControl

        control = KafkaControl.__new__(KafkaControl)
        consumers = {f"group-{index}": Mock() for index in range(20)}
        for consumer in consumers.values():
            consumer.committed.side_effect = lambda query, timeout: query
        control._group_consumers = consumers
        group_by_partition = {
            TopicPartition(f"topic-{index}", 0): f"group-{index}"
            for index in range(20)
        }

        control.committed_offsets(group_by_partition, timeout_seconds=9.0)

        for consumer in consumers.values():
            self.assertEqual(consumer.committed.call_args.kwargs["timeout"], 3.0)


if __name__ == "__main__":
    unittest.main()
