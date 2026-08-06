import importlib.util
import json
import unittest
import uuid
from unittest.mock import Mock, patch

from flipbench.core import LeafFenceMarker, OffsetError, TopicPartition


HAS_CONFLUENT_KAFKA = importlib.util.find_spec("confluent_kafka") is not None


@unittest.skipUnless(HAS_CONFLUENT_KAFKA, "confluent-kafka is installed in the runner image")
class KafkaControlTests(unittest.TestCase):
    def test_read_committed_target_offsets_ignore_transaction_control_gaps(self) -> None:
        from confluent_kafka import KafkaError

        from flipbench.kafka_io import KafkaControl

        first = TopicPartition("topic-a", 0)
        second = TopicPartition("topic-b", 0)

        def data_message(partition: TopicPartition, offset: int) -> Mock:
            message = Mock()
            message.error.return_value = None
            message.topic.return_value = partition.topic
            message.partition.return_value = partition.partition
            message.offset.return_value = offset
            return message

        def eof_message(partition: TopicPartition) -> Mock:
            error = Mock()
            error.code.return_value = KafkaError._PARTITION_EOF
            message = Mock()
            message.error.return_value = error
            message.topic.return_value = partition.topic
            message.partition.return_value = partition.partition
            return message

        consumer = Mock()
        consumer.poll.side_effect = (
            data_message(first, 102),
            eof_message(first),
            eof_message(second),
        )
        control = KafkaControl.__new__(KafkaControl)
        control._consumer = Mock(return_value=consumer)

        observed = control.read_committed_target_offsets(
            (first, second),
            {first: 100, second: 200},
            timeout_seconds=1,
        )

        # topic-a may contain an invisible transaction-control record at 103;
        # the correct sink target is still the next offset after visible data.
        self.assertEqual(observed, {first: 103, second: 200})
        control._consumer.assert_called_once_with(
            "flipbench-read-committed-target-observer",
            enable_partition_eof=True,
        )
        consumer.assign.assert_called_once()
        consumer.close.assert_called_once()

    def test_read_committed_target_offsets_require_complete_non_negative_starts(self) -> None:
        from flipbench.kafka_io import KafkaControl

        first = TopicPartition("topic-a", 0)
        second = TopicPartition("topic-b", 0)
        control = KafkaControl.__new__(KafkaControl)

        with self.assertRaises(OffsetError):
            control.read_committed_target_offsets(
                (first, second),
                {first: 0},
                timeout_seconds=1,
            )
        with self.assertRaises(OffsetError):
            control.read_committed_target_offsets(
                (first,),
                {first: -1},
                timeout_seconds=1,
            )

    def test_marker_observer_returns_exact_marker_next_offsets(self) -> None:
        from flipbench.connector_configs import FENCE_HEADER_NAME, FENCE_HEADER_VALUE
        from flipbench.kafka_io import KafkaControl

        attempt_id = uuid.uuid4()
        partitions = tuple(TopicPartition(f"topic-{index}", 0) for index in range(2))
        markers = tuple(
            LeafFenceMarker(
                partition=partition,
                parent=f"parent_{index}",
                leaf=f"leaf_{index}",
                cell="cell01",
                timeslot="retiring",
                marker_id=uuid.uuid5(attempt_id, f"leaf_{index}"),
                attempt_id=attempt_id,
                attempt_epoch=7,
            )
            for index, partition in enumerate(partitions)
        )

        def message(marker: LeafFenceMarker, offset: int) -> Mock:
            value = {
                "payload": {
                    "after": {
                        "marker_id": str(marker.marker_id),
                        "attempt_id": str(marker.attempt_id),
                        "attempt_epoch": marker.attempt_epoch,
                        "marker_schema_version": 1,
                        "cell": marker.cell,
                        "timeslot": marker.timeslot,
                        "parent_name": marker.parent,
                        "leaf_name": marker.leaf,
                    },
                    "source": {"schema": "flipbench_fence", "table": marker.leaf},
                }
            }
            result = Mock()
            result.error.return_value = None
            result.topic.return_value = marker.partition.topic
            result.partition.return_value = marker.partition.partition
            result.offset.return_value = offset
            result.headers.return_value = [(FENCE_HEADER_NAME, FENCE_HEADER_VALUE.encode())]
            result.value.return_value = json.dumps(value).encode()
            return result

        consumer = Mock()
        consumer.poll.side_effect = [message(markers[0], 11), message(markers[1], 17)]
        control = KafkaControl.__new__(KafkaControl)
        control._bootstrap = "kafka:19092"
        control._consumer = Mock(return_value=consumer)

        observed = control.wait_leaf_fence_markers(
            markers,
            {partitions[0]: 10, partitions[1]: 15},
            timeout_seconds=1,
        )

        self.assertEqual(observed, {partitions[0]: 12, partitions[1]: 18})
        consumer.assign.assert_called_once()
        consumer.close.assert_called_once()

    def test_marker_observer_ignores_business_and_stale_attempts(self) -> None:
        from flipbench.kafka_io import KafkaControl

        attempt_id = uuid.uuid4()
        partition = TopicPartition("topic-a", 0)
        marker = LeafFenceMarker(
            partition=partition,
            parent="parent_a",
            leaf="leaf_a",
            cell="cell01",
            timeslot="retiring",
            marker_id=uuid.uuid5(attempt_id, "leaf-a"),
            attempt_id=attempt_id,
            attempt_epoch=7,
        )
        business = Mock()
        business.error.return_value = None
        business.topic.return_value = partition.topic
        business.partition.return_value = 0
        business.offset.return_value = 4
        business.headers.return_value = []
        business.value.return_value = b"{}"
        consumer = Mock()
        consumer.poll.side_effect = [business, None]
        control = KafkaControl.__new__(KafkaControl)
        control._bootstrap = "kafka:19092"
        control._consumer = Mock(return_value=consumer)

        with patch("flipbench.kafka_io.time.monotonic", side_effect=[0.0, 0.0, 2.0]), self.assertRaises(
            TimeoutError
        ):
            control.wait_leaf_fence_markers((marker,), {partition: 4}, timeout_seconds=1)
        consumer.close.assert_called_once()

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
