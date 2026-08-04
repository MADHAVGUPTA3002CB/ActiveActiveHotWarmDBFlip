from __future__ import annotations

import json
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from typing import Mapping, Sequence

from confluent_kafka import Consumer, TopicPartition as KafkaTopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from .connector_configs import FENCE_HEADER_NAME, FENCE_HEADER_VALUE, FENCE_SCHEMA
from .core import LeafFenceMarker, OffsetError, TopicPartition


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int = 1
    replication_factor: int = 1
    min_insync_replicas: int = 1


class KafkaControl:
    def __init__(self, bootstrap: str) -> None:
        self._bootstrap = bootstrap
        self._admin = AdminClient({"bootstrap.servers": bootstrap, "client.id": "flipbench-admin"})
        self._group_consumers: dict[str, Consumer] = {}

    def ensure_topics(self, specs: Sequence[TopicSpec]) -> None:
        futures = self._admin.create_topics(
            [
                NewTopic(
                    spec.name,
                    num_partitions=spec.partitions,
                    replication_factor=spec.replication_factor,
                    config={
                        "min.insync.replicas": str(spec.min_insync_replicas),
                        "cleanup.policy": "delete",
                    },
                )
                for spec in specs
            ],
            operation_timeout=20,
        )
        for name, future in futures.items():
            try:
                future.result()
            except Exception as error:
                if "TOPIC_ALREADY_EXISTS" not in str(error):
                    raise OffsetError(f"failed to create topic {name}: {error}") from error
        deadline = time.monotonic() + 20.0
        last_error: OffsetError | None = None
        while time.monotonic() < deadline:
            try:
                self.validate_topics(specs)
                return
            except OffsetError as error:
                # The create-topic future may complete before every broker
                # metadata view used by list_topics exposes the new topic.
                last_error = error
                time.sleep(0.25)
        raise OffsetError(f"topic metadata did not converge: {last_error}") from last_error

    def validate_topics(self, specs: Sequence[TopicSpec]) -> None:
        metadata = self._admin.list_topics(timeout=10)
        for spec in specs:
            topic = metadata.topics.get(spec.name)
            if topic is None or topic.error is not None:
                raise OffsetError(f"topic missing or unhealthy: {spec.name}")
            if len(topic.partitions) != spec.partitions:
                raise OffsetError(
                    f"topic {spec.name} has {len(topic.partitions)} partitions; expected {spec.partitions}"
                )
            observed_replication = {
                len(partition.replicas) for partition in topic.partitions.values()
            }
            if observed_replication != {spec.replication_factor}:
                raise OffsetError(
                    f"topic {spec.name} has replication factors {sorted(observed_replication)}; "
                    f"expected {spec.replication_factor}"
                )

        resources = {
            spec.name: ConfigResource(ConfigResource.Type.TOPIC, spec.name) for spec in specs
        }
        futures = self._admin.describe_configs(list(resources.values()))
        for spec in specs:
            config = futures[resources[spec.name]].result()
            observed_min_isr = int(config["min.insync.replicas"].value)
            if observed_min_isr != spec.min_insync_replicas:
                raise OffsetError(
                    f"topic {spec.name} has min.insync.replicas={observed_min_isr}; "
                    f"expected {spec.min_insync_replicas}"
                )

    def end_offsets(self, partitions: Sequence[TopicPartition]) -> Mapping[TopicPartition, int]:
        consumer = self._consumer("flipbench-end-offset-observer")
        try:
            return {
                partition: consumer.get_watermark_offsets(
                    KafkaTopicPartition(partition.topic, partition.partition),
                    timeout=10,
                    cached=False,
                )[1]
                for partition in partitions
            }
        finally:
            consumer.close()

    def committed_offsets(
        self,
        group_by_partition: Mapping[TopicPartition, str],
        timeout_seconds: float = 10.0,
    ) -> Mapping[TopicPartition, int]:
        grouped: dict[str, list[TopicPartition]] = {}
        for partition, group in group_by_partition.items():
            grouped.setdefault(group, []).append(partition)
        if not grouped:
            return {}
        if timeout_seconds <= 0:
            raise TimeoutError("committed-offset query timeout must be positive")
        result: dict[TopicPartition, int] = {}
        consumers: dict[str, Consumer] = {}
        for group in grouped:
            consumer = self._group_consumers.get(group)
            if consumer is None:
                consumer = self._consumer(group)
                self._group_consumers[group] = consumer
            consumers[group] = consumer

        worker_count = min(8, len(grouped))
        wave_count = (len(grouped) + worker_count - 1) // worker_count
        per_query_timeout = timeout_seconds / wave_count

        def query_group(group: str, partitions: list[TopicPartition]) -> tuple[tuple[TopicPartition, int], ...]:
            consumer = consumers[group]
            query = [KafkaTopicPartition(item.topic, item.partition) for item in partitions]
            committed = consumer.committed(query, timeout=per_query_timeout)
            return tuple(
                (expected, observed.offset)
                for expected, observed in zip(partitions, committed)
                if observed.offset >= 0
            )

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = tuple(executor.submit(query_group, group, partitions) for group, partitions in grouped.items())
            for future in futures:
                result.update(future.result())
        return result

    def wait_leaf_fence_markers(
        self,
        markers: Sequence[LeafFenceMarker],
        baselines: Mapping[TopicPartition, int],
        timeout_seconds: float,
    ) -> Mapping[TopicPartition, int]:
        expected = {marker.partition: marker for marker in markers}
        if not expected or len(expected) != len(markers) or set(baselines) != set(expected):
            raise OffsetError("leaf fence marker plan and baseline vector must be complete and unique")
        if timeout_seconds <= 0:
            raise TimeoutError("leaf fence marker timeout must be positive")
        if any(
            not isinstance(offset, int) or isinstance(offset, bool) or offset < 0
            for offset in baselines.values()
        ):
            raise OffsetError("leaf fence baselines must be non-negative offsets")
        consumer = self._consumer("flipbench-leaf-fence-observer")
        consumer.assign(
            [
                KafkaTopicPartition(partition.topic, partition.partition, baselines[partition])
                for partition in sorted(expected)
            ]
        )
        observed: dict[TopicPartition, int] = {}
        deadline = time.monotonic() + timeout_seconds
        try:
            while len(observed) < len(expected):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    missing = sorted(partition.key for partition in expected if partition not in observed)
                    raise TimeoutError(f"leaf fence markers not observed before deadline: {missing}")
                message = consumer.poll(min(0.25, remaining))
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    raise OffsetError(f"leaf fence observer failed: {error}")
                partition = TopicPartition(message.topic(), message.partition())
                marker = expected.get(partition)
                if marker is None or message.offset() < baselines[partition]:
                    continue
                headers = dict(message.headers() or ())
                raw_header = headers.get(FENCE_HEADER_NAME)
                header_value = (
                    raw_header.decode("utf-8", errors="strict")
                    if isinstance(raw_header, bytes)
                    else raw_header
                )
                if header_value != FENCE_HEADER_VALUE:
                    continue
                try:
                    envelope = json.loads(message.value())
                    payload = envelope["payload"]
                    after = payload["after"]
                    source = payload["source"]
                except (TypeError, ValueError, KeyError, UnicodeDecodeError) as error:
                    raise OffsetError("malformed leaf fence control record") from error
                candidate = (
                    after.get("marker_schema_version"),
                    after.get("marker_id"),
                    after.get("attempt_id"),
                    after.get("attempt_epoch"),
                    after.get("cell"),
                    after.get("timeslot"),
                    after.get("parent_name"),
                    after.get("leaf_name"),
                    source.get("schema"),
                    source.get("table"),
                )
                wanted = (
                    1,
                    str(marker.marker_id),
                    str(marker.attempt_id),
                    marker.attempt_epoch,
                    marker.cell,
                    marker.timeslot,
                    marker.parent,
                    marker.leaf,
                    FENCE_SCHEMA,
                    marker.leaf,
                )
                if candidate != wanted:
                    continue
                next_offset = message.offset() + 1
                previous = observed.get(partition)
                if previous is not None and previous != next_offset:
                    raise OffsetError(f"conflicting duplicate leaf fence marker on {partition.key}")
                observed[partition] = next_offset
            return observed
        finally:
            consumer.close()

    def close(self) -> None:
        for consumer in self._group_consumers.values():
            consumer.close()
        self._group_consumers = {}

    def _consumer(self, group: str) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self._bootstrap,
                "group.id": group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
                "socket.timeout.ms": 10_000,
            }
        )
