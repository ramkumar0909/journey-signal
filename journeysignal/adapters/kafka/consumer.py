"""Synchronous Kafka adapter with explicit acknowledgement of completed records."""

from confluent_kafka import Consumer, KafkaError, TopicPartition

from journeysignal.domain.events import SourceRef, TransportRecord


class BrokerFailure(RuntimeError):
    pass


class KafkaSource:
    def __init__(self, servers: str, topic: str, group_id: str) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "max.poll.interval.ms": 120000,
                "session.timeout.ms": 10000,
                "socket.timeout.ms": 10000,
                "fetch.message.max.bytes": 1048576,
                "queued.max.messages.kbytes": 4096,
                "log_level": 0,
            }
        )
        self.rebalances = 0
        self.consumer.subscribe([topic], on_revoke=self._revoked)

    def _revoked(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Poll invokes callbacks between records. No speculative offset is committed.
        self.rebalances += 1

    def poll(self, timeout: float) -> TransportRecord | None:
        message = self.consumer.poll(timeout)
        if message is None:
            return None
        error = message.error()
        if error:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            raise BrokerFailure("Kafka poll failed")
        value = message.value()
        if value is not None and not isinstance(value, bytes):
            raise BrokerFailure("Unexpected Kafka value representation")
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        if topic is None or partition is None or offset is None:
            raise BrokerFailure("Kafka delivery lacks transport identity")
        return TransportRecord(SourceRef(topic, partition, offset), value)

    def acknowledge(self, record: TransportRecord) -> None:
        ref = record.source_ref
        self.consumer.commit(
            offsets=[TopicPartition(ref.topic, ref.partition, ref.offset + 1)], asynchronous=False
        )

    def lag(self, record: TransportRecord) -> int:
        ref = record.source_ref
        _, high = self.consumer.get_watermark_offsets(
            TopicPartition(ref.topic, ref.partition), timeout=2, cached=True
        )
        return max(0, int(high) - ref.offset - 1)

    def close(self) -> None:
        self.consumer.close()

    def ready(self) -> bool:
        metadata = self.consumer.list_topics(timeout=3)
        return bool(metadata.brokers)
