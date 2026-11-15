import time
from collections.abc import Callable
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from journeysignal.application.simulator import Attempt
from journeysignal.domain.events import SourceRef


def ensure_topic(servers: str, topic: str, partitions: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": servers, "log_level": 0})
    futures = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=partitions,
                replication_factor=1,
                config={"retention.ms": "86400000", "max.message.bytes": "1048576"},
            )
        ],
        request_timeout=15,
    )
    try:
        futures[topic].result(timeout=20)
    except KafkaException as error:
        if error.args[0].name() != "TOPIC_ALREADY_EXISTS":
            raise RuntimeError("Kafka topic creation failed") from None


@dataclass(frozen=True)
class PublishedAttempt:
    attempt_index: int
    source_ref: SourceRef


def publish(
    servers: str,
    topic: str,
    attempts: tuple[Attempt, ...],
    rate_per_second: float = 0,
) -> tuple[PublishedAttempt, ...]:
    producer = Producer(
        {
            "bootstrap.servers": servers,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 30000,
            "queue.buffering.max.messages": 10000,
            "log_level": 0,
        }
    )
    delivered: list[PublishedAttempt] = []
    failed: list[bool] = []

    def callback(index: int, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            failed.append(True)
            return
        topic_value = message.topic()
        partition = message.partition()
        offset = message.offset()
        if topic_value is None or partition is None or offset is None:
            failed.append(True)
            return
        delivered.append(PublishedAttempt(index, SourceRef(topic_value, partition, offset)))

    def delivery_callback(index: int) -> Callable[[KafkaError | None, Message], None]:
        def confirmed(error: KafkaError | None, message: Message) -> None:
            callback(index, error, message)

        return confirmed

    started = time.monotonic()
    for index, attempt in enumerate(attempts):
        if rate_per_second:
            time.sleep(max(0, started + index / rate_per_second - time.monotonic()))
        # Bounded enqueue retry; delivery errors still fail the whole command.
        deadline = time.monotonic() + 30
        while True:
            try:
                producer.produce(
                    topic,
                    value=attempt.value,
                    on_delivery=delivery_callback(index),
                )
                break
            except BufferError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Kafka producer queue remained full") from None
                producer.poll(0.1)
        producer.poll(0)
    remaining = producer.flush(35)
    if remaining or failed or len(delivered) != len(attempts):
        raise RuntimeError("Not all Kafka deliveries were confirmed")
    return tuple(sorted(delivered, key=lambda item: item.attempt_index))
