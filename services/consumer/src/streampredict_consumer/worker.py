"""Kafka poll/process/publish/commit loop for one consumer-group member.

Delivery is at-least-once: offsets are committed only after every result and dead-letter message
of the batch is acknowledged. If publishing or committing fails, the batch's partitions are rewound
and the batch is polled again; the idempotency store turns already-finished events into no-ops.
"""

import asyncio
import contextlib
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import KafkaError

from .config import ConsumerSettings
from .idempotency import IdempotencyStore
from .metrics import ConsumerMetrics
from .processor import EventProcessor, InboundRecord

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 10.0


class KafkaWorker:
    def __init__(
        self,
        settings: ConsumerSettings,
        processor: EventProcessor,
        idempotency: IdempotencyStore,
        metrics: ConsumerMetrics,
    ) -> None:
        self._settings = settings
        self._processor = processor
        self._idempotency = idempotency
        self._metrics = metrics
        self._stopping = asyncio.Event()
        self._lag_partitions: set[str] = set()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        backoff = 0.5
        while not self._stopping.is_set():
            consumer, producer = self._build_clients()
            try:
                await consumer.start()
                await producer.start()
                logger.info(
                    "Consumer joined group",
                    extra={
                        "group": self._settings.kafka_consumer_group,
                        "topic": self._settings.kafka_prediction_topic,
                    },
                )
                backoff = 0.5
                await self._consume(consumer, producer)
            except (KafkaError, OSError) as exc:
                logger.warning("Kafka unavailable; retrying", extra={"error": repr(exc)})
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            finally:
                # Stopping the consumer leaves the group so partitions rebalance immediately.
                await consumer.stop()
                await producer.stop()
        logger.info("Consumer stopped")

    def _build_clients(self) -> tuple[Any, Any]:
        settings = self._settings
        consumer = AIOKafkaConsumer(
            settings.kafka_prediction_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            client_id="streampredict-consumer",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=settings.consumer_batch_size,
            # Notice new or recreated topics/partitions quickly; a member that joined while the
            # topic was missing gets no partitions until metadata refreshes (default 5 minutes).
            metadata_max_age_ms=15_000,
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="streampredict-consumer",
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
        )
        return consumer, producer

    async def _consume(self, consumer: Any, producer: Any) -> None:
        while not self._stopping.is_set():
            batch: dict[Any, list[Any]] = await consumer.getmany(
                timeout_ms=self._settings.consumer_poll_timeout_ms,
                max_records=self._settings.consumer_batch_size,
            )
            if batch:
                await self._handle_batch(consumer, producer, batch)
            await self._update_lag(consumer)

    async def _handle_batch(
        self, consumer: Any, producer: Any, batch: dict[Any, list[Any]]
    ) -> None:
        records = [
            InboundRecord(r.topic, r.partition, r.offset, r.key, r.value)
            for messages in batch.values()
            for r in messages
        ]
        self._metrics.batch_size.observe(len(records))
        outcome = await self._processor.process_batch(records)
        try:
            pending = [
                await producer.send(m.topic, value=m.value, key=m.key, headers=m.headers)
                for m in outcome.messages
            ]
            await asyncio.gather(*pending)
            await self._idempotency.mark(outcome.completed_event_ids)
            await consumer.commit({tp: messages[-1].offset + 1 for tp, messages in batch.items()})
        except KafkaError as exc:
            self._metrics.batch_failures.inc()
            logger.warning("Batch not committed; re-polling", extra={"error": repr(exc)})
            for tp, messages in batch.items():
                # The partition may have been revoked by a rebalance; its new owner re-reads it.
                with contextlib.suppress(Exception):
                    consumer.seek(tp, messages[0].offset)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), 1.0)

    async def _update_lag(self, consumer: Any) -> None:
        assigned: set[TopicPartition] = consumer.assignment()
        self._metrics.assigned_partitions.set(len(assigned))
        current: set[str] = set()
        for tp in assigned:
            highwater = consumer.highwater(tp)
            if highwater is None:
                continue
            position = await consumer.position(tp)
            label = str(tp.partition)
            current.add(label)
            self._metrics.lag.labels(label).set(max(0, highwater - position))
        # Drop series for partitions that moved to another worker after a rebalance.
        for label in self._lag_partitions - current:
            with contextlib.suppress(KeyError):
                self._metrics.lag.remove(label)
        self._lag_partitions = current
