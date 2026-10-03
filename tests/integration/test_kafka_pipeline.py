"""End-to-end event pipeline against a real broker: publish -> consume -> results / dead letters.

Skipped unless STREAMPREDICT_TEST_KAFKA points at a broker (`make up` then `make test-kafka`).
Each test run uses its own topics and consumer group, so a running Compose stack does not interfere.
"""

import asyncio
import contextlib
import json
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import fakeredis
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from redis.asyncio import Redis

from streampredict_api.events import KafkaEventPublisher, new_event
from streampredict_api.kafka_monitor import KafkaOffsetSource
from streampredict_api.metrics import ApiMetrics
from streampredict_api.schemas import PredictionFeatures
from streampredict_consumer.app import build_worker
from streampredict_consumer.config import ConsumerSettings
from streampredict_consumer.metrics import ConsumerMetrics
from streampredict_consumer.replay import replay

BOOTSTRAP = os.environ.get("STREAMPREDICT_TEST_KAFKA", "")

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(not BOOTSTRAP, reason="STREAMPREDICT_TEST_KAFKA is not set"),
]


async def collect(topic: str, expected: int, timeout: float = 30.0) -> list[Any]:
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest", enable_auto_commit=False
    )
    await consumer.start()
    records: list[Any] = []
    try:
        async with asyncio.timeout(timeout):
            while len(records) < expected:
                batch = await consumer.getmany(timeout_ms=500)
                records.extend(r for messages in batch.values() for r in messages)
        # Give late duplicates a moment to show up before asserting exactly-once results.
        batch = await consumer.getmany(timeout_ms=1500)
        records.extend(r for messages in batch.values() for r in messages)
    finally:
        await consumer.stop()
    return records


async def wait_for_zero_lag(source: KafkaOffsetSource, timeout: float = 15.0) -> None:
    async with asyncio.timeout(timeout):
        while True:
            snapshot = await source.read()
            if sum(snapshot.lag_by_partition().values()) == 0:
                return
            await asyncio.sleep(0.5)


@contextlib.asynccontextmanager
async def temporary_topics() -> AsyncIterator[dict[str, str]]:
    suffix = uuid.uuid4().hex[:8]
    names = {
        "events": f"test-events-{suffix}",
        "results": f"test-results-{suffix}",
        "dlq": f"test-dlq-{suffix}",
    }
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    await admin.create_topics([NewTopic(name, 3, 1) for name in names.values()])
    try:
        yield names
    finally:
        await admin.delete_topics(list(names.values()))
        await admin.close()


def test_events_flow_through_the_pipeline_exactly_once() -> None:
    asyncio.run(_run_pipeline())


async def _run_pipeline() -> None:
    async with temporary_topics() as topics:
        await _exercise(topics)


async def _exercise(topics: dict[str, str]) -> None:
    group = f"test-group-{uuid.uuid4().hex[:8]}"
    settings = ConsumerSettings(
        _env_file=None,
        kafka_bootstrap_servers=BOOTSTRAP,
        kafka_prediction_topic=topics["events"],
        kafka_result_topic=topics["results"],
        kafka_dead_letter_topic=topics["dlq"],
        kafka_consumer_group=group,
        mock_inference_latency_ms=0,
        model_version="v-kafka-test",
        consumer_poll_timeout_ms=200,
    )
    redis = cast(Redis, fakeredis.FakeAsyncRedis())
    worker = build_worker(settings, redis, ConsumerMetrics())
    worker_task = asyncio.create_task(worker.run())

    publisher = KafkaEventPublisher(
        BOOTSTRAP,
        topics["events"],
        ApiMetrics(),
        timeout_seconds=10,
        reconnect_interval_seconds=0,
    )
    raw_producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await raw_producer.start()
    source = KafkaOffsetSource(BOOTSTRAP, topics["events"], group, timeout_seconds=5)
    try:
        events = [
            new_event(
                PredictionFeatures(amount=100 + i, events_per_hour=i % 7, distance_km=10 * i),
                request_id=f"req-{i}",
                model_name="streampredict-demo",
                model_version="v-kafka-test",
            )
            for i in range(25)
        ]
        for event in events:
            await publisher.publish(event)
        await raw_producer.send_and_wait(topics["events"], b"{not json", key=b"bad")
        # A redelivered copy of a processed event must not produce a second result.
        await wait_for_zero_lag(source)
        await raw_producer.send_and_wait(
            topics["events"], events[0].model_dump_json().encode(), key=b"dup"
        )
        await wait_for_zero_lag(source)

        results = await collect(topics["results"], expected=len(events))
        dead_letters = await collect(topics["dlq"], expected=1)

        # Replaying republishes the original bytes; still invalid, so it dead-letters again.
        replay_group = f"{group}-replay"
        stats = await replay(
            settings, group=replay_group, error_code="invalid_event", max_records=10, dry_run=False
        )
        await collect(topics["dlq"], expected=2)
        # A second run only picks up the new dead letter (progress is committed) and stops at the
        # offsets it saw on start, so the poison record cannot cycle within a run.
        again = await replay(
            settings, group=replay_group, error_code=None, max_records=10, dry_run=False
        )
        redelivered = await collect(topics["dlq"], expected=3)
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, 15)
        await raw_producer.stop()
        await publisher.close()
        await source.close()

    payloads = [json.loads(record.value) for record in results]
    assert sorted(p["event_id"] for p in payloads) == sorted(e.event_id for e in events)
    assert {p["model_version"] for p in payloads} == {"v-kafka-test"}
    assert {p["request_id"] for p in payloads} == {e.request_id for e in events}

    assert len(dead_letters) == 1
    headers = {key: value for key, value in dead_letters[0].headers}
    assert dead_letters[0].value == b"{not json"
    assert headers["dlq.error_code"] == b"invalid_event"
    assert headers["dlq.source_topic"] == topics["events"].encode()

    assert stats["replayed"] == 1
    assert again["replayed"] == 1
    assert [record.value for record in redelivered] == [b"{not json"] * 3
