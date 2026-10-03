"""Unit tests for the event contract, consumer batch processing, and lag/throughput math."""

import asyncio
import json
from typing import cast

import fakeredis
import pytest
from redis.asyncio import Redis

from streampredict_api.cache import PredictionCache
from streampredict_api.errors import AppError
from streampredict_api.events import InvalidEvent, new_event, parse_event
from streampredict_api.inference import InferenceError, InferenceResult, MockInferenceClient
from streampredict_api.kafka_monitor import KafkaMonitor, OffsetSnapshot
from streampredict_api.metrics import ApiMetrics
from streampredict_api.prediction import PredictionService
from streampredict_api.schemas import PredictionFeatures
from streampredict_consumer.idempotency import RedisIdempotencyStore
from streampredict_consumer.metrics import ConsumerMetrics
from streampredict_consumer.processor import EventProcessor, InboundRecord
from tests.conftest import FakeOffsetSource

FEATURES = PredictionFeatures(amount=860, events_per_hour=4, distance_km=120)


def encoded_event(**overrides: object) -> bytes:
    event = new_event(FEATURES, request_id="req-1", model_name="demo", model_version="v1")
    payload = json.loads(event.model_dump_json())
    payload.update(overrides)
    return json.dumps(payload).encode()


def record(value: bytes | None, offset: int = 0) -> InboundRecord:
    return InboundRecord("prediction-events", 0, offset, b"key", value)


# --- event contract ---------------------------------------------------------------------------


def test_parse_event_round_trips_and_ignores_unknown_fields() -> None:
    event = parse_event(encoded_event(schema_version="1.3", added_in_1_3="ignored"))
    assert event.schema_version == "1.3"
    assert event.features == FEATURES
    assert event.metadata.source == "api"


@pytest.mark.parametrize(
    ("raw", "fragment"),
    [
        (None, "empty"),
        (b"not json", "payload"),
        (encoded_event(schema_version="2.0"), "schema_version"),
        (encoded_event(features={"amount": -1, "events_per_hour": 0, "distance_km": 0}), "amount"),
    ],
)
def test_parse_event_rejects_invalid_records(raw: bytes | None, fragment: str) -> None:
    with pytest.raises(InvalidEvent, match=fragment):
        parse_event(raw)


def test_invalid_event_errors_do_not_echo_values() -> None:
    raw = encoded_event(features={"amount": -123456, "events_per_hour": 0, "distance_km": 0})
    with pytest.raises(InvalidEvent) as info:
        parse_event(raw)
    assert "123456" not in str(info.value)


# --- consumer processing ----------------------------------------------------------------------


class FlakyInference(MockInferenceClient):
    """Fails the first `failures` calls like an unavailable model backend."""

    def __init__(self, failures: int) -> None:
        super().__init__("demo", "v1")
        self.failures = failures
        self.calls = 0

    async def predict(self, features: PredictionFeatures) -> InferenceResult:
        self.calls += 1
        if self.calls <= self.failures:
            raise InferenceError("backend down")
        return await super().predict(features)


class Harness:
    def __init__(self, inference: MockInferenceClient, max_attempts: int = 3) -> None:
        self.metrics = ConsumerMetrics()
        self.redis = cast(Redis, fakeredis.FakeAsyncRedis())
        self.sleeps: list[float] = []
        cache = PredictionCache(
            self.redis,
            ttl_seconds=60,
            timeout_seconds=1,
            circuit_open_seconds=0,
            metrics=self.metrics,
        )
        predictions = PredictionService(
            cache, inference, self.metrics, None, inference_timeout_seconds=1, max_in_flight=100
        )
        self.idempotency = RedisIdempotencyStore(
            self.redis, ttl_seconds=60, timeout_seconds=1, metrics=self.metrics
        )

        async def fake_sleep(seconds: float) -> None:
            self.sleeps.append(seconds)

        self.processor = EventProcessor(
            predictions,
            self.idempotency,
            self.metrics,
            result_topic="prediction-results",
            dead_letter_topic="prediction-events-dlq",
            max_attempts=max_attempts,
            retry_backoff_seconds=0.1,
            concurrency=4,
            sleep=fake_sleep,
        )

    def counter(self, name: str, **labels: str) -> float:
        value = self.metrics.registry.get_sample_value(name, labels)
        return value or 0.0


def test_processor_publishes_results_for_valid_events() -> None:
    harness = Harness(MockInferenceClient("demo", "v7"))
    raw = encoded_event()

    outcome = asyncio.run(harness.processor.process_batch([record(raw)]))

    assert outcome.succeeded == 1
    assert len(outcome.messages) == 1
    message = outcome.messages[0]
    result = json.loads(message.value)
    source = json.loads(raw)
    assert message.topic == "prediction-results"
    assert message.key == source["event_id"].encode()
    assert result["event_id"] == source["event_id"]
    assert result["request_id"] == "req-1"
    assert result["model_version"] == "v7"
    assert result["label"] == "low_risk"
    assert outcome.completed_event_ids == [source["event_id"]]


def test_processor_skips_redelivered_and_repeated_events() -> None:
    harness = Harness(MockInferenceClient("demo", "v1"))
    done, fresh = encoded_event(), encoded_event()
    asyncio.run(harness.idempotency.mark([json.loads(done)["event_id"]]))

    outcome = asyncio.run(
        harness.processor.process_batch([record(done, 0), record(fresh, 1), record(fresh, 2)])
    )

    assert outcome.succeeded == 1
    assert outcome.duplicates == 2
    assert outcome.completed_event_ids == [json.loads(fresh)["event_id"]]
    assert harness.counter("streampredict_consumer_events_total", outcome="duplicate") == 2


def test_processor_dead_letters_invalid_records_with_context() -> None:
    harness = Harness(MockInferenceClient("demo", "v1"))

    outcome = asyncio.run(harness.processor.process_batch([record(b"{broken", offset=42)]))

    assert outcome.dead_lettered == 1
    message = outcome.messages[0]
    headers = dict(message.headers)
    assert message.topic == "prediction-events-dlq"
    assert message.value == b"{broken"
    assert message.key == b"key"
    assert headers["dlq.error_code"] == b"invalid_event"
    assert headers["dlq.source_offset"] == b"42"
    assert headers["dlq.attempts"] == b"0"
    assert outcome.completed_event_ids == []


def test_processor_retries_transient_failures() -> None:
    inference = FlakyInference(failures=2)
    harness = Harness(inference)

    outcome = asyncio.run(harness.processor.process_batch([record(encoded_event())]))

    assert outcome.succeeded == 1
    assert inference.calls == 3
    assert harness.sleeps == [0.1, 0.2]
    assert harness.counter("streampredict_consumer_retries_total") == 2


def test_processor_dead_letters_after_max_attempts() -> None:
    harness = Harness(FlakyInference(failures=10), max_attempts=3)

    outcome = asyncio.run(harness.processor.process_batch([record(encoded_event())]))

    assert outcome.succeeded == 0
    assert outcome.dead_lettered == 1
    headers = dict(outcome.messages[0].headers)
    assert headers["dlq.error_code"] == b"inference_unavailable"
    assert headers["dlq.attempts"] == b"3"
    assert (
        harness.counter("streampredict_consumer_dead_letters_total", reason="inference_unavailable")
        == 1
    )


def test_non_retryable_errors_are_not_retried() -> None:
    class Rejecting(MockInferenceClient):
        async def predict(self, features: PredictionFeatures) -> InferenceResult:
            raise AppError(422, "unsupported_features", "Features not supported.")

    harness = Harness(Rejecting("demo", "v1"))
    outcome = asyncio.run(harness.processor.process_batch([record(encoded_event())]))

    assert dict(outcome.messages[0].headers)["dlq.attempts"] == b"1"
    assert harness.sleeps == []


# --- lag and throughput -----------------------------------------------------------------------


def test_offset_snapshot_lag_treats_uncommitted_partitions_as_unconsumed() -> None:
    snapshot = OffsetSnapshot(end={0: 10, 1: 5, 2: 3}, committed={0: 4, 2: 9})
    assert snapshot.lag_by_partition() == {0: 6, 1: 5, 2: 0}


def test_kafka_monitor_derives_rates_from_offsets() -> None:
    now = [0.0]
    source = FakeOffsetSource(
        [
            OffsetSnapshot(end={0: 100}, committed={0: 100}),
            OffsetSnapshot(end={0: 400}, committed={0: 200}),
        ]
    )
    monitor = KafkaMonitor(
        source, ApiMetrics(), topic="t", consumer_group="g", clock=lambda: now[0]
    )

    asyncio.run(monitor.sample())
    now[0] = 2.0
    asyncio.run(monitor.sample())
    overview = monitor.overview()

    assert overview.status == "ok"
    assert overview.incoming_rate == 150.0
    assert overview.consumer_rate == 50.0
    assert overview.lag == 200
    assert overview.lag_history == [0, 200]

    asyncio.run(monitor.sample())  # the source is exhausted and times out
    assert monitor.overview().status == "unavailable"
    assert monitor.overview().lag is None
