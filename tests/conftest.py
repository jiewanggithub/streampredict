"""Shared fixtures for API gateway tests."""

from collections.abc import Callable, Iterator
from typing import Any, cast

import fakeredis
import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from streampredict_api.config import Settings
from streampredict_api.errors import AppError
from streampredict_api.events import EventPublisher, PredictionEvent, PublishReceipt
from streampredict_api.kafka_monitor import KafkaMonitor, OffsetSnapshot
from streampredict_api.main import KafkaFactory, create_app
from streampredict_api.metrics import ApiMetrics


class UnavailableRedis:
    """Redis double whose every command fails like an unreachable server."""

    async def get(self, *_: object, **__: object) -> None:
        raise RedisConnectionError("connection refused")

    async def set(self, *_: object, **__: object) -> None:
        raise RedisConnectionError("connection refused")

    async def ping(self) -> None:
        raise RedisConnectionError("connection refused")

    async def mget(self, *_: object, **__: object) -> None:
        raise RedisConnectionError("connection refused")

    def pipeline(self, *_: object, **__: object) -> "UnavailableRedis":
        return self

    def incrby(self, *_: object, **__: object) -> None:
        return None

    def expire(self, *_: object, **__: object) -> None:
        return None

    async def execute(self) -> None:
        raise RedisConnectionError("connection refused")

    async def aclose(self) -> None:
        return None


class FakePublisher:
    """In-memory EventPublisher that records events or fails like an unreachable broker."""

    def __init__(self, available: bool = True) -> None:
        self.available = available
        self.events: list[PredictionEvent] = []

    async def publish(self, event: PredictionEvent) -> PublishReceipt:
        if not self.available:
            raise AppError(503, "kafka_unavailable", "The event pipeline is unavailable.")
        self.events.append(event)
        return PublishReceipt(topic="prediction-events", partition=0, offset=len(self.events) - 1)

    async def ready(self) -> bool:
        return self.available

    async def close(self) -> None:
        return None


class FakeOffsetSource:
    def __init__(self, snapshots: list[OffsetSnapshot]) -> None:
        self.snapshots = snapshots

    async def read(self) -> OffsetSnapshot:
        if not self.snapshots:
            raise TimeoutError
        return self.snapshots.pop(0)

    async def close(self) -> None:
        return None


def fake_kafka(publisher: FakePublisher, source: FakeOffsetSource | None = None) -> KafkaFactory:
    def factory(
        settings: Settings, metrics: ApiMetrics
    ) -> tuple[EventPublisher, KafkaMonitor | None]:
        monitor = KafkaMonitor(
            source or FakeOffsetSource([]),
            metrics,
            topic=settings.kafka_prediction_topic,
            consumer_group=settings.kafka_consumer_group,
            interval_seconds=3600,
        )
        return publisher, monitor

    return factory


def build_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "app_env": "test",
        "log_level": "WARNING",
        "model_version": "v-test",
        "mock_inference_latency_ms": 0,
        "demo_max_rps": 50,
        "demo_max_duration_seconds": 300,
        "demo_control_token": None,
        "kafka_bootstrap_servers": "",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


ClientFactory = Callable[..., TestClient]


@pytest.fixture
def make_client() -> Iterator[ClientFactory]:
    clients: list[TestClient] = []

    def factory(
        redis_available: bool = True, kafka: KafkaFactory | None = None, **overrides: Any
    ) -> TestClient:
        def redis_factory(_: Settings) -> Redis:
            if redis_available:
                return cast(Redis, fakeredis.FakeAsyncRedis())
            return cast(Redis, UnavailableRedis())

        factories: dict[str, Any] = {"redis_factory": redis_factory}
        if kafka is not None:
            overrides.setdefault("kafka_bootstrap_servers", "kafka:9092")
            factories["kafka_factory"] = kafka
        if "deployment_factory" in overrides:
            factories["deployment_factory"] = overrides.pop("deployment_factory")
        app = create_app(build_settings(**overrides), **factories)
        client = TestClient(app)
        client.__enter__()
        clients.append(client)
        return client

    yield factory
    for client in clients:
        client.__exit__(None, None, None)


@pytest.fixture
def client(make_client: ClientFactory) -> TestClient:
    return make_client()
