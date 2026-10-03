"""Shared fixtures for API gateway tests."""

from collections.abc import Callable, Iterator
from typing import Any, cast

import fakeredis
import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from streampredict_api.config import Settings
from streampredict_api.main import create_app


class UnavailableRedis:
    """Redis double whose every command fails like an unreachable server."""

    async def get(self, *_: object, **__: object) -> None:
        raise RedisConnectionError("connection refused")

    async def set(self, *_: object, **__: object) -> None:
        raise RedisConnectionError("connection refused")

    async def ping(self) -> None:
        raise RedisConnectionError("connection refused")

    async def aclose(self) -> None:
        return None


def build_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "app_env": "test",
        "log_level": "WARNING",
        "model_version": "v-test",
        "mock_inference_latency_ms": 0,
        "demo_max_rps": 50,
        "demo_max_duration_seconds": 300,
        "demo_control_token": None,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


ClientFactory = Callable[..., TestClient]


@pytest.fixture
def make_client() -> Iterator[ClientFactory]:
    clients: list[TestClient] = []

    def factory(redis_available: bool = True, **overrides: Any) -> TestClient:
        def redis_factory(_: Settings) -> Redis:
            if redis_available:
                return cast(Redis, fakeredis.FakeAsyncRedis())
            return cast(Redis, UnavailableRedis())

        app = create_app(build_settings(**overrides), redis_factory=redis_factory)
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
