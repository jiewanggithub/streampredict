"""Redis-backed prediction cache with fail-fast timeouts and a circuit breaker.

Redis is an optimization, never a dependency for correctness: every failure degrades to a cache
bypass so the gateway keeps serving predictions from the inference backend.
"""

import asyncio
import hashlib
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from .inference import InferenceResult, label_for
from .metrics import ApiMetrics, RollingWindow
from .schemas import CacheStatus, PredictionFeatures

logger = logging.getLogger(__name__)

KEY_PREFIX = "sp:v1:prediction"
TTL_JITTER_FRACTION = 0.1

T = TypeVar("T")


def build_redis(url: str, timeout_seconds: float) -> Redis:
    """Create a Redis client that fails fast instead of retrying on the request path."""
    client: Redis = Redis.from_url(
        url,
        socket_timeout=timeout_seconds,
        socket_connect_timeout=timeout_seconds,
        retry=Retry(NoBackoff(), retries=0),
        health_check_interval=30,
    )
    return client


def prediction_key(model_name: str, model_version: str, features: PredictionFeatures) -> str:
    """Return the cache key for a prediction.

    The model version is part of the key, so publishing a new model never serves stale scores.
    Features are hashed so raw inputs are not stored in key names.
    """
    canonical = json.dumps(features.model_dump(), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()[:32]
    return f"{KEY_PREFIX}:{model_name}:{model_version}:{digest}"


class CacheUnavailable(Exception):
    """Raised internally when Redis cannot be used for this operation."""


class PredictionCache:
    def __init__(
        self,
        client: Redis,
        ttl_seconds: int,
        timeout_seconds: float,
        circuit_open_seconds: float,
        metrics: ApiMetrics,
        window: RollingWindow,
    ) -> None:
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._timeout_seconds = timeout_seconds
        self._circuit_open_seconds = circuit_open_seconds
        self._metrics = metrics
        self._window = window
        self._open_until = 0.0

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds

    @property
    def circuit_open(self) -> bool:
        return time.monotonic() < self._open_until

    async def _run(self, operation: str, call: Callable[[], Awaitable[T]]) -> T:
        if self.circuit_open:
            self._metrics.cache_errors.labels(operation, "circuit_open").inc()
            raise CacheUnavailable
        started = time.perf_counter()
        try:
            result = await asyncio.wait_for(call(), timeout=self._timeout_seconds)
        except (RedisError, OSError, TimeoutError) as exc:
            reason = "timeout" if isinstance(exc, TimeoutError) else "error"
            self._metrics.cache_errors.labels(operation, reason).inc()
            if self._circuit_open_seconds and not self.circuit_open:
                logger.warning(
                    "Redis unavailable; bypassing cache",
                    extra={"operation": operation, "error": type(exc).__name__},
                )
            self._open_until = time.monotonic() + self._circuit_open_seconds
            raise CacheUnavailable from exc
        elapsed = time.perf_counter() - started
        self._metrics.cache_duration.labels(operation).observe(elapsed)
        if operation == "get":
            self._window.record_cache_lookup(elapsed * 1000)
        return result

    async def get(self, key: str) -> tuple[CacheStatus, InferenceResult | None]:
        try:
            raw = await self._run("get", lambda: self._client.get(key))
        except CacheUnavailable:
            return "bypass", None
        if raw is None:
            return "miss", None
        try:
            score = float(json.loads(raw)["score"])
        except (ValueError, KeyError, TypeError):
            logger.warning("Discarding malformed cache entry")
            return "miss", None
        return "hit", InferenceResult(score=score, label=label_for(score))

    async def set(self, key: str, result: InferenceResult) -> None:
        # Jittered TTLs keep entries written during a burst from expiring at the same instant.
        ttl = max(1, round(self._ttl_seconds * random.uniform(1 - TTL_JITTER_FRACTION, 1)))
        payload = json.dumps({"score": result.score})
        try:
            await self._run("set", lambda: self._client.set(key, payload, ex=ttl))
        except CacheUnavailable:
            return

    async def ping(self) -> bool:
        try:
            return bool(await self._run("ping", lambda: self._client.ping()))
        except CacheUnavailable:
            return False
