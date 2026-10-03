"""Processed-event markers that make redelivered events no-ops.

Kafka delivery is at-least-once: a crash between publishing results and committing offsets
redelivers the batch. Event IDs are marked done only after their results are acknowledged, and
marked events are skipped on redelivery. Redis failures fail open (the event is processed again),
which can duplicate a result message but never loses one.
"""

import asyncio
import logging
from typing import Protocol

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .metrics import ConsumerMetrics

logger = logging.getLogger(__name__)

KEY_PREFIX = "sp:v1:event-done"


class IdempotencyStore(Protocol):
    async def seen(self, event_ids: list[str]) -> set[str]: ...

    async def mark(self, event_ids: list[str]) -> None: ...


class RedisIdempotencyStore:
    def __init__(
        self, client: Redis, ttl_seconds: int, timeout_seconds: float, metrics: ConsumerMetrics
    ) -> None:
        self._client = client
        self._ttl = ttl_seconds
        self._timeout = timeout_seconds
        self._metrics = metrics

    @staticmethod
    def key(event_id: str) -> str:
        return f"{KEY_PREFIX}:{event_id}"

    async def seen(self, event_ids: list[str]) -> set[str]:
        if not event_ids:
            return set()
        try:
            values = await asyncio.wait_for(
                self._client.mget([self.key(event_id) for event_id in event_ids]),
                self._timeout,
            )
        except (RedisError, OSError, TimeoutError) as exc:
            self._metrics.idempotency_errors.labels("seen").inc()
            logger.warning(
                "Idempotency lookup failed; processing anyway", extra={"error": repr(exc)}
            )
            return set()
        return {event_id for event_id, value in zip(event_ids, values, strict=True) if value}

    async def mark(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        pipeline = self._client.pipeline(transaction=False)
        for event_id in event_ids:
            pipeline.set(self.key(event_id), b"1", ex=self._ttl)
        try:
            await asyncio.wait_for(pipeline.execute(), self._timeout)
        except (RedisError, OSError, TimeoutError) as exc:
            self._metrics.idempotency_errors.labels("mark").inc()
            logger.warning("Idempotency mark failed", extra={"error": repr(exc)})
