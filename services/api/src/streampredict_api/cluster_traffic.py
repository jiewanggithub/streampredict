"""Cluster-wide request counts shared through Redis.

Each gateway replica keeps its own rolling window, so with several replicas behind a Service a
single replica only sees its share of traffic. Replicas add their per-second request and error
counts to shared Redis counters once a second (one pipelined write, never on the request path);
the overview sums them for cluster-wide RPS and success rate. Latency percentiles and cache rates
stay per replica: they are distributions, and one replica's share is a fair sample of them.

Best effort by design: counts that fail to flush are dropped, and readers fall back to the local
window when Redis is unavailable.
"""

import asyncio
import contextlib
import logging
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

KEY_PREFIX = "sp:v1:traffic"
TTL_SECONDS = 180


@dataclass(frozen=True)
class ClusterCounts:
    history: list[float]  # requests per second, oldest first, last `window_seconds` seconds
    requests: int
    errors: int
    rps: float


class ClusterTraffic:
    def __init__(
        self,
        client: Redis,
        *,
        window_seconds: int = 60,
        flush_seconds: float = 1.0,
        timeout_seconds: float = 0.2,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._window = window_seconds
        self._flush_seconds = flush_seconds
        self._timeout = timeout_seconds
        self._clock = clock
        self._pending: dict[int, list[int]] = defaultdict(lambda: [0, 0])
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def record(self, success: bool) -> None:
        counts = self._pending[int(self._clock())]
        counts[0] += 1
        counts[1] += not success

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="cluster-traffic")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self.flush()

    async def _loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self._flush_seconds)
            await self.flush()

    async def flush(self) -> None:
        pending, self._pending = self._pending, defaultdict(lambda: [0, 0])
        if not pending:
            return
        pipeline = self._client.pipeline(transaction=False)
        for second, (requests, errors) in pending.items():
            for kind, value in (("req", requests), ("err", errors)):
                if value:
                    key = f"{KEY_PREFIX}:{second}:{kind}"
                    pipeline.incrby(key, value)
                    pipeline.expire(key, TTL_SECONDS)
        try:
            await asyncio.wait_for(pipeline.execute(), self._timeout)
        except (RedisError, OSError, TimeoutError) as exc:
            logger.debug("Dropping cluster traffic counts", extra={"error": repr(exc)})

    async def read(self, rps_seconds: int = 5) -> ClusterCounts | None:
        now = int(self._clock())
        seconds = list(range(now - self._window + 1, now + 1))
        keys = [f"{KEY_PREFIX}:{s}:{kind}" for s in seconds for kind in ("req", "err")]
        try:
            values = await asyncio.wait_for(self._client.mget(keys), self._timeout)
        except (RedisError, OSError, TimeoutError):
            return None
        counts = [int(v) if v is not None else 0 for v in values]
        # Add this replica's not-yet-flushed counts; other replicas' last second lags by <= 1 s.
        requests = [
            r + self._pending.get(s, [0, 0])[0] for s, r in zip(seconds, counts[0::2], strict=True)
        ]
        errors = [
            e + self._pending.get(s, [0, 0])[1] for s, e in zip(seconds, counts[1::2], strict=True)
        ]
        return ClusterCounts(
            history=[float(r) for r in requests],
            requests=sum(requests),
            errors=sum(errors),
            rps=round(sum(requests[-rps_seconds:]) / rps_seconds, 2),
        )
