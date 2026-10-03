"""Bounded synthetic traffic generator that drives demo sessions.

Safety limits are enforced here rather than trusted from callers: one session at a time, the
request rate is capped by DEMO_MAX_RPS, the duration by DEMO_MAX_DURATION_SECONDS, in-flight
requests are bounded (excess is dropped, never queued), and every exit path cancels outstanding
work.
"""

import asyncio
import contextlib
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from .errors import AppError
from .events import EventMetadata, EventPublisher, new_event
from .metrics import ApiMetrics
from .prediction import PredictionService
from .schemas import (
    DemoChannel,
    DemoProfile,
    DemoStartRequest,
    DemoState,
    DemoStatus,
    DemoSummary,
    PredictionFeatures,
)

logger = logging.getLogger(__name__)

ACTIVE_STATES: frozenset[DemoState] = frozenset({"starting", "running", "cooling_down"})
DEFAULT_DURATION_SECONDS: dict[DemoProfile, int] = {"standard": 60, "spike": 90}
DEFAULT_RATE_FRACTION: dict[DemoProfile, float] = {"standard": 0.4, "spike": 1.0}
SYNTHETIC_POOL_SIZE = 400

StopReason = Literal["duration_reached", "manual", "shutdown", "error"]


def target_rate(profile: DemoProfile, progress: float, target_rps: int) -> float:
    """Return the requested rate at `progress` (0..1) through a session."""
    if profile == "standard":
        return target_rps * min(1.0, progress / 0.2)
    baseline = 0.15 * target_rps
    if progress < 0.2 or progress >= 0.7:
        return baseline
    ramp = min(1.0, (progress - 0.2) / 0.05)
    return baseline + (target_rps - baseline) * ramp


def synthetic_pool(rng: random.Random, size: int = SYNTHETIC_POOL_SIZE) -> list[PredictionFeatures]:
    return [
        PredictionFeatures(
            amount=round(min(rng.lognormvariate(5.5, 1.1), 999_999), 2),
            events_per_hour=float(rng.randint(0, 40)),
            distance_km=round(min(rng.expovariate(1 / 300), 19_999), 1),
        )
        for _ in range(size)
    ]


@dataclass
class _Session:
    session_id: str
    profile: DemoProfile
    channel: DemoChannel
    target_rps: int
    duration_seconds: int
    started_at: datetime
    started_monotonic: float
    state: DemoState = "starting"
    current_target_rps: float = 0.0
    generated: int = 0
    completed: int = 0
    errors: int = 0
    dropped: int = 0
    cache_hits: int = 0
    cache_lookups: int = 0
    per_second: dict[int, int] = field(default_factory=dict)
    ended_at: datetime | None = None
    ended_monotonic: float | None = None
    stop_reason: StopReason | None = None
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def elapsed(self) -> float:
        end = self.ended_monotonic if self.ended_monotonic is not None else time.monotonic()
        return end - self.started_monotonic


class DemoOrchestrator:
    def __init__(
        self,
        predictions: PredictionService,
        metrics: ApiMetrics,
        max_rps: int,
        max_duration_seconds: int,
        *,
        publisher: EventPublisher | None = None,
        tick_seconds: float = 0.1,
        drain_timeout_seconds: float = 5.0,
        max_in_flight: int = 64,
    ) -> None:
        self._predictions = predictions
        self._publisher = publisher
        self._metrics = metrics
        self._max_rps = max_rps
        self._max_duration = max_duration_seconds
        self._tick = tick_seconds
        self._drain_timeout = drain_timeout_seconds
        self._max_in_flight = max_in_flight
        self._lock = asyncio.Lock()
        self._session: _Session | None = None
        self._task: asyncio.Task[None] | None = None
        self._in_flight: set[asyncio.Task[None]] = set()

    async def start(self, request: DemoStartRequest) -> DemoStatus:
        async with self._lock:
            if self._session is not None and self._session.state in ACTIVE_STATES:
                raise AppError(409, "demo_already_running", "A demo session is already active.")
            if request.channel == "events" and (
                self._publisher is None or not await self._publisher.ready()
            ):
                raise AppError(
                    503, "kafka_unavailable", "The event pipeline is unavailable for this demo."
                )
            default_rps = max(1, round(self._max_rps * DEFAULT_RATE_FRACTION[request.profile]))
            duration = request.duration_seconds or DEFAULT_DURATION_SECONDS[request.profile]
            session = _Session(
                session_id=str(uuid.uuid4()),
                profile=request.profile,
                channel=request.channel,
                target_rps=min(request.target_rps or default_rps, self._max_rps),
                duration_seconds=min(duration, self._max_duration),
                started_at=datetime.now(UTC),
                started_monotonic=time.monotonic(),
            )
            self._session = session
            self._task = asyncio.create_task(self._run(session), name="demo-session")
            logger.info(
                "Demo session started",
                extra={
                    "demo_session_id": session.session_id,
                    "profile": session.profile,
                    "channel": session.channel,
                    "target_rps": session.target_rps,
                    "duration_seconds": session.duration_seconds,
                },
            )
            return self.status()

    async def stop(self) -> DemoStatus:
        session = self._session
        if session is not None and session.state in {"starting", "running"}:
            session.stop_reason = "manual"
            session.stop_event.set()
        return self.status()

    async def shutdown(self) -> None:
        task = self._task
        if task is None or task.done():
            return
        if self._session is not None:
            self._session.stop_reason = "shutdown"
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    def status(self) -> DemoStatus:
        session = self._session
        if session is None:
            return DemoStatus(
                session_id=None,
                state="idle",
                profile=None,
                channel=None,
                current_target_rps=0,
                target_rps=0,
                max_rps=self._max_rps,
                elapsed_seconds=0,
                duration_seconds=0,
                max_duration_seconds=self._max_duration,
                generated_requests=0,
                errors=0,
                started_at=None,
                ended_at=None,
                stop_reason=None,
                summary=None,
            )
        summary = None
        if session.state not in ACTIVE_STATES:
            summary = DemoSummary(
                total_requests=session.generated,
                errors=session.errors,
                dropped=session.dropped,
                peak_rps=max(session.per_second.values(), default=0),
                cache_hit_rate=round(100 * session.cache_hits / session.cache_lookups, 2)
                if session.cache_lookups
                else None,
                duration_seconds=round(session.elapsed, 2),
            )
        return DemoStatus(
            session_id=session.session_id,
            state=session.state,
            profile=session.profile,
            channel=session.channel,
            current_target_rps=round(session.current_target_rps, 2),
            target_rps=session.target_rps,
            max_rps=self._max_rps,
            elapsed_seconds=round(session.elapsed, 2),
            duration_seconds=session.duration_seconds,
            max_duration_seconds=self._max_duration,
            generated_requests=session.generated,
            errors=session.errors,
            started_at=session.started_at,
            ended_at=session.ended_at,
            stop_reason=session.stop_reason,
            summary=summary,
        )

    async def _run(self, session: _Session) -> None:
        self._metrics.demo_active.set(1)
        try:
            session.state = "running"
            await self._generate(session)
            session.state = "cooling_down"
            session.current_target_rps = 0
            self._metrics.demo_target_rps.set(0)
            await self._drain()
            session.state = "completed"
        except asyncio.CancelledError:
            session.state = "failed"
            session.stop_reason = session.stop_reason or "shutdown"
            raise
        except Exception:
            logger.exception("Demo session failed", extra={"demo_session_id": session.session_id})
            session.state = "failed"
            session.stop_reason = "error"
        finally:
            for task in list(self._in_flight):
                task.cancel()
            session.current_target_rps = 0
            session.ended_at = datetime.now(UTC)
            session.ended_monotonic = time.monotonic()
            self._metrics.demo_active.set(0)
            self._metrics.demo_target_rps.set(0)
            self._metrics.demo_sessions.labels(session.state, session.stop_reason or "none").inc()
            logger.info(
                "Demo session ended",
                extra={
                    "demo_session_id": session.session_id,
                    "state": session.state,
                    "stop_reason": session.stop_reason,
                    "generated_requests": session.generated,
                    "errors": session.errors,
                },
            )

    async def _generate(self, session: _Session) -> None:
        rng = random.Random(session.session_id)
        pool = synthetic_pool(rng)
        credit = 0.0
        last = time.monotonic()
        while True:
            now = time.monotonic()
            elapsed = now - session.started_monotonic
            if session.stop_reason is not None:
                return
            if elapsed >= session.duration_seconds:
                session.stop_reason = "duration_reached"
                return

            rate = target_rate(
                session.profile, elapsed / session.duration_seconds, session.target_rps
            )
            session.current_target_rps = rate
            self._metrics.demo_target_rps.set(rate)
            credit += rate * (now - last)
            last = now
            bucket = int(elapsed)
            while credit >= 1:
                credit -= 1
                if session.per_second.get(bucket, 0) >= self._max_rps:
                    # Hard cap: never exceed DEMO_MAX_RPS within any wall-clock second.
                    credit = 0
                    break
                if len(self._in_flight) >= self._max_in_flight:
                    session.dropped += 1
                    self._metrics.demo_dropped.inc()
                    continue
                # A skewed pick over a fixed pool lets the cache warm up as the demo runs.
                features = pool[min(len(pool) - 1, int(rng.expovariate(1 / 40)))]
                task = asyncio.create_task(self._one(session, features))
                self._in_flight.add(task)
                task.add_done_callback(self._in_flight.discard)
                session.generated += 1
                session.per_second[bucket] = session.per_second.get(bucket, 0) + 1

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(session.stop_event.wait(), timeout=self._tick)

    async def _one(self, session: _Session, features: PredictionFeatures) -> None:
        if session.channel == "events":
            await self._publish_one(session, features)
            return
        try:
            outcome = await self._predictions.predict(features, source="demo")
        except AppError:
            session.errors += 1
            return
        session.completed += 1
        if outcome.cache != "bypass":
            session.cache_lookups += 1
            session.cache_hits += outcome.cache == "hit"

    async def _publish_one(self, session: _Session, features: PredictionFeatures) -> None:
        assert self._publisher is not None
        event = new_event(
            features,
            request_id=str(uuid.uuid4()),
            model_name=self._predictions.model_name,
            model_version=self._predictions.model_version,
            metadata=EventMetadata(source="dashboard-demo", demo_session_id=session.session_id),
        )
        try:
            await self._publisher.publish(event)
        except AppError:
            session.errors += 1
            return
        session.completed += 1

    async def _drain(self) -> None:
        if self._in_flight:
            await asyncio.wait(set(self._in_flight), timeout=self._drain_timeout)
