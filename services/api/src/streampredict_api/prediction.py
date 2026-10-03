"""Cache-aside prediction flow shared by the API and the demo load generator."""

import asyncio
import time
from dataclasses import dataclass
from typing import Literal

from .cache import PredictionCache, prediction_key
from .errors import AppError
from .inference import InferenceClient, InferenceError, InferenceResult
from .metrics import ApiMetrics, RollingWindow
from .schemas import CacheStatus, PredictionFeatures

TrafficSource = Literal["api", "demo"]


@dataclass(frozen=True)
class PredictionOutcome:
    result: InferenceResult
    cache: CacheStatus
    latency_ms: float


class PredictionService:
    def __init__(
        self,
        cache: PredictionCache,
        inference: InferenceClient,
        metrics: ApiMetrics,
        window: RollingWindow,
        inference_timeout_seconds: float,
        max_in_flight: int,
    ) -> None:
        self._cache = cache
        self._inference = inference
        self._metrics = metrics
        self._window = window
        self._timeout = inference_timeout_seconds
        self._max_in_flight = max_in_flight
        self._in_flight = 0
        self._pending: dict[str, asyncio.Future[InferenceResult]] = {}

    @property
    def model_name(self) -> str:
        return self._inference.model_name

    @property
    def model_version(self) -> str:
        return self._inference.model_version

    async def predict(
        self, features: PredictionFeatures, source: TrafficSource = "api"
    ) -> PredictionOutcome:
        if self._in_flight >= self._max_in_flight:
            raise AppError(
                503,
                "overloaded",
                "Too many predictions in flight; retry shortly.",
                headers={"Retry-After": "1"},
            )
        self._in_flight += 1
        started = time.perf_counter()
        cache_status: CacheStatus = "bypass"
        try:
            key = prediction_key(self.model_name, self.model_version, features)
            cache_status, result = await self._cache.get(key)
            if result is None:
                result = await self._infer_once(key, features)
                if cache_status == "miss":
                    await self._cache.set(key, result)
        except AppError:
            self._record(source, cache_status, started, success=False)
            raise
        finally:
            self._in_flight -= 1
        latency_ms = self._record(source, cache_status, started, success=True)
        return PredictionOutcome(result=result, cache=cache_status, latency_ms=latency_ms)

    async def _infer_once(self, key: str, features: PredictionFeatures) -> InferenceResult:
        """Run inference, coalescing concurrent misses for the same key (cache stampede guard)."""
        pending = self._pending.get(key)
        if pending is not None:
            return await asyncio.shield(pending)

        future: asyncio.Future[InferenceResult] = asyncio.get_running_loop().create_future()
        self._pending[key] = future
        try:
            result = await asyncio.wait_for(self._inference.predict(features), self._timeout)
        except (InferenceError, TimeoutError) as exc:
            error = AppError(503, "inference_unavailable", "The model backend is unavailable.")
            future.set_exception(error)
            # Mark the exception as retrieved when no other request is waiting on it.
            future.exception()
            raise error from exc
        else:
            future.set_result(result)
            return result
        finally:
            del self._pending[key]
            if not future.done():
                # The leader was cancelled or failed unexpectedly; release any waiters.
                future.set_exception(
                    AppError(503, "inference_unavailable", "The model backend is unavailable.")
                )
                future.exception()

    def _record(
        self, source: TrafficSource, cache: CacheStatus, started: float, success: bool
    ) -> float:
        elapsed = time.perf_counter() - started
        outcome = "success" if success else "error"
        self._metrics.predictions.labels(source, cache, outcome).inc()
        if success:
            self._metrics.prediction_duration.labels(source).observe(elapsed)
        self._window.record_prediction(elapsed * 1000, success, cache)
        return round(elapsed * 1000, 2)

    async def ready(self) -> bool:
        try:
            return await asyncio.wait_for(self._inference.ready(), self._timeout)
        except (InferenceError, TimeoutError):
            return False
