"""Prometheus metrics and the rolling window that feeds the dashboard overview."""

import math
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from .schemas import CacheStatus, RiskLabel

LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2.5)
CACHE_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1)


class PredictionMetrics:
    """Collectors for the cache-aside prediction path, shared by the API and the consumer.

    Labels are restricted to bounded sets (route templates, status codes, fixed enums) to keep
    series cardinality predictable.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.predictions = Counter(
            "streampredict_predictions_total",
            "Predictions served, by traffic source, cache outcome, and result.",
            ["source", "cache", "outcome"],
            registry=self.registry,
        )
        self.prediction_duration = Histogram(
            "streampredict_prediction_duration_seconds",
            "End-to-end prediction duration including cache lookup.",
            ["source"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.prediction_labels = Counter(
            "streampredict_prediction_labels_total",
            "Successful predictions by predicted label (the model's output distribution).",
            ["source", "label"],
            registry=self.registry,
        )
        self.cache_duration = Histogram(
            "streampredict_cache_operation_duration_seconds",
            "Redis cache operation duration.",
            ["operation"],
            buckets=CACHE_BUCKETS,
            registry=self.registry,
        )
        self.redis_connections = Gauge(
            "streampredict_redis_connections",
            "Redis client connections in this process, by state (in_use, idle).",
            ["state"],
            registry=self.registry,
        )
        self.cache_errors = Counter(
            "streampredict_cache_errors_total",
            "Redis cache operations that failed or were skipped by the circuit breaker.",
            ["operation", "reason"],
            registry=self.registry,
        )


class DemoMetrics:
    """Collectors for the demo load generator (in the gateway or the standalone orchestrator)."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.demo_active = Gauge(
            "streampredict_demo_active",
            "Whether a demo session is generating or draining traffic.",
            registry=self.registry,
        )
        self.demo_target_rps = Gauge(
            "streampredict_demo_target_rps",
            "Current target request rate of the demo load generator.",
            registry=self.registry,
        )
        self.demo_sessions = Counter(
            "streampredict_demo_sessions_total",
            "Demo sessions by final state and stop reason.",
            ["state", "reason"],
            registry=self.registry,
        )
        self.demo_generated = Counter(
            "streampredict_demo_generated_requests_total",
            "Synthetic requests or events sent by the demo load generator.",
            registry=self.registry,
        )
        self.demo_elapsed = Gauge(
            "streampredict_demo_elapsed_seconds",
            "Elapsed time of the current demo session (0 when idle).",
            registry=self.registry,
        )
        self.demo_dropped = Counter(
            "streampredict_demo_dropped_requests_total",
            "Synthetic requests skipped because the in-flight limit was reached.",
            registry=self.registry,
        )


class ApiMetrics(PredictionMetrics, DemoMetrics):
    """Prometheus collectors owned by one API application instance."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        PredictionMetrics.__init__(self, registry)
        DemoMetrics.__init__(self, self.registry)
        self.http_requests = Counter(
            "streampredict_http_requests_total",
            "HTTP requests handled by the API gateway.",
            ["method", "route", "status"],
            registry=self.registry,
        )
        self.http_duration = Histogram(
            "streampredict_http_request_duration_seconds",
            "HTTP request duration.",
            ["method", "route"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.http_in_flight = Gauge(
            "streampredict_http_requests_in_flight",
            "HTTP requests currently being handled.",
            registry=self.registry,
        )
        self.model_info = Gauge(
            "streampredict_model_info",
            "Model currently served by the gateway (value is always 1).",
            ["model_name", "model_version"],
            registry=self.registry,
        )
        self.events_published = Counter(
            "streampredict_events_published_total",
            "Prediction events handed to Kafka, by outcome.",
            ["outcome"],
            registry=self.registry,
        )
        self.event_publish_duration = Histogram(
            "streampredict_event_publish_duration_seconds",
            "Time to get a Kafka acknowledgement for a published event.",
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.kafka_consumer_lag = Gauge(
            "streampredict_kafka_consumer_lag",
            "Unconsumed prediction events for the consumer group, as seen by the gateway.",
            registry=self.registry,
        )


@dataclass(frozen=True)
class PredictionSample:
    timestamp: float
    latency_ms: float
    success: bool
    cache: CacheStatus
    label: RiskLabel | None


@dataclass(frozen=True)
class WindowSnapshot:
    rps: float
    rps_history: list[float]
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    success_rate: float | None
    requests_in_window: int
    errors_in_window: int
    hit_rate: float | None
    miss_rate: float | None
    bypass_count: int
    lookup_p95_ms: float | None
    label_counts: dict[RiskLabel, int]


def percentile(values: list[float], fraction: float) -> float | None:
    """Return the nearest-rank percentile of `values`, or None when empty."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1]


class RollingWindow:
    """Bounded in-memory window of recent predictions for low-latency dashboard aggregates.

    Prometheus remains the source of truth for long-term metrics; this window only serves the
    overview endpoint and is per process.
    """

    def __init__(
        self,
        window_seconds: int = 60,
        max_samples: int = 200_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.window_seconds = window_seconds
        self._clock = clock
        self._predictions: deque[PredictionSample] = deque(maxlen=max_samples)
        self._cache_lookups: deque[tuple[float, float]] = deque(maxlen=max_samples)

    def record_prediction(
        self,
        latency_ms: float,
        success: bool,
        cache: CacheStatus,
        label: RiskLabel | None = None,
    ) -> None:
        self._predictions.append(PredictionSample(self._clock(), latency_ms, success, cache, label))

    def record_cache_lookup(self, latency_ms: float) -> None:
        self._cache_lookups.append((self._clock(), latency_ms))

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._predictions and self._predictions[0].timestamp < cutoff:
            self._predictions.popleft()
        while self._cache_lookups and self._cache_lookups[0][0] < cutoff:
            self._cache_lookups.popleft()

    def snapshot(self, rps_seconds: int = 5) -> WindowSnapshot:
        now = self._clock()
        self._prune(now)
        samples = list(self._predictions)

        history = [0.0] * self.window_seconds
        for sample in samples:
            age = int(now - sample.timestamp)
            if 0 <= age < self.window_seconds:
                history[self.window_seconds - 1 - age] += 1
        recent = sum(1 for sample in samples if now - sample.timestamp < rps_seconds)

        successful = [sample.latency_ms for sample in samples if sample.success]
        errors = sum(1 for sample in samples if not sample.success)
        hits = sum(1 for sample in samples if sample.cache == "hit")
        misses = sum(1 for sample in samples if sample.cache == "miss")
        bypass = sum(1 for sample in samples if sample.cache == "bypass")
        lookups = [latency for _, latency in self._cache_lookups]
        cacheable = hits + misses
        label_counts: dict[RiskLabel, int] = {"low_risk": 0, "review": 0, "high_risk": 0}
        for sample in samples:
            if sample.label is not None:
                label_counts[sample.label] += 1

        return WindowSnapshot(
            rps=round(recent / rps_seconds, 2),
            rps_history=history,
            p50_ms=percentile(successful, 0.50),
            p95_ms=percentile(successful, 0.95),
            p99_ms=percentile(successful, 0.99),
            success_rate=round(100 * (len(samples) - errors) / len(samples), 3)
            if samples
            else None,
            requests_in_window=len(samples),
            errors_in_window=errors,
            hit_rate=round(100 * hits / cacheable, 2) if cacheable else None,
            miss_rate=round(100 * misses / cacheable, 2) if cacheable else None,
            bypass_count=bypass,
            lookup_p95_ms=percentile(lookups, 0.95),
            label_counts=label_counts,
        )
