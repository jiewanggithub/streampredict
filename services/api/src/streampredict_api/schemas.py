"""Request and response schemas for the public API."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

RiskLabel = Literal["low_risk", "review", "high_risk"]
CacheStatus = Literal["hit", "miss", "bypass"]
DependencyStatus = Literal["ok", "unavailable"]
DemoState = Literal["idle", "starting", "running", "cooling_down", "completed", "failed"]
DemoProfile = Literal["standard", "spike"]
# `sync` calls the prediction path directly; `events` publishes to Kafka for the consumers.
DemoChannel = Literal["sync", "events"]


class PredictionFeatures(BaseModel):
    """Synthetic transaction features scored by the demo model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    amount: float = Field(gt=0, le=1_000_000, description="Transaction amount.")
    events_per_hour: float = Field(ge=0, le=10_000, description="Recent event velocity.")
    distance_km: float = Field(ge=0, le=20_000, description="Distance from usual location.")


class PredictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    features: PredictionFeatures


class PredictResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    request_id: str
    model_name: str
    model_version: str
    label: RiskLabel
    score: float = Field(ge=0, le=1)
    cache: CacheStatus
    latency_ms: float


class EventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    features: PredictionFeatures


class EventAccepted(BaseModel):
    event_id: str
    request_id: str
    topic: str
    partition: int
    offset: int


class HealthResponse(BaseModel):
    status: Literal["ok"]


class ReadyResponse(BaseModel):
    status: Literal["ready", "degraded", "not_ready"]
    checks: dict[str, DependencyStatus]


class DemoStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile: DemoProfile = "spike"
    channel: DemoChannel = "sync"
    target_rps: int | None = Field(default=None, ge=1, description="Capped by DEMO_MAX_RPS.")
    duration_seconds: int | None = Field(
        default=None, ge=1, description="Capped by DEMO_MAX_DURATION_SECONDS."
    )


class DemoSummary(BaseModel):
    total_requests: int
    errors: int
    dropped: int
    peak_rps: int
    cache_hit_rate: float | None
    duration_seconds: float


class DemoStatus(BaseModel):
    session_id: str | None
    state: DemoState
    profile: DemoProfile | None
    channel: DemoChannel | None
    current_target_rps: float
    target_rps: int
    max_rps: int
    elapsed_seconds: float
    duration_seconds: int
    max_duration_seconds: int
    generated_requests: int
    errors: int
    started_at: datetime | None
    ended_at: datetime | None
    stop_reason: Literal["duration_reached", "manual", "shutdown", "error"] | None
    summary: DemoSummary | None


class TrafficMetrics(BaseModel):
    rps: float
    rps_history: list[float]
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    success_rate: float | None
    requests_in_window: int
    errors_in_window: int


class CacheMetrics(BaseModel):
    hit_rate: float | None
    miss_rate: float | None
    bypass_count: int
    lookup_p95_ms: float | None
    ttl_seconds: int


class PartitionLag(BaseModel):
    partition: int
    lag: int


class KafkaMetrics(BaseModel):
    status: DependencyStatus
    topic: str
    consumer_group: str
    partitions: int | None
    lag: int | None
    lag_history: list[int]
    incoming_rate: float | None = Field(description="Events appended to the topic per second.")
    consumer_rate: float | None = Field(description="Events committed by the group per second.")
    lag_by_partition: list[PartitionLag]


class ModelInfo(BaseModel):
    name: str
    version: str


class MetricsOverview(BaseModel):
    generated_at: datetime
    window_seconds: int
    model: ModelInfo
    traffic: TrafficMetrics
    cache: CacheMetrics
    dependencies: dict[str, DependencyStatus]
    demo: DemoStatus
    # None when the event pipeline is disabled (KAFKA_BOOTSTRAP_SERVERS is empty).
    kafka: KafkaMetrics | None = None
    # Populated once the Kubernetes deployment (Phase 4) exists.
    infrastructure: None = None


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None
    details: list[dict[str, object]] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody
