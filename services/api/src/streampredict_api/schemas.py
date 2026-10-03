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
    backend: str
    predictions_in_window: int
    error_rate: float | None = Field(description="Percent of predictions that failed (window).")
    label_distribution: dict[RiskLabel, int]


class ConsumerMember(BaseModel):
    member_id: str
    host: str
    partitions: list[int]


class InfrastructureMetrics(BaseModel):
    """What the gateway can observe about the deployment today.

    Consumer replicas come from the Kafka group membership, so they are real on any platform. Pod
    CPU, memory, and HPA events need the Kubernetes deployment (M9) and are not reported yet.
    """

    platform: str
    status: DependencyStatus
    consumer_group: str
    group_state: str | None
    consumer_replicas: int | None
    replicas_history: list[int]
    members: list[ConsumerMember]


class ModelVersionInfo(BaseModel):
    version: str
    profile: str = ""
    description: str = ""
    status: str = ""
    auc: float | None = None


class ActiveReleaseInfo(BaseModel):
    version: str
    previous: str | None
    stage: str
    started_at: float
    detail: str = ""


class ReleaseRecordInfo(BaseModel):
    version: str
    previous: str | None
    outcome: str
    stage: str
    reasons: list[str] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)
    started_at: float
    finished_at: float


class ControllerEventInfo(BaseModel):
    at: float
    level: str
    message: str


class DeploymentStatus(BaseModel):
    """Release state reported by the deployment controller (M6)."""

    status: DependencyStatus
    champion: str | None = None
    serving_default: str | None = None
    versions: list[ModelVersionInfo] = Field(default_factory=list)
    active: ActiveReleaseInfo | None = None
    history: list[ReleaseRecordInfo] = Field(default_factory=list)
    events: list[ControllerEventInfo] = Field(default_factory=list)


class ReleaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = Field(pattern=r"^[0-9]{1,6}$")


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
    # None when the event pipeline is disabled; consumer replicas are read from Kafka.
    infrastructure: InfrastructureMetrics | None = None
    # None when no deployment controller is configured (CONTROLLER_URL is empty).
    deployment: DeploymentStatus | None = None


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None
    details: list[dict[str, object]] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody
