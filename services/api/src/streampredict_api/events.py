"""Prediction event contract and the Kafka publisher used by the API gateway.

Events follow the contract in docs/roadmap.md. Consumers accept any `1.x` schema version; a breaking
change must bump the major version and keep the old one readable during the rollout.
"""

import asyncio
import contextlib
import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .errors import AppError
from .metrics import ApiMetrics
from .schemas import CacheStatus, PredictionFeatures, RiskLabel

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"
SUPPORTED_SCHEMA_MAJOR = "1"

EventSource = Literal["api", "dashboard-demo"]


class EventMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: EventSource = "api"
    demo_session_id: str | None = None


class PredictionEvent(BaseModel):
    """An asynchronous prediction request published to `prediction-events`."""

    # Unknown fields are ignored so newer 1.x producers stay readable by older consumers.
    model_config = ConfigDict(extra="ignore", frozen=True, protected_namespaces=())

    schema_version: str
    event_id: str
    request_id: str
    created_at: datetime
    model_name: str
    model_version: str
    features: PredictionFeatures
    metadata: EventMetadata = Field(default_factory=EventMetadata)

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, value: str) -> str:
        if value.split(".", 1)[0] != SUPPORTED_SCHEMA_MAJOR:
            raise ValueError(f"unsupported schema_version {value!r}")
        return value


class PredictionResultEvent(BaseModel):
    """The outcome of a prediction event, published to `prediction-results`."""

    model_config = ConfigDict(protected_namespaces=())

    schema_version: str = SCHEMA_VERSION
    event_id: str
    request_id: str
    model_name: str
    model_version: str
    label: RiskLabel
    score: float = Field(ge=0, le=1)
    cache: CacheStatus
    latency_ms: float
    created_at: datetime
    processed_at: datetime
    metadata: EventMetadata


class InvalidEvent(Exception):
    """The record cannot be parsed into a supported PredictionEvent; retrying will not help."""


def parse_event(raw: bytes | None) -> PredictionEvent:
    if raw is None:
        raise InvalidEvent("empty record value")
    try:
        return PredictionEvent.model_validate_json(raw)
    except ValidationError as exc:
        # Report field locations only; never copy feature values into errors or logs.
        fields = sorted({".".join(str(part) for part in error["loc"]) for error in exc.errors()})
        raise InvalidEvent(f"invalid fields: {', '.join(fields) or 'payload'}") from exc


def new_event(
    features: PredictionFeatures,
    *,
    request_id: str,
    model_name: str,
    model_version: str,
    metadata: EventMetadata | None = None,
) -> PredictionEvent:
    return PredictionEvent(
        schema_version=SCHEMA_VERSION,
        event_id=str(uuid.uuid4()),
        request_id=request_id,
        created_at=datetime.now(UTC),
        model_name=model_name,
        model_version=model_version,
        features=features,
        metadata=metadata or EventMetadata(),
    )


class PublishReceipt(BaseModel):
    topic: str
    partition: int
    offset: int


class EventPublisher(Protocol):
    async def publish(self, event: PredictionEvent) -> PublishReceipt: ...

    async def ready(self) -> bool: ...

    async def close(self) -> None: ...


class DisabledEventPublisher:
    """Used when KAFKA_BOOTSTRAP_SERVERS is empty."""

    async def publish(self, event: PredictionEvent) -> PublishReceipt:
        raise AppError(503, "events_disabled", "The event pipeline is not configured.")

    async def ready(self) -> bool:
        return False

    async def close(self) -> None:
        return None


class KafkaEventPublisher:
    """Idempotent Kafka producer that connects lazily.

    The gateway must start and keep serving synchronous predictions while Kafka is down, so a
    failed connection is retried at most once per `reconnect_interval` instead of blocking startup
    or every request.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        metrics: ApiMetrics,
        *,
        timeout_seconds: float,
        reconnect_interval_seconds: float,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._metrics = metrics
        self._timeout = timeout_seconds
        self._reconnect_interval = reconnect_interval_seconds
        self._producer: Any = None
        self._next_attempt = 0.0
        self._lock = asyncio.Lock()
        self._connect_task: asyncio.Task[Any] | None = None

    async def _connected(self) -> Any:
        if self._producer is not None:
            return self._producer
        async with self._lock:
            if self._producer is not None:
                return self._producer
            if time.monotonic() < self._next_attempt:
                return None
            producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                client_id="streampredict-api",
                acks="all",
                enable_idempotence=True,
                linger_ms=5,
                request_timeout_ms=round(self._timeout * 1000),
            )
            try:
                await asyncio.wait_for(producer.start(), self._timeout)
            except (KafkaError, OSError, TimeoutError) as exc:
                self._next_attempt = time.monotonic() + self._reconnect_interval
                logger.warning("Kafka unavailable; events disabled", extra={"error": repr(exc)})
                await producer.stop()
                return None
            logger.info("Kafka producer connected", extra={"topic": self._topic})
            self._producer = producer
            return producer

    async def publish(self, event: PredictionEvent) -> PublishReceipt:
        producer = await self._connected()
        if producer is None:
            self._metrics.events_published.labels("unavailable").inc()
            raise AppError(
                503,
                "kafka_unavailable",
                "The event pipeline is unavailable; retry shortly.",
                headers={"Retry-After": "5"},
            )
        started = time.perf_counter()
        try:
            metadata = await asyncio.wait_for(
                producer.send_and_wait(
                    self._topic,
                    value=event.model_dump_json().encode(),
                    key=event.event_id.encode(),
                    headers=[("schema_version", event.schema_version.encode())],
                ),
                self._timeout,
            )
        except (KafkaError, TimeoutError) as exc:
            self._metrics.events_published.labels("error").inc()
            logger.warning("Event publish failed", extra={"error": repr(exc)})
            raise AppError(503, "kafka_unavailable", "Publishing the event failed.") from exc
        self._metrics.events_published.labels("success").inc()
        self._metrics.event_publish_duration.observe(time.perf_counter() - started)
        return PublishReceipt(
            topic=metadata.topic, partition=metadata.partition, offset=metadata.offset
        )

    async def ready(self) -> bool:
        """Report connection state without blocking; schedules a reconnect attempt when due."""
        idle = self._connect_task is None or self._connect_task.done()
        if self._producer is None and idle and time.monotonic() >= self._next_attempt:
            self._connect_task = asyncio.create_task(self._connected(), name="kafka-connect")
        return self._producer is not None

    async def close(self) -> None:
        if self._connect_task is not None and not self._connect_task.done():
            self._connect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._connect_task
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
