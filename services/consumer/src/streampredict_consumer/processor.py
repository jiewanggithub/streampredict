"""Broker-independent batch processing: parse, deduplicate, predict, retry, dead-letter.

The worker hands in polled records and gets back the messages to publish. Offsets are committed
only after those messages are acknowledged, so nothing here talks to Kafka directly.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from streampredict_api.errors import AppError
from streampredict_api.events import (
    InvalidEvent,
    PredictionEvent,
    PredictionResultEvent,
    parse_event,
)
from streampredict_api.prediction import PredictionService

from .idempotency import IdempotencyStore
from .metrics import ConsumerMetrics

logger = logging.getLogger(__name__)

Headers = list[tuple[str, bytes]]


@dataclass(frozen=True)
class InboundRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


@dataclass(frozen=True)
class OutboundMessage:
    topic: str
    key: bytes | None
    value: bytes
    headers: Headers = field(default_factory=list)


@dataclass
class BatchOutcome:
    messages: list[OutboundMessage] = field(default_factory=list)
    # Marked done once `messages` are acknowledged by Kafka.
    completed_event_ids: list[str] = field(default_factory=list)
    succeeded: int = 0
    duplicates: int = 0
    dead_lettered: int = 0


@dataclass(frozen=True)
class _Failure:
    code: str
    message: str
    attempts: int


def is_retryable(error: AppError) -> bool:
    """Backend unavailability and overload are transient; anything else will fail again."""
    return error.status_code in {429, 503}


class EventProcessor:
    def __init__(
        self,
        predictions: PredictionService,
        idempotency: IdempotencyStore,
        metrics: ConsumerMetrics,
        *,
        result_topic: str,
        dead_letter_topic: str,
        max_attempts: int,
        retry_backoff_seconds: float,
        concurrency: int,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._predictions = predictions
        self._idempotency = idempotency
        self._metrics = metrics
        self._result_topic = result_topic
        self._dead_letter_topic = dead_letter_topic
        self._max_attempts = max_attempts
        self._backoff = retry_backoff_seconds
        self._semaphore = asyncio.Semaphore(concurrency)
        self._sleep = sleep

    async def process_batch(self, records: list[InboundRecord]) -> BatchOutcome:
        outcome = BatchOutcome()
        parsed: list[tuple[InboundRecord, PredictionEvent]] = []
        for record in records:
            try:
                parsed.append((record, parse_event(record.value)))
            except InvalidEvent as exc:
                failure = _Failure("invalid_event", str(exc), attempts=0)
                self._dead_letter(outcome, record, failure)

        # Redelivered or duplicated events: skip those already done or repeated in this batch.
        done = await self._idempotency.seen([event.event_id for _, event in parsed])
        unique: list[tuple[InboundRecord, PredictionEvent]] = []
        for record, event in parsed:
            if event.event_id in done:
                outcome.duplicates += 1
                self._metrics.events_processed.labels("duplicate").inc()
                continue
            done.add(event.event_id)
            unique.append((record, event))

        handled = await asyncio.gather(*(self._handle(event) for _, event in unique))
        for (record, event), result in zip(unique, handled, strict=True):
            if isinstance(result, _Failure):
                self._dead_letter(outcome, record, result)
                continue
            outcome.messages.append(
                OutboundMessage(
                    topic=self._result_topic,
                    key=event.event_id.encode(),
                    value=result.model_dump_json().encode(),
                    headers=[("schema_version", result.schema_version.encode())],
                )
            )
            outcome.completed_event_ids.append(event.event_id)
            outcome.succeeded += 1
            self._metrics.events_processed.labels("success").inc()
            age = (result.processed_at - event.created_at).total_seconds()
            self._metrics.event_age.observe(max(0.0, age))
        return outcome

    async def _handle(self, event: PredictionEvent) -> PredictionResultEvent | _Failure:
        async with self._semaphore:
            started = time.perf_counter()
            try:
                return await self._predict_with_retries(event)
            finally:
                self._metrics.event_duration.observe(time.perf_counter() - started)

    async def _predict_with_retries(
        self, event: PredictionEvent
    ) -> PredictionResultEvent | _Failure:
        attempt = 0
        while True:
            attempt += 1
            try:
                outcome = await self._predictions.predict(event.features, source="consumer")
            except AppError as exc:
                if not is_retryable(exc) or attempt >= self._max_attempts:
                    return _Failure(exc.code, exc.message, attempt)
                self._metrics.retries.inc()
                await self._sleep(self._backoff * 2 ** (attempt - 1))
                continue
            except Exception as exc:
                logger.exception(
                    "Unexpected error processing event", extra={"event_id": event.event_id}
                )
                return _Failure("internal_error", type(exc).__name__, attempt)
            return PredictionResultEvent(
                event_id=event.event_id,
                request_id=event.request_id,
                model_name=self._predictions.model_name,
                model_version=self._predictions.model_version,
                label=outcome.result.label,
                score=outcome.result.score,
                cache=outcome.cache,
                latency_ms=outcome.latency_ms,
                created_at=event.created_at,
                processed_at=datetime.now(UTC),
                metadata=event.metadata,
            )

    def _dead_letter(self, outcome: BatchOutcome, record: InboundRecord, failure: _Failure) -> None:
        """Forward the original bytes untouched, with failure context in headers for replay."""
        headers: Headers = [
            ("dlq.error_code", failure.code.encode()),
            ("dlq.error_message", failure.message.encode()),
            ("dlq.source_topic", record.topic.encode()),
            ("dlq.source_partition", str(record.partition).encode()),
            ("dlq.source_offset", str(record.offset).encode()),
            ("dlq.attempts", str(failure.attempts).encode()),
            ("dlq.failed_at", datetime.now(UTC).isoformat().encode()),
        ]
        outcome.messages.append(
            OutboundMessage(
                topic=self._dead_letter_topic,
                key=record.key,
                value=record.value or b"",
                headers=headers,
            )
        )
        outcome.dead_lettered += 1
        self._metrics.events_processed.labels("dead_lettered").inc()
        self._metrics.dead_letters.labels(failure.code).inc()
        logger.warning(
            "Event dead-lettered",
            extra={
                "error_code": failure.code,
                "partition": record.partition,
                "offset": record.offset,
                "attempts": failure.attempts,
            },
        )
