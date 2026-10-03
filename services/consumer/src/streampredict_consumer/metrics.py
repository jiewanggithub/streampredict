"""Prometheus collectors for consumer workers."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from streampredict_api.metrics import LATENCY_BUCKETS, PredictionMetrics

BATCH_BUCKETS = (1, 5, 10, 25, 50, 100, 200, 500, 1000)


class ConsumerMetrics(PredictionMetrics):
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        super().__init__(registry)
        self.events_processed = Counter(
            "streampredict_consumer_events_total",
            "Prediction events handled, by outcome (success, duplicate, dead_lettered).",
            ["outcome"],
            registry=self.registry,
        )
        self.event_duration = Histogram(
            "streampredict_consumer_event_duration_seconds",
            "Time to process one event, including retries.",
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.event_age = Histogram(
            "streampredict_consumer_event_age_seconds",
            "Time from event creation to processing completion (queueing + processing).",
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
            registry=self.registry,
        )
        self.retries = Counter(
            "streampredict_consumer_retries_total",
            "Retry attempts after retryable processing failures.",
            registry=self.registry,
        )
        self.dead_letters = Counter(
            "streampredict_consumer_dead_letters_total",
            "Events routed to the dead-letter topic, by error code.",
            ["reason"],
            registry=self.registry,
        )
        self.batch_size = Histogram(
            "streampredict_consumer_batch_size",
            "Records returned per poll.",
            buckets=BATCH_BUCKETS,
            registry=self.registry,
        )
        self.batch_failures = Counter(
            "streampredict_consumer_batch_failures_total",
            "Batches that could not be published or committed and were re-polled.",
            registry=self.registry,
        )
        self.lag = Gauge(
            "streampredict_consumer_partition_lag",
            "Records between the partition high watermark and this worker's position.",
            ["partition"],
            registry=self.registry,
        )
        self.assigned_partitions = Gauge(
            "streampredict_consumer_assigned_partitions",
            "Partitions currently assigned to this worker.",
            registry=self.registry,
        )
        self.idempotency_errors = Counter(
            "streampredict_consumer_idempotency_errors_total",
            "Idempotency store operations that failed open.",
            ["operation"],
            registry=self.registry,
        )
