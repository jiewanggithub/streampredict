"""Prometheus collectors for the model-serving service.

Labels are limited to model name, version, and fixed outcomes, so cardinality is bounded by the
number of loaded versions.
"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

LATENCY_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1)
BATCH_BUCKETS = (1, 2, 4, 8, 16, 32, 64, 128, 256)


class ServingMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.requests = Counter(
            "streampredict_serving_requests_total",
            "Inference requests, by model, version, and outcome.",
            ["model", "version", "outcome"],
            registry=self.registry,
        )
        self.request_duration = Histogram(
            "streampredict_serving_request_duration_seconds",
            "Time from request arrival to response, including queueing.",
            ["model", "version"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.queue_duration = Histogram(
            "streampredict_serving_queue_duration_seconds",
            "Time a request waited for its batch to start.",
            ["model", "version"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.compute_duration = Histogram(
            "streampredict_serving_compute_duration_seconds",
            "ONNX Runtime execution time per batch.",
            ["model", "version"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.batch_size = Histogram(
            "streampredict_serving_batch_rows",
            "Rows per executed batch (dynamic batching effectiveness).",
            ["model", "version"],
            buckets=BATCH_BUCKETS,
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "streampredict_serving_queue_depth",
            "Requests waiting to be batched.",
            ["model", "version"],
            registry=self.registry,
        )
        # Output distribution per version: the signal that exposes training/serving skew.
        self.output_score = Histogram(
            "streampredict_serving_output_score",
            "Model output (probability) distribution.",
            ["model", "version"],
            buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
            registry=self.registry,
        )
        self.version_ready = Gauge(
            "streampredict_serving_model_ready",
            "1 when a model version is loaded and warmed up.",
            ["model", "version"],
            registry=self.registry,
        )
