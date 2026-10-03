"""Consumer settings: the shared gateway settings plus worker-specific knobs."""

from functools import lru_cache

from pydantic import Field

from streampredict_api.config import Settings


class ConsumerSettings(Settings):
    consumer_batch_size: int = Field(default=200, ge=1, le=5_000)
    consumer_poll_timeout_ms: int = Field(default=1_000, ge=10, le=30_000)
    # Concurrent predictions per process; scale throughput further by adding replicas.
    consumer_concurrency: int = Field(default=32, ge=1, le=1_000)
    consumer_max_attempts: int = Field(default=3, ge=1, le=10)
    consumer_retry_backoff_seconds: float = Field(default=0.2, ge=0, le=30)
    consumer_idempotency_ttl_seconds: int = Field(default=86_400, ge=60, le=7 * 86_400)
    consumer_metrics_port: int = Field(default=9102, ge=1, le=65_535)


@lru_cache
def get_consumer_settings() -> ConsumerSettings:
    return ConsumerSettings()
