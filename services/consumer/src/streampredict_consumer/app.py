"""Consumer process assembly and entry point."""

import asyncio
import logging
import signal

from prometheus_client import start_http_server
from redis.asyncio import Redis

from streampredict_api.cache import PredictionCache, build_redis
from streampredict_api.inference import InferenceClient, build_inference
from streampredict_api.logs import configure_logging
from streampredict_api.prediction import PredictionService

from .config import ConsumerSettings, get_consumer_settings
from .idempotency import RedisIdempotencyStore
from .metrics import ConsumerMetrics
from .processor import EventProcessor
from .worker import KafkaWorker

logger = logging.getLogger("streampredict.consumer")


def build_worker(
    settings: ConsumerSettings,
    redis_client: Redis,
    metrics: ConsumerMetrics,
    inference: InferenceClient | None = None,
) -> KafkaWorker:
    cache = PredictionCache(
        redis_client,
        ttl_seconds=settings.redis_cache_ttl_seconds,
        timeout_seconds=settings.redis_timeout_seconds,
        circuit_open_seconds=settings.redis_circuit_open_seconds,
        metrics=metrics,
    )
    inference = inference or build_inference(settings)
    predictions = PredictionService(
        cache,
        inference,
        metrics,
        None,
        inference_timeout_seconds=settings.inference_timeout_seconds,
        max_in_flight=settings.consumer_concurrency,
    )
    idempotency = RedisIdempotencyStore(
        redis_client,
        ttl_seconds=settings.consumer_idempotency_ttl_seconds,
        timeout_seconds=max(settings.redis_timeout_seconds, 0.5),
        metrics=metrics,
    )
    processor = EventProcessor(
        predictions,
        idempotency,
        metrics,
        result_topic=settings.kafka_result_topic,
        dead_letter_topic=settings.kafka_dead_letter_topic,
        max_attempts=settings.consumer_max_attempts,
        retry_backoff_seconds=settings.consumer_retry_backoff_seconds,
        concurrency=settings.consumer_concurrency,
        simulated_work_seconds=settings.consumer_simulated_work_ms / 1000,
    )
    return KafkaWorker(settings, processor, idempotency, metrics)


async def run(settings: ConsumerSettings) -> None:
    metrics = ConsumerMetrics()
    start_http_server(settings.consumer_metrics_port, registry=metrics.registry)
    redis_client = build_redis(settings.redis_url, settings.redis_timeout_seconds)
    inference = build_inference(settings)
    await inference.start()
    worker = build_worker(settings, redis_client, metrics, inference)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Finish and commit the in-flight batch, then leave the group.
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await inference.close()
        await redis_client.aclose()


def main() -> None:
    settings = get_consumer_settings()
    configure_logging(settings.log_level)
    if not settings.kafka_bootstrap_servers:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS must be set for the consumer.")
    logger.info(
        "Starting consumer",
        extra={
            "model_version": settings.model_version,
            "metrics_port": settings.consumer_metrics_port,
        },
    )
    asyncio.run(run(settings))
