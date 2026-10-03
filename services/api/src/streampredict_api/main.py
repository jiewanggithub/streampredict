"""FastAPI application factory for the StreamPredict API gateway."""

import asyncio
import hmac
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, cast

from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from .cache import PredictionCache, build_redis
from .cluster_traffic import ClusterTraffic
from .config import Settings, get_settings
from .demo import DemoControl, DemoOrchestrator, DemoProxy
from .deployment import DeploymentMonitor
from .errors import AppError, register_error_handlers
from .events import (
    DisabledEventPublisher,
    EventMetadata,
    EventPublisher,
    KafkaEventPublisher,
    new_event,
)
from .inference import InferenceClient, build_inference
from .kafka_monitor import KafkaMonitor, KafkaOffsetSource
from .kubernetes_monitor import KubernetesMonitor
from .logs import configure_logging, request_id_var
from .metrics import ApiMetrics, RollingWindow
from .prediction import PredictionService
from .prometheus_monitor import PrometheusMonitor
from .schemas import (
    CacheMetrics,
    DemoStartRequest,
    DemoStatus,
    DependencyStatus,
    ErrorResponse,
    EventAccepted,
    EventRequest,
    HealthResponse,
    InfrastructureMetrics,
    MetricsOverview,
    ModelInfo,
    PredictRequest,
    PredictResponse,
    ReadyResponse,
    ReleaseRequest,
    TrafficMetrics,
)
from .traffic import InProcessSink

logger = logging.getLogger("streampredict.api")

REQUEST_ID_HEADER = "X-Request-ID"
VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")

RedisFactory = Callable[[Settings], Redis]
InferenceFactory = Callable[[Settings], InferenceClient]
KafkaFactory = Callable[[Settings, ApiMetrics], tuple[EventPublisher, KafkaMonitor | None]]
DeploymentFactory = Callable[[Settings], DeploymentMonitor | None]


@dataclass
class Container:
    settings: Settings
    metrics: ApiMetrics
    window: RollingWindow
    cache: PredictionCache
    predictions: PredictionService
    demo: DemoControl
    publisher: EventPublisher
    kafka_monitor: KafkaMonitor | None
    deployment: DeploymentMonitor | None
    cluster_traffic: ClusterTraffic | None = None
    prometheus: PrometheusMonitor | None = None
    kubernetes: KubernetesMonitor | None = None


def _default_redis(settings: Settings) -> Redis:
    return build_redis(settings.redis_url, settings.redis_timeout_seconds)


def _default_inference(settings: Settings) -> InferenceClient:
    return build_inference(settings)


def _default_kafka(
    settings: Settings, metrics: ApiMetrics
) -> tuple[EventPublisher, KafkaMonitor | None]:
    if not settings.kafka_bootstrap_servers:
        return DisabledEventPublisher(), None
    publisher = KafkaEventPublisher(
        settings.kafka_bootstrap_servers,
        settings.kafka_prediction_topic,
        metrics,
        timeout_seconds=settings.kafka_publish_timeout_seconds,
        reconnect_interval_seconds=settings.kafka_reconnect_interval_seconds,
    )
    source = KafkaOffsetSource(
        settings.kafka_bootstrap_servers,
        settings.kafka_prediction_topic,
        settings.kafka_consumer_group,
        timeout_seconds=settings.kafka_publish_timeout_seconds,
    )
    monitor = KafkaMonitor(
        source,
        metrics,
        topic=settings.kafka_prediction_topic,
        consumer_group=settings.kafka_consumer_group,
        timeout_seconds=settings.kafka_publish_timeout_seconds,
    )
    return publisher, monitor


def _default_deployment(settings: Settings) -> DeploymentMonitor | None:
    if not settings.controller_url:
        return None
    return DeploymentMonitor(settings.controller_url, settings.model_name)


def _local_demo(
    settings: Settings,
    predictions: PredictionService,
    publisher: EventPublisher,
    metrics: ApiMetrics,
    kafka_monitor: KafkaMonitor | None,
) -> DemoOrchestrator:
    """In-process orchestrator for single-replica deployments (Docker Compose)."""
    return DemoOrchestrator(
        InProcessSink(predictions, publisher),
        metrics,
        max_rps=settings.demo_max_rps,
        max_duration_seconds=settings.demo_max_duration_seconds,
        lag_probe=kafka_monitor.current_lag if kafka_monitor else None,
        replicas_probe=kafka_monitor.current_replicas if kafka_monitor else None,
        recovery_timeout_seconds=settings.demo_recovery_timeout_seconds,
    )


def get_container(request: Request) -> Container:
    return cast(Container, request.app.state.container)


ContainerDep = Annotated[Container, Depends(get_container)]


def require_demo_token(
    container: ContainerDep,
    x_demo_token: Annotated[str | None, Header()] = None,
) -> None:
    """Protect demo controls when DEMO_CONTROL_TOKEN is configured."""
    expected = container.settings.demo_control_token
    if expected is None:
        return
    if x_demo_token is None or not hmac.compare_digest(
        x_demo_token.encode(), expected.get_secret_value().encode()
    ):
        raise AppError(401, "unauthorized", "A valid X-Demo-Token header is required.")


ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    422: {"model": ErrorResponse},
    503: {"model": ErrorResponse},
}


def create_app(
    settings: Settings | None = None,
    *,
    redis_factory: RedisFactory = _default_redis,
    inference_factory: InferenceFactory = _default_inference,
    kafka_factory: KafkaFactory = _default_kafka,
    deployment_factory: DeploymentFactory = _default_deployment,
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        metrics = ApiMetrics()
        window = RollingWindow()
        redis_client = redis_factory(settings)
        inference = inference_factory(settings)
        await inference.start()
        cache = PredictionCache(
            redis_client,
            ttl_seconds=settings.redis_cache_ttl_seconds,
            timeout_seconds=settings.redis_timeout_seconds,
            circuit_open_seconds=settings.redis_circuit_open_seconds,
            metrics=metrics,
            window=window,
        )
        cluster_traffic = ClusterTraffic(redis_client)
        predictions = PredictionService(
            cache,
            inference,
            metrics,
            window,
            inference_timeout_seconds=settings.inference_timeout_seconds,
            max_in_flight=settings.api_max_in_flight_predictions,
            cluster_traffic=cluster_traffic,
        )
        publisher, kafka_monitor = kafka_factory(settings, metrics)
        demo: DemoControl
        if settings.demo_orchestrator_url:
            proxy = DemoProxy(
                settings.demo_orchestrator_url,
                max_rps=settings.demo_max_rps,
                max_duration_seconds=settings.demo_max_duration_seconds,
            )
            proxy.start_polling()
            demo = proxy
        else:
            demo = _local_demo(settings, predictions, publisher, metrics, kafka_monitor)
        metrics.model_info.labels(inference.model_name, inference.model_version).set(1)
        deployment = deployment_factory(settings)
        app.state.container = Container(
            settings,
            metrics,
            window,
            cache,
            predictions,
            demo,
            publisher,
            kafka_monitor,
            deployment,
        )
        # Connect in the background; Kafka being down must not block or fail startup.
        await publisher.ready()
        if kafka_monitor is not None:
            kafka_monitor.start()
        if deployment is not None:
            deployment.start()
        app.state.container.cluster_traffic = cluster_traffic
        cluster_traffic.start()
        if settings.prometheus_url:
            app.state.container.prometheus = PrometheusMonitor(settings.prometheus_url)
            app.state.container.prometheus.start()
        if settings.kubernetes_namespace:
            app.state.container.kubernetes = KubernetesMonitor(settings.kubernetes_namespace)
            app.state.container.kubernetes.start()
        logger.info(
            "API gateway started",
            extra={"model_name": inference.model_name, "model_version": inference.model_version},
        )
        try:
            yield
        finally:
            await demo.shutdown()
            await cluster_traffic.stop()
            if app.state.container.prometheus is not None:
                await app.state.container.prometheus.stop()
            if kafka_monitor is not None:
                await kafka_monitor.stop()
            if deployment is not None:
                await deployment.stop()
            if app.state.container.kubernetes is not None:
                await app.state.container.kubernetes.stop()
            await publisher.close()
            await inference.close()
            await redis_client.aclose()
            logger.info("API gateway stopped")

    app = FastAPI(
        title="StreamPredict API Gateway",
        version="0.1.0",
        description="Synchronous predictions, demo control, and aggregated metrics.",
        lifespan=lifespan,
    )
    register_error_handlers(app)

    @app.middleware("http")
    async def request_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        incoming = request.headers.get(REQUEST_ID_HEADER, "")
        request_id = incoming if VALID_REQUEST_ID.match(incoming) else str(uuid.uuid4())
        token = request_id_var.set(request_id)
        container: Container | None = getattr(request.app.state, "container", None)
        started = time.perf_counter()
        status = 500
        if container:
            container.metrics.http_in_flight.inc()
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            elapsed = time.perf_counter() - started
            route = getattr(request.scope.get("route"), "path", "unmatched")
            if container:
                container.metrics.http_in_flight.dec()
                container.metrics.http_requests.labels(request.method, route, str(status)).inc()
                container.metrics.http_duration.labels(request.method, route).observe(elapsed)
            if route not in {"/metrics", "/health", "/ready", "/api/v1/metrics/overview"}:
                logger.info(
                    "request completed",
                    extra={
                        "method": request.method,
                        "route": route,
                        "status": status,
                        "duration_ms": round(elapsed * 1000, 2),
                    },
                )
            request_id_var.reset(token)

    # Added after the request middleware so CORS headers are applied to every response.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", REQUEST_ID_HEADER, "X-Demo-Token"],
        expose_headers=[REQUEST_ID_HEADER],
    )

    @app.get("/health", response_model=HealthResponse, tags=["health"])
    async def health() -> HealthResponse:
        """Liveness: the process is up and serving requests."""
        return HealthResponse(status="ok")

    @app.get(
        "/ready",
        response_model=ReadyResponse,
        responses={503: {"model": ReadyResponse}},
        tags=["health"],
    )
    async def ready(container: ContainerDep, response: Response) -> ReadyResponse:
        """Readiness. Redis and Kafka losses degrade caching and async events but do not fail it."""
        checks = await _dependency_checks(container)
        if checks["inference"] != "ok":
            response.status_code = 503
            return ReadyResponse(status="not_ready", checks=checks)
        status = "ready" if all(value == "ok" for value in checks.values()) else "degraded"
        return ReadyResponse(status=status, checks=checks)

    @app.post(
        "/api/v1/predict",
        response_model=PredictResponse,
        responses=ERROR_RESPONSES,
        tags=["predictions"],
    )
    async def predict(body: PredictRequest, container: ContainerDep) -> PredictResponse:
        outcome = await container.predictions.predict(body.features, source="api")
        return PredictResponse(
            request_id=request_id_var.get() or "",
            model_name=container.predictions.model_name,
            model_version=container.predictions.model_version,
            label=outcome.result.label,
            score=outcome.result.score,
            cache=outcome.cache,
            latency_ms=outcome.latency_ms,
        )

    @app.post(
        "/api/v1/events",
        response_model=EventAccepted,
        status_code=202,
        responses=ERROR_RESPONSES,
        tags=["predictions"],
    )
    async def publish_event(body: EventRequest, container: ContainerDep) -> EventAccepted:
        """Publish an asynchronous prediction event; the result lands on `prediction-results`."""
        request_id = request_id_var.get() or str(uuid.uuid4())
        event = new_event(
            body.features,
            request_id=request_id,
            model_name=container.predictions.model_name,
            model_version=container.predictions.model_version,
            metadata=EventMetadata(**body.metadata.model_dump()) if body.metadata else None,
        )
        receipt = await container.publisher.publish(event)
        return EventAccepted(
            event_id=event.event_id,
            request_id=request_id,
            topic=receipt.topic,
            partition=receipt.partition,
            offset=receipt.offset,
        )

    demo_guard = [Depends(require_demo_token)]

    def deployment_of(container: Container) -> DeploymentMonitor:
        if container.deployment is None:
            raise AppError(503, "controller_disabled", "No deployment controller is configured.")
        return container.deployment

    @app.post(
        "/api/v1/models/releases",
        status_code=202,
        dependencies=demo_guard,
        responses={401: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
        tags=["models"],
    )
    async def release_model(body: ReleaseRequest, container: ContainerDep) -> dict[str, Any]:
        """Start a gated release of a registered model version (controller-managed)."""
        return await deployment_of(container).release(body.version)

    @app.post(
        "/api/v1/models/rollback",
        status_code=202,
        dependencies=demo_guard,
        responses={401: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
        tags=["models"],
    )
    async def rollback_model(container: ContainerDep) -> dict[str, Any]:
        """Roll back to the previous stable champion."""
        return await deployment_of(container).rollback()

    @app.post(
        "/api/v1/demo/traffic-spike",
        response_model=DemoStatus,
        status_code=202,
        dependencies=demo_guard,
        responses={401: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
        tags=["demo"],
    )
    async def start_demo(
        container: ContainerDep, body: DemoStartRequest | None = None
    ) -> DemoStatus:
        """Start a bounded synthetic traffic session (`standard` ramp or `spike` profile)."""
        return await container.demo.start(body or DemoStartRequest())

    @app.post(
        "/api/v1/demo/stop",
        response_model=DemoStatus,
        dependencies=demo_guard,
        responses={401: {"model": ErrorResponse}},
        tags=["demo"],
    )
    async def stop_demo(container: ContainerDep) -> DemoStatus:
        return await container.demo.stop()

    @app.get("/api/v1/demo/status", response_model=DemoStatus, tags=["demo"])
    async def demo_status(container: ContainerDep) -> DemoStatus:
        return container.demo.status()

    @app.get("/api/v1/metrics/overview", response_model=MetricsOverview, tags=["metrics"])
    async def metrics_overview(container: ContainerDep) -> MetricsOverview:
        """Aggregates over the last 60 seconds for the dashboard (per API process)."""
        return await _build_overview(container)

    @app.get(
        "/api/v1/metrics/stream",
        response_class=StreamingResponse,
        responses={200: {"content": {"text/event-stream": {}}}},
        tags=["metrics"],
    )
    async def metrics_stream(request: Request, container: ContainerDep) -> StreamingResponse:
        """Server-Sent Events: one `overview` event per interval, same payload as the overview.

        The stream closes after METRICS_STREAM_MAX_SECONDS; browsers reconnect automatically.
        """
        interval = container.settings.metrics_stream_interval_seconds
        deadline = time.monotonic() + container.settings.metrics_stream_max_seconds

        async def events() -> AsyncIterator[str]:
            yield f"retry: {round(interval * 1000)}\n\n"
            while time.monotonic() < deadline and not await request.is_disconnected():
                overview = await _build_overview(container)
                yield f"event: overview\ndata: {overview.model_dump_json()}\n\n"
                await asyncio.sleep(interval)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics(container: ContainerDep) -> Response:
        in_use, idle = container.cache.connection_counts()
        container.metrics.redis_connections.labels("in_use").set(in_use)
        container.metrics.redis_connections.labels("idle").set(idle)
        # The served version can change at runtime (controller promotion or rollback).
        container.metrics.model_info.clear()
        container.metrics.model_info.labels(
            container.predictions.model_name, container.predictions.model_version
        ).set(1)
        return Response(generate_latest(container.metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return app


def _infrastructure(container: Container) -> InfrastructureMetrics | None:
    if container.kafka_monitor is None:
        return None
    infrastructure = container.kafka_monitor.infrastructure(container.settings.deployment_platform)
    if container.kubernetes is not None:
        infrastructure.kubernetes = container.kubernetes.overview()
    return infrastructure


def _ms(value: float | None) -> float | None:
    return None if value is None else round(value, 2)


async def _build_overview(container: Container) -> MetricsOverview:
    snapshot = container.window.snapshot()
    cluster = (
        await container.cluster_traffic.read() if container.cluster_traffic is not None else None
    )
    observability = container.prometheus.overview() if container.prometheus is not None else None
    # Prefer Prometheus's cluster-wide percentiles once it has data for the window.
    prom = observability if observability and observability.p95_ms is not None else None
    return MetricsOverview(
        generated_at=datetime.now(UTC),
        window_seconds=container.window.window_seconds,
        model=ModelInfo(
            name=container.predictions.model_name,
            version=container.predictions.model_version,
            backend=container.predictions.backend,
            predictions_in_window=snapshot.requests_in_window,
            error_rate=None
            if snapshot.success_rate is None
            else round(100 - snapshot.success_rate, 3),
            label_distribution=snapshot.label_counts,
        ),
        traffic=TrafficMetrics(
            scope="cluster" if cluster else "replica",
            rps=cluster.rps if cluster else snapshot.rps,
            rps_history=cluster.history if cluster else snapshot.rps_history,
            latency_scope="cluster" if prom else "replica",
            p50_ms=prom.p50_ms if prom else _ms(snapshot.p50_ms),
            p95_ms=prom.p95_ms if prom else _ms(snapshot.p95_ms),
            p99_ms=prom.p99_ms if prom else _ms(snapshot.p99_ms),
            success_rate=(
                round(100 * (cluster.requests - cluster.errors) / cluster.requests, 3)
                if cluster.requests
                else None
            )
            if cluster
            else snapshot.success_rate,
            requests_in_window=cluster.requests if cluster else snapshot.requests_in_window,
            errors_in_window=cluster.errors if cluster else snapshot.errors_in_window,
        ),
        cache=CacheMetrics(
            hit_rate=snapshot.hit_rate,
            miss_rate=snapshot.miss_rate,
            bypass_count=snapshot.bypass_count,
            lookup_p95_ms=_ms(snapshot.lookup_p95_ms),
            ttl_seconds=container.cache.ttl_seconds,
        ),
        dependencies=await _dependency_checks(container),
        demo=container.demo.status(),
        kafka=container.kafka_monitor.overview() if container.kafka_monitor else None,
        infrastructure=_infrastructure(container),
        deployment=container.deployment.overview() if container.deployment else None,
        observability=observability,
    )


async def _dependency_checks(container: Container) -> dict[str, DependencyStatus]:
    redis_ok = await container.cache.ping()
    inference_ok = await container.predictions.ready()
    checks: dict[str, DependencyStatus] = {
        "redis": "ok" if redis_ok else "unavailable",
        "inference": "ok" if inference_ok else "unavailable",
    }
    if container.settings.kafka_bootstrap_servers:
        checks["kafka"] = "ok" if await container.publisher.ready() else "unavailable"
    return checks


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory streampredict_api.main:app_factory`."""
    return create_app()
