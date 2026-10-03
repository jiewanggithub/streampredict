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
from .config import Settings, get_settings
from .demo import DemoOrchestrator
from .errors import AppError, register_error_handlers
from .inference import InferenceClient, MockInferenceClient
from .logs import configure_logging, request_id_var
from .metrics import ApiMetrics, RollingWindow
from .prediction import PredictionService
from .schemas import (
    CacheMetrics,
    DemoStartRequest,
    DemoStatus,
    DependencyStatus,
    ErrorResponse,
    HealthResponse,
    MetricsOverview,
    ModelInfo,
    PredictRequest,
    PredictResponse,
    ReadyResponse,
    TrafficMetrics,
)

logger = logging.getLogger("streampredict.api")

REQUEST_ID_HEADER = "X-Request-ID"
VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")

RedisFactory = Callable[[Settings], Redis]
InferenceFactory = Callable[[Settings], InferenceClient]


@dataclass
class Container:
    settings: Settings
    metrics: ApiMetrics
    window: RollingWindow
    cache: PredictionCache
    predictions: PredictionService
    demo: DemoOrchestrator


def _default_redis(settings: Settings) -> Redis:
    return build_redis(settings.redis_url, settings.redis_timeout_seconds)


def _default_inference(settings: Settings) -> InferenceClient:
    return MockInferenceClient(
        settings.model_name,
        settings.model_version,
        latency_seconds=settings.mock_inference_latency_ms / 1000,
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
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        metrics = ApiMetrics()
        window = RollingWindow()
        redis_client = redis_factory(settings)
        inference = inference_factory(settings)
        cache = PredictionCache(
            redis_client,
            ttl_seconds=settings.redis_cache_ttl_seconds,
            timeout_seconds=settings.redis_timeout_seconds,
            circuit_open_seconds=settings.redis_circuit_open_seconds,
            metrics=metrics,
            window=window,
        )
        predictions = PredictionService(
            cache,
            inference,
            metrics,
            window,
            inference_timeout_seconds=settings.inference_timeout_seconds,
            max_in_flight=settings.api_max_in_flight_predictions,
        )
        demo = DemoOrchestrator(
            predictions,
            metrics,
            max_rps=settings.demo_max_rps,
            max_duration_seconds=settings.demo_max_duration_seconds,
        )
        metrics.model_info.labels(inference.model_name, inference.model_version).set(1)
        app.state.container = Container(settings, metrics, window, cache, predictions, demo)
        logger.info(
            "API gateway started",
            extra={"model_name": inference.model_name, "model_version": inference.model_version},
        )
        try:
            yield
        finally:
            await demo.shutdown()
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
        """Readiness. Redis loss only degrades caching, so it does not fail readiness."""
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

    demo_guard = [Depends(require_demo_token)]

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
        return Response(generate_latest(container.metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return app


def _ms(value: float | None) -> float | None:
    return None if value is None else round(value, 2)


async def _build_overview(container: Container) -> MetricsOverview:
    snapshot = container.window.snapshot()
    return MetricsOverview(
        generated_at=datetime.now(UTC),
        window_seconds=container.window.window_seconds,
        model=ModelInfo(
            name=container.predictions.model_name,
            version=container.predictions.model_version,
        ),
        traffic=TrafficMetrics(
            rps=snapshot.rps,
            rps_history=snapshot.rps_history,
            p50_ms=_ms(snapshot.p50_ms),
            p95_ms=_ms(snapshot.p95_ms),
            p99_ms=_ms(snapshot.p99_ms),
            success_rate=snapshot.success_rate,
            requests_in_window=snapshot.requests_in_window,
            errors_in_window=snapshot.errors_in_window,
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
    )


async def _dependency_checks(container: Container) -> dict[str, DependencyStatus]:
    redis_ok = await container.cache.ping()
    inference_ok = await container.predictions.ready()
    return {
        "redis": "ok" if redis_ok else "unavailable",
        "inference": "ok" if inference_ok else "unavailable",
    }


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory streampredict_api.main:app_factory`."""
    return create_app()
