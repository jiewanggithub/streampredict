"""Standalone demo orchestrator: one load generator per cluster, driving the gateway over HTTP.

    uvicorn --factory streampredict_api.demo_service:app_factory

Gateway replicas proxy their demo endpoints here (DEMO_ORCHESTRATOR_URL), so there is exactly one
session state and the "one session at a time" limit holds cluster-wide. Lag and consumer replica
counts for the session summary are read from the gateway's overview.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx
from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import Settings, get_settings
from .demo import DemoOrchestrator
from .errors import register_error_handlers
from .logs import configure_logging
from .metrics import DemoMetrics
from .schemas import DemoStartRequest, DemoStatus
from .traffic import HttpSink

logger = logging.getLogger("streampredict.demo")


class PipelineProbe:
    """Polls the gateway overview for consumer lag and consumer replica count."""

    def __init__(self, api_url: str, interval_seconds: float = 1.0) -> None:
        self._client = httpx.AsyncClient(base_url=api_url.rstrip("/"), timeout=3)
        self._interval = interval_seconds
        self.lag: int | None = None
        self.replicas: int | None = None
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="pipeline-probe")

    async def _loop(self) -> None:
        while True:
            try:
                overview: dict[str, Any] = (
                    await self._client.get("/api/v1/metrics/overview")
                ).json()
                self.lag = (overview.get("kafka") or {}).get("lag")
                self.replicas = (overview.get("infrastructure") or {}).get("consumer_replicas")
            except Exception:
                self.lag = self.replicas = None
            await asyncio.sleep(self._interval)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._client.aclose()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        metrics = DemoMetrics()
        sink = HttpSink(settings.demo_target_url)
        probe = PipelineProbe(settings.demo_target_url)
        probe.start()
        orchestrator = DemoOrchestrator(
            sink,
            metrics,
            max_rps=settings.demo_max_rps,
            max_duration_seconds=settings.demo_max_duration_seconds,
            lag_probe=lambda: probe.lag,
            replicas_probe=lambda: probe.replicas,
            recovery_timeout_seconds=settings.demo_recovery_timeout_seconds,
        )
        app.state.orchestrator = orchestrator
        app.state.metrics = metrics
        try:
            yield
        finally:
            await orchestrator.shutdown()
            await probe.stop()
            await sink.close()

    app = FastAPI(title="StreamPredict Demo Orchestrator", version="0.1.0", lifespan=lifespan)
    register_error_handlers(app)

    def orchestrator(request: Request) -> DemoOrchestrator:
        return cast(DemoOrchestrator, request.app.state.orchestrator)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/demo/sessions", response_model=DemoStatus, status_code=202)
    async def start(request: Request, body: DemoStartRequest) -> DemoStatus:
        return await orchestrator(request).start(body)

    @app.post("/demo/stop", response_model=DemoStatus)
    async def stop(request: Request) -> DemoStatus:
        return await orchestrator(request).stop()

    @app.get("/demo/status", response_model=DemoStatus)
    async def status(request: Request) -> DemoStatus:
        return orchestrator(request).status()

    @app.get("/metrics", include_in_schema=False)
    async def metrics(request: Request) -> Response:
        registry = cast(DemoMetrics, request.app.state.metrics).registry
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app


def app_factory() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)
    return create_app(settings)
