"""HTTP API of the deployment controller.

GET  /health | /ready
GET  /models/{model}/deployment     champion, serving default, versions, active release, history
POST /models/{model}/releases       {"version": "3"} -> 202, runs the gated release
POST /models/{model}/rollback       -> 202, back to the previous stable champion
GET  /metrics
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Annotated, Any, cast

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest
from pydantic import BaseModel

from .config import ControllerSettings, get_settings
from .logs import configure_logging
from .pipeline import ReleaseConflict, ReleaseManager
from .registry import MlflowRegistry, Registry
from .serving import FileServingAdmin, ServingAdmin

logger = logging.getLogger("streampredict.controller")


class ReleaseRequest(BaseModel):
    version: str


def create_app(
    settings: ControllerSettings | None = None,
    *,
    registry: Registry | None = None,
    serving: ServingAdmin | None = None,
) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        admin = serving or FileServingAdmin(
            settings.deployed_repository, settings.model_name, settings.serving_url
        )
        manager = ReleaseManager(
            settings,
            registry or MlflowRegistry(settings.mlflow_tracking_uri, settings.model_name),
            admin,
        )
        app.state.manager = manager
        task = asyncio.create_task(_reconcile_until_ready(manager), name="reconcile")
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            with contextlib.suppress(Exception):
                await manager.wait()
            if isinstance(admin, FileServingAdmin):
                await admin.close()

    app = FastAPI(title="StreamPredict Deployment Controller", version="0.1.0", lifespan=lifespan)

    def manager_dep(request: Request) -> ReleaseManager:
        return cast(ReleaseManager, request.app.state.manager)

    Manager = Annotated[ReleaseManager, Depends(manager_dep)]

    def check_model(model: str) -> None:
        if model != settings.model_name:
            raise HTTPException(404, f"model {model!r} is not managed by this controller")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(manager: Manager, response: Response) -> dict[str, bool]:
        if not manager.reconciled:
            response.status_code = 503
        return {"ready": manager.reconciled}

    @app.get("/models/{model}/deployment")
    async def deployment(model: str, manager: Manager) -> dict[str, Any]:
        check_model(model)
        champion = await asyncio.to_thread(manager.registry.champion)
        versions = await asyncio.to_thread(manager.registry.versions)
        return {
            "model": model,
            "champion": champion,
            "serving_default": await manager.serving.default(),
            "installed": await manager.serving.installed(),
            "versions": [
                {
                    "version": v.version,
                    "profile": v.profile,
                    "description": v.description,
                    "status": v.status,
                    "auc": v.metrics.get("auc"),
                }
                for v in versions
            ],
            **manager.snapshot(),
        }

    @app.post("/models/{model}/releases", status_code=202)
    async def release(model: str, body: ReleaseRequest, manager: Manager) -> dict[str, Any]:
        check_model(model)
        if not manager.reconciled:
            raise HTTPException(503, "controller is not ready")
        # Cheap checks answer synchronously; the gated pipeline runs in the background.
        versions = {v.version for v in await asyncio.to_thread(manager.registry.versions)}
        if body.version not in versions:
            raise HTTPException(404, f"version {body.version} is not registered")
        if body.version == await asyncio.to_thread(manager.registry.champion):
            raise HTTPException(409, f"v{body.version} is already the champion")
        try:
            return asdict(manager.start_release(body.version))
        except ReleaseConflict as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/models/{model}/rollback", status_code=202)
    async def rollback(model: str, manager: Manager) -> dict[str, Any]:
        check_model(model)
        try:
            return asdict(manager.start_rollback())
        except ReleaseConflict as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/metrics", include_in_schema=False)
    async def metrics(manager: Manager) -> Response:
        registry = CollectorRegistry()
        outcomes = Gauge(
            "streampredict_controller_releases",
            "Releases in the recent history window, by outcome.",
            ["outcome"],
            registry=registry,
        )
        for record in manager.history:
            outcomes.labels(record.outcome).inc()
        champion = Gauge(
            "streampredict_controller_champion_version",
            "Version number currently promoted as champion.",
            registry=registry,
        )
        current = await asyncio.to_thread(manager.registry.champion)
        champion.set(int(current) if current and current.isdigit() else 0)
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app


async def _reconcile_until_ready(manager: ReleaseManager) -> None:
    delay = 1.0
    while not manager.reconciled:
        try:
            await manager.reconcile()
        except Exception as exc:
            logger.warning("Reconcile failed; retrying", extra={"error": repr(exc)})
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15.0)


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory streampredict_controller.api:app_factory`."""
    configure_logging(get_settings().log_level)
    return create_app()
