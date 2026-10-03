"""HTTP server implementing the Open Inference Protocol (KServe v2) subset the platform uses.

    GET  /v2/health/live | /v2/health/ready
    GET  /v2/models/{model}[/versions/{version}]          metadata
    GET  /v2/models/{model}[/versions/{version}]/ready
    POST /v2/models/{model}[/versions/{version}]/infer
    POST /v2/repository/index                             (Triton repository extension)
    POST /v2/repository/models/{model}/load
    GET  /metrics

Requests without a version go to the model's default version (`serving.json`, written by the
deployment controller), or the highest loaded version when none is set. Errors use the protocol's
`{"error": "..."}` body.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, cast

import numpy as np
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field

from .config import ServingSettings, get_settings
from .logs import configure_logging
from .metrics import ServingMetrics
from .repository import ModelConfig, RepositoryError, discover
from .runtime import InferenceFailed, LoadedVersion

logger = logging.getLogger("streampredict.serving")


class ServingError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class ModelManager:
    """Loads model versions from the repository and tracks which ones are serving."""

    def __init__(self, settings: ServingSettings, metrics: ServingMetrics) -> None:
        self._settings = settings
        self._metrics = metrics
        self._models: dict[str, dict[str, LoadedVersion]] = {}
        self._configs: dict[str, ModelConfig] = {}
        self._defaults: dict[str, str | None] = {}
        self._lock = asyncio.Lock()

    async def load(self, only: str | None = None) -> dict[str, list[str]]:
        """(Re)scan the repository: load new versions and unload removed ones.

        Versions already loaded keep serving untouched, so publishing a new version never
        interrupts traffic to the current one.
        """
        async with self._lock:
            try:
                sources = discover(self._settings.model_repository)
            except (RepositoryError, ValueError) as exc:
                raise ServingError(500, f"cannot read model repository: {exc}") from exc
            if only is not None:
                if only not in sources:
                    raise ServingError(404, f"model {only!r} not found in repository")
                sources = {only: sources[only]}
            for name, source in sources.items():
                versions = source.versions
                if self._settings.version_policy == "latest":
                    versions = versions[-1:]
                wanted = {v.version: v for v in versions}
                loaded = self._models.setdefault(name, {})
                self._configs[name] = source.config
                self._defaults[name] = source.default_version
                for version, version_source in wanted.items():
                    if version in loaded:
                        continue
                    instance = LoadedVersion(
                        source.config,
                        version_source,
                        self._metrics,
                        max_batch_size=self._settings.max_batch_size
                        or source.config.max_batch_size,
                        max_queue_delay_seconds=(
                            self._settings.max_queue_delay_ms / 1000
                            if self._settings.max_queue_delay_ms is not None
                            else source.config.dynamic_batching.max_queue_delay_microseconds / 1e6
                        ),
                        intra_op_threads=self._settings.intra_op_threads,
                    )
                    await instance.start()
                    loaded[version] = instance
                for version in [v for v in loaded if v not in wanted]:
                    await loaded.pop(version).stop()
                    logger.info("Model version unloaded", extra={"model": name, "version": version})
            return self.index()

    def index(self) -> dict[str, list[str]]:
        return {name: sorted(v, key=int) for name, v in self._models.items() if v}

    def resolve(self, model: str, version: str | None) -> LoadedVersion:
        versions = self._models.get(model)
        if not versions:
            raise ServingError(404, f"model {model!r} is not loaded")
        if version is None:
            default = self._defaults.get(model)
            if default is not None and default in versions:
                return versions[default]
            return versions[max(versions, key=int)]
        if version not in versions:
            raise ServingError(404, f"version {version!r} of model {model!r} is not loaded")
        return versions[version]

    def config(self, model: str) -> ModelConfig:
        if model not in self._configs:
            raise ServingError(404, f"model {model!r} is not loaded")
        return self._configs[model]

    @property
    def ready(self) -> bool:
        return any(v.ready for versions in self._models.values() for v in versions.values())

    async def close(self) -> None:
        for versions in self._models.values():
            for instance in versions.values():
                await instance.stop()


# --- protocol schemas -------------------------------------------------------------------------


class InferInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    shape: list[int] = Field(min_length=1, max_length=2)
    datatype: str
    data: list[float] | list[list[float]]


class InferRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str | None = None
    inputs: list[InferInput] = Field(min_length=1, max_length=1)


class InferOutput(BaseModel):
    name: str
    shape: list[int]
    datatype: str
    data: list[float]


class InferResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    model_name: str
    model_version: str
    id: str | None
    outputs: list[InferOutput]


def to_batch(request: InferRequest, config: ModelConfig, max_rows: int) -> np.ndarray:
    tensor = request.inputs[0]
    spec = config.inputs[0]
    if tensor.name != spec.name:
        raise ServingError(400, f"unexpected input {tensor.name!r}; expected {spec.name!r}")
    if tensor.datatype != spec.datatype:
        raise ServingError(400, f"input datatype must be {spec.datatype}")
    features = spec.shape[-1]
    shape = tensor.shape if len(tensor.shape) == 2 else [1, tensor.shape[0]]
    if shape[1] != features:
        raise ServingError(400, f"input shape must be [N, {features}]")
    if not 1 <= shape[0] <= max_rows:
        raise ServingError(400, f"batch must have between 1 and {max_rows} rows")
    try:
        array = np.asarray(tensor.data, dtype=np.float32).reshape(shape)
    except ValueError as exc:
        raise ServingError(400, "data does not match the declared shape") from exc
    if not np.isfinite(array).all():
        raise ServingError(400, "input contains NaN or infinite values")
    return array


# --- application ------------------------------------------------------------------------------


def create_app(settings: ServingSettings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        metrics = ServingMetrics()
        manager = ModelManager(settings, metrics)
        app.state.metrics = metrics
        app.state.manager = manager
        index = await manager.load()
        logger.info("Model repository loaded", extra={"models": index})
        try:
            yield
        finally:
            await manager.close()

    app = FastAPI(
        title="StreamPredict Model Serving",
        version="0.1.0",
        description="ONNX Runtime inference over the Open Inference Protocol (KServe v2).",
        lifespan=lifespan,
    )

    def manager_dep(request: Request) -> ModelManager:
        return cast(ModelManager, request.app.state.manager)

    Manager = Annotated[ModelManager, Depends(manager_dep)]

    @app.exception_handler(ServingError)
    async def serving_error(_: Request, exc: ServingError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"error": exc.message})

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        fields = sorted({".".join(str(p) for p in e["loc"][1:]) or "body" for e in exc.errors()})
        return JSONResponse(
            status_code=400, content={"error": f"invalid request: {', '.join(fields)}"}
        )

    @app.get("/v2/health/live")
    async def live() -> dict[str, bool]:
        return {"live": True}

    @app.get("/v2/health/ready")
    async def ready(manager: Manager, response: Response) -> dict[str, bool]:
        if not manager.ready:
            response.status_code = 503
        return {"ready": manager.ready}

    @app.get("/v2/models/{model}")
    @app.get("/v2/models/{model}/versions/{version}")
    async def metadata(model: str, manager: Manager, version: str | None = None) -> dict[str, Any]:
        config = manager.config(model)
        instance = manager.resolve(model, version)
        return {
            "name": model,
            "versions": manager.index().get(model, []),
            "platform": config.platform,
            "inputs": [spec.model_dump() for spec in config.inputs],
            "outputs": [spec.model_dump() for spec in config.outputs],
            "parameters": {
                **config.parameters,
                "served_version": instance.version,
                "training": instance.metadata,
            },
        }

    @app.get("/v2/models/{model}/ready")
    @app.get("/v2/models/{model}/versions/{version}/ready")
    async def model_ready(
        model: str, manager: Manager, response: Response, version: str | None = None
    ) -> dict[str, Any]:
        try:
            ok = manager.resolve(model, version).ready
        except ServingError:
            ok = False
        if not ok:
            response.status_code = 503
        return {"name": model, "ready": ok}

    @app.post("/v2/models/{model}/infer", response_model=InferResponse)
    @app.post("/v2/models/{model}/versions/{version}/infer", response_model=InferResponse)
    async def infer(
        model: str,
        body: InferRequest,
        manager: Manager,
        request: Request,
        version: str | None = None,
    ) -> InferResponse:
        metrics: ServingMetrics = request.app.state.metrics
        instance = manager.resolve(model, version)
        labels = (model, instance.version)
        started = time.perf_counter()
        try:
            batch = to_batch(body, instance.config, settings.max_request_rows)
            output = await instance.infer(batch)
        except ServingError:
            metrics.requests.labels(*labels, "invalid").inc()
            raise
        except InferenceFailed as exc:
            metrics.requests.labels(*labels, "error").inc()
            raise ServingError(503, f"inference failed: {exc}") from exc
        metrics.requests.labels(*labels, "success").inc()
        for score in output.ravel():
            metrics.output_score.labels(*labels).observe(float(score))
        metrics.request_duration.labels(*labels).observe(time.perf_counter() - started)
        spec = instance.config.outputs[0]
        return InferResponse(
            model_name=model,
            model_version=instance.version,
            id=body.id,
            outputs=[
                InferOutput(
                    name=spec.name,
                    shape=list(output.shape),
                    datatype=spec.datatype,
                    data=output.ravel().tolist(),
                )
            ],
        )

    @app.post("/v2/repository/index")
    async def repository_index(manager: Manager) -> list[dict[str, str]]:
        return [
            {"name": name, "version": version, "state": "READY"}
            for name, versions in manager.index().items()
            for version in versions
        ]

    @app.post("/v2/repository/models/{model}/load")
    async def repository_load(model: str, manager: Manager) -> dict[str, list[str]]:
        """Pick up versions added to (or removed from) the repository without a restart."""
        return await manager.load(only=model)

    @app.get("/metrics", include_in_schema=False)
    async def prometheus(request: Request) -> Response:
        registry = request.app.state.metrics.registry
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory streampredict_serving.server:app_factory`."""
    configure_logging(get_settings().log_level)
    return create_app()
