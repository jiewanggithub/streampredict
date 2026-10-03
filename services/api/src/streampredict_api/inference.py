"""Inference clients: the model-serving service (Open Inference Protocol) and a mock fallback.

The gateway owns business logic (risk thresholds, caching); the serving service only turns
features into a probability, so models can be swapped without touching the API.
"""

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from .config import Settings
from .errors import AppError
from .schemas import PredictionFeatures, RiskLabel

HIGH_RISK_THRESHOLD = 0.7
REVIEW_THRESHOLD = 0.4


@dataclass(frozen=True)
class InferenceResult:
    score: float
    label: RiskLabel


class InferenceError(Exception):
    """Raised when the inference backend cannot produce a prediction."""


class InferenceClient(Protocol):
    model_name: str
    model_version: str
    # Serving backend shown on the dashboard, e.g. "mock" or "onnxruntime".
    backend: str

    async def start(self) -> None: ...

    async def predict(self, features: PredictionFeatures) -> InferenceResult: ...

    async def ready(self) -> bool: ...

    async def close(self) -> None: ...


logger = logging.getLogger(__name__)

# Feature order expected by the model, as declared in its serving config (`feature_names`).
FEATURE_ORDER = ("amount", "events_per_hour", "distance_km")


def label_for(score: float) -> RiskLabel:
    if score >= HIGH_RISK_THRESHOLD:
        return "high_risk"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "low_risk"


class MockInferenceClient:
    """Deterministic logistic scorer with configurable latency.

    It stands in for TorchServe so the API, cache, and dashboard can be exercised end to end.
    """

    def __init__(self, model_name: str, model_version: str, latency_seconds: float = 0.0) -> None:
        self.model_name = model_name
        self.model_version = model_version
        self.backend = "mock"
        self._latency_seconds = latency_seconds

    async def predict(self, features: PredictionFeatures) -> InferenceResult:
        if self._latency_seconds:
            await asyncio.sleep(self._latency_seconds)
        logit = (
            -4.0
            + 0.9 * math.log1p(features.amount / 100)
            + 0.08 * features.events_per_hour
            + 0.0009 * features.distance_km
        )
        score = round(1 / (1 + math.exp(-logit)), 6)
        return InferenceResult(score=score, label=label_for(score))

    async def start(self) -> None:
        return None

    async def ready(self) -> bool:
        return True

    async def close(self) -> None:
        return None


class ServingInferenceClient:
    """Calls the model-serving service over the Open Inference Protocol (KServe v2).

    The served version is pinned at startup, either to MODEL_VERSION or, when that is `latest`, to
    the highest version the server reports. Pinning keeps cache keys and metrics consistent;
    switching versions is an explicit deployment step (M6).
    """

    def __init__(
        self,
        base_url: str,
        model_name: str,
        model_version: str,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.model_name = model_name
        self.backend = "onnxruntime"
        self._requested = model_version
        self.model_version = model_version if model_version != "latest" else "unresolved"
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            transport=transport,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )
        self._next_resolve = 0.0

    @property
    def resolved(self) -> bool:
        return self.model_version != "unresolved"

    async def start(self) -> None:
        await self._resolve()

    async def _resolve(self) -> bool:
        if self.resolved:
            return True
        if time.monotonic() < self._next_resolve:
            return False
        self._next_resolve = time.monotonic() + 2.0
        try:
            response = await self._client.get(f"/v2/models/{self.model_name}")
            response.raise_for_status()
            versions: list[str] = response.json()["versions"]
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            logger.warning("Model serving unavailable", extra={"error": repr(exc)})
            return False
        if not versions:
            return False
        self.model_version = max(versions, key=int)
        logger.info(
            "Resolved served model version",
            extra={"model_name": self.model_name, "model_version": self.model_version},
        )
        return True

    async def predict(self, features: PredictionFeatures) -> InferenceResult:
        if not await self._resolve():
            raise InferenceError("no model version is available")
        values = [getattr(features, name) for name in FEATURE_ORDER]
        body = {
            "id": str(uuid.uuid4()),
            "inputs": [
                {"name": "features", "shape": [1, len(values)], "datatype": "FP32", "data": values}
            ],
        }
        path = f"/v2/models/{self.model_name}/versions/{self.model_version}/infer"
        try:
            response = await self._client.post(path, json=body)
        except httpx.HTTPError as exc:
            raise InferenceError(f"model serving request failed: {exc!r}") from exc
        if response.status_code == 400:
            raise AppError(502, "inference_rejected", "The model rejected the request.")
        if response.status_code != 200:
            # 404 (version not loaded) and 5xx are transient from the caller's point of view.
            raise InferenceError(f"model serving returned {response.status_code}")
        payload: dict[str, Any] = response.json()
        score = round(float(payload["outputs"][0]["data"][0]), 6)
        return InferenceResult(score=score, label=label_for(score))

    async def ready(self) -> bool:
        if not await self._resolve():
            return False
        try:
            response = await self._client.get(
                f"/v2/models/{self.model_name}/versions/{self.model_version}/ready"
            )
        except httpx.HTTPError:
            return False
        return response.status_code == 200

    async def close(self) -> None:
        await self._client.aclose()


def build_inference(settings: Settings) -> InferenceClient:
    if settings.inference_backend == "serving":
        return ServingInferenceClient(
            settings.serving_url,
            settings.model_name,
            settings.model_version,
            timeout_seconds=settings.inference_timeout_seconds,
        )
    return MockInferenceClient(
        settings.model_name,
        settings.model_version,
        latency_seconds=settings.mock_inference_latency_ms / 1000,
    )
