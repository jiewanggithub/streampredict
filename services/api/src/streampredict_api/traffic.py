"""Where demo traffic goes: straight into this process, or over HTTP to the gateway Service.

In a single-process deployment (Docker Compose) the orchestrator calls the prediction service and
the event publisher directly. When the orchestrator runs as its own service (Kubernetes), it sends
real HTTP requests to the gateway, so demo load is balanced across gateway replicas, shows up in
their metrics, and drives the gateway autoscaler like user traffic would.
"""

from typing import Any, Protocol

import httpx

from .errors import AppError
from .events import EventMetadata, EventPublisher, new_event
from .prediction import PredictionService
from .schemas import CacheStatus, PredictionFeatures


class TrafficSink(Protocol):
    async def predict(self, features: PredictionFeatures) -> CacheStatus: ...

    async def publish(self, features: PredictionFeatures, session_id: str) -> None: ...

    async def events_ready(self) -> bool: ...

    async def close(self) -> None: ...


class InProcessSink:
    def __init__(self, predictions: PredictionService, publisher: EventPublisher | None) -> None:
        self._predictions = predictions
        self._publisher = publisher

    async def predict(self, features: PredictionFeatures) -> CacheStatus:
        return (await self._predictions.predict(features, source="demo")).cache

    async def publish(self, features: PredictionFeatures, session_id: str) -> None:
        if self._publisher is None:
            raise AppError(503, "kafka_unavailable", "The event pipeline is not configured.")
        await self._publisher.publish(
            new_event(
                features,
                request_id=session_id,
                model_name=self._predictions.model_name,
                model_version=self._predictions.model_version,
                metadata=EventMetadata(source="dashboard-demo", demo_session_id=session_id),
            )
        )

    async def events_ready(self) -> bool:
        return self._publisher is not None and await self._publisher.ready()

    async def close(self) -> None:
        return None


class HttpSink:
    def __init__(self, api_url: str, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._client = httpx.AsyncClient(
            base_url=api_url.rstrip("/"),
            timeout=5,
            transport=transport,
            limits=httpx.Limits(max_connections=128, max_keepalive_connections=64),
        )

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self._client.post(path, json=body)
        except httpx.HTTPError as exc:
            raise AppError(503, "gateway_unavailable", "The API gateway is unreachable.") from exc
        payload: dict[str, Any] = response.json() if response.content else {}
        if response.status_code >= 400:
            error = payload.get("error", {})
            raise AppError(
                response.status_code,
                str(error.get("code", "gateway_error")),
                str(error.get("message", "The gateway rejected the request.")),
            )
        return payload

    async def predict(self, features: PredictionFeatures) -> CacheStatus:
        payload = await self._post("/api/v1/predict", {"features": features.model_dump()})
        cache: CacheStatus = payload.get("cache", "bypass")
        return cache

    async def publish(self, features: PredictionFeatures, session_id: str) -> None:
        await self._post(
            "/api/v1/events",
            {
                "features": features.model_dump(),
                "metadata": {"source": "dashboard-demo", "demo_session_id": session_id},
            },
        )

    async def events_ready(self) -> bool:
        try:
            response = await self._client.get("/ready")
        except httpx.HTTPError:
            return False
        return bool(response.json().get("checks", {}).get("kafka") == "ok")

    async def close(self) -> None:
        await self._client.aclose()
