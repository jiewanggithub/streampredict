"""Gateway view of the deployment controller: cached status for the dashboard and release proxies.

The controller is the release authority; the gateway only reads its status (polled in the
background, so the overview never waits on it) and forwards demo-guarded release requests.
"""

import asyncio
import contextlib
import logging
from typing import Any

import httpx

from .errors import AppError
from .schemas import DeploymentStatus

logger = logging.getLogger(__name__)


class DeploymentMonitor:
    def __init__(
        self,
        controller_url: str,
        model_name: str,
        *,
        interval_seconds: float = 2.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._model = model_name
        self._interval = interval_seconds
        self._client = httpx.AsyncClient(
            base_url=controller_url.rstrip("/"), timeout=5, transport=transport
        )
        self._latest: DeploymentStatus | None = None
        self._available = False
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="deployment-monitor")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._client.aclose()

    async def _loop(self) -> None:
        while not self._stopping:
            await self.refresh()
            await asyncio.sleep(self._interval)

    async def refresh(self) -> None:
        try:
            response = await self._client.get(f"/models/{self._model}/deployment")
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            data["history"] = data.get("history", [])[:10]
            data["events"] = data.get("events", [])[:10]
            self._latest = DeploymentStatus.model_validate({"status": "ok", **data})
            self._available = True
        except Exception as exc:
            if self._available:
                logger.warning("Deployment controller unavailable", extra={"error": repr(exc)})
            self._available = False

    def overview(self) -> DeploymentStatus:
        if self._available and self._latest is not None:
            return self._latest
        return DeploymentStatus(status="unavailable")

    async def release(self, version: str) -> dict[str, Any]:
        result = await self._post(f"/models/{self._model}/releases", {"version": version})
        await self.refresh()
        return result

    async def rollback(self) -> dict[str, Any]:
        result = await self._post(f"/models/{self._model}/rollback", None)
        await self.refresh()
        return result

    async def _post(self, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        try:
            response = await self._client.post(path, json=body)
        except httpx.HTTPError as exc:
            raise AppError(
                503, "controller_unavailable", "The deployment controller is unreachable."
            ) from exc
        if response.status_code in (404, 409):
            detail = response.json().get("detail", "Request rejected by the controller.")
            code = "release_conflict" if response.status_code == 409 else "not_found"
            raise AppError(response.status_code, code, str(detail))
        if response.status_code >= 400:
            raise AppError(502, "controller_error", "The deployment controller failed.")
        result: dict[str, Any] = response.json()
        return result
