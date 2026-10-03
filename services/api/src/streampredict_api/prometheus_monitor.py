"""Cluster-wide latency percentiles and active alerts from Prometheus.

Prometheus scrapes every replica, so its recording rules give true cluster percentiles where a
single gateway can only sample its own requests. Alerts (pending and firing) are surfaced on the
dashboard. Polled in the background; the overview falls back to local figures when it is down.
"""

import asyncio
import contextlib
import logging
import math
from datetime import datetime
from typing import Any

import httpx

from .schemas import AlertInfo, ObservabilityStatus

logger = logging.getLogger(__name__)

QUERIES = {
    "p50": "streampredict:prediction_latency_seconds:p50_1m",
    "p95": "streampredict:prediction_latency_seconds:p95_1m",
    "p99": "streampredict:prediction_latency_seconds:p99_1m",
}


class PrometheusMonitor:
    def __init__(
        self,
        base_url: str,
        *,
        interval_seconds: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=3, transport=transport
        )
        self._interval = interval_seconds
        self._latest = ObservabilityStatus(status="unavailable")
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="prometheus-monitor")

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

    async def _scalar(self, query: str) -> float | None:
        response = await self._client.get("/api/v1/query", params={"query": query})
        response.raise_for_status()
        result: list[dict[str, Any]] = response.json()["data"]["result"]
        if not result:
            return None
        value = float(result[0]["value"][1])
        return None if math.isnan(value) or math.isinf(value) else value

    async def refresh(self) -> None:
        try:
            latencies = {name: await self._scalar(q) for name, q in QUERIES.items()}
            response = await self._client.get("/api/v1/alerts")
            response.raise_for_status()
            raw_alerts: list[dict[str, Any]] = response.json()["data"]["alerts"]
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            if self._latest.status == "ok":
                logger.warning("Prometheus unavailable", extra={"error": repr(exc)})
            self._latest = ObservabilityStatus(status="unavailable")
            return
        alerts = [
            AlertInfo(
                name=alert["labels"].get("alertname", "unknown"),
                severity=alert["labels"].get("severity", "none"),
                state=alert.get("state", "firing"),
                summary=alert.get("annotations", {}).get("summary", ""),
                active_at=datetime.fromisoformat(alert["activeAt"].replace("Z", "+00:00"))
                if alert.get("activeAt")
                else None,
            )
            for alert in raw_alerts
        ]
        order = {"critical": 0, "warning": 1}
        alerts.sort(key=lambda a: (a.state != "firing", order.get(a.severity, 2), a.name))
        self._latest = ObservabilityStatus(
            status="ok",
            p50_ms=_ms(latencies["p50"]),
            p95_ms=_ms(latencies["p95"]),
            p99_ms=_ms(latencies["p99"]),
            alerts=alerts,
        )

    def overview(self) -> ObservabilityStatus:
        return self._latest


def _ms(seconds: float | None) -> float | None:
    return None if seconds is None else round(seconds * 1000, 2)
