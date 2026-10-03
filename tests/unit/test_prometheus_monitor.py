"""Prometheus monitor: percentile queries, NaN handling, alert ordering, and degradation."""

import asyncio
from typing import Any

import httpx

from streampredict_api.prometheus_monitor import PrometheusMonitor

ALERTS = [
    {"labels": {"alertname": "DeadLettersIncreasing", "severity": "warning"}, "state": "firing",
     "annotations": {"summary": "3 events dead-lettered"}, "activeAt": "2026-10-03T01:00:00Z"},
    {"labels": {"alertname": "KafkaConsumerLagHigh", "severity": "warning"}, "state": "pending",
     "annotations": {"summary": "Consumer lag 1500"}, "activeAt": "2026-10-03T01:01:00Z"},
    {"labels": {"alertname": "ServingNoModelLoaded", "severity": "critical"}, "state": "firing",
     "annotations": {"summary": "No model"}, "activeAt": "2026-10-03T01:02:00Z"},
]  # fmt: skip


def run(handler: Any) -> PrometheusMonitor:
    async def go() -> PrometheusMonitor:
        monitor = PrometheusMonitor("http://prom", transport=httpx.MockTransport(handler))
        await monitor.refresh()
        await monitor.stop()
        return monitor

    return asyncio.run(go())


def test_monitor_reads_percentiles_and_sorts_alerts() -> None:
    values = {"p50": "0.0021", "p95": "0.0154", "p99": "NaN"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/alerts":
            return httpx.Response(200, json={"data": {"alerts": ALERTS}})
        query = request.url.params["query"]
        name = query.split(":")[-1].split("_")[0]
        return httpx.Response(
            200, json={"data": {"result": [{"value": [1790000000, values[name]]}]}}
        )

    status = run(handler).overview()

    assert status.status == "ok"
    assert (status.p50_ms, status.p95_ms, status.p99_ms) == (2.1, 15.4, None)
    assert [a.name for a in status.alerts] == [
        "ServingNoModelLoaded",  # firing critical first
        "DeadLettersIncreasing",
        "KafkaConsumerLagHigh",  # pending last
    ]


def test_monitor_handles_empty_results_and_outages() -> None:
    def empty(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/alerts":
            return httpx.Response(200, json={"data": {"alerts": []}})
        return httpx.Response(200, json={"data": {"result": []}})

    status = run(empty).overview()
    assert status.status == "ok"
    assert status.p95_ms is None and status.alerts == []

    def down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    assert run(down).overview().status == "unavailable"
