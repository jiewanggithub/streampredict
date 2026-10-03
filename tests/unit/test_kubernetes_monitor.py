"""Kubernetes monitor: parsing workloads, autoscalers, pod usage, and rescale events."""

import asyncio

import httpx
import pytest

from streampredict_api.kubernetes_monitor import KubernetesMonitor, cpu_millicores, memory_mib

NS = "streampredict"

DEPLOYMENTS = {
    "items": [
        {
            "metadata": {"name": "consumer", "labels": {"app.kubernetes.io/name": "consumer"}},
            "spec": {"replicas": 4},
            "status": {"readyReplicas": 3},
        },
        {
            "metadata": {"name": "api", "labels": {"app.kubernetes.io/name": "api"}},
            "spec": {"replicas": 2},
            "status": {"readyReplicas": 2},
        },
    ]
}
HPAS = {
    "items": [
        {
            "metadata": {"name": "keda-hpa-consumer"},
            "spec": {"scaleTargetRef": {"name": "consumer"}, "minReplicas": 2, "maxReplicas": 8},
            "status": {
                "desiredReplicas": 8,
                "currentMetrics": [
                    {"type": "External", "external": {"current": {"averageValue": "212500m"}}}
                ],
            },
        },
        {
            "metadata": {"name": "api"},
            "spec": {"scaleTargetRef": {"name": "api"}, "minReplicas": 1, "maxReplicas": 4},
            "status": {
                "desiredReplicas": 2,
                "currentMetrics": [
                    {
                        "type": "Resource",
                        "resource": {"name": "cpu", "current": {"averageUtilization": 63}},
                    }
                ],
            },
        },
    ]
}
POD_METRICS = {
    "items": [
        {
            "metadata": {"labels": {"app.kubernetes.io/name": "consumer"}},
            "containers": [{"usage": {"cpu": "150000000n", "memory": "65536Ki"}}],
        },
        {
            "metadata": {"labels": {"app.kubernetes.io/name": "consumer"}},
            "containers": [{"usage": {"cpu": "50m", "memory": "64Mi"}}],
        },
    ]
}
EVENTS = {
    "items": [
        {
            "metadata": {"creationTimestamp": "2026-10-03T01:00:00Z"},
            "lastTimestamp": "2026-10-03T01:00:05Z",
            "involvedObject": {"name": "keda-hpa-consumer"},
            "message": "New size: 4; reason: external metric above target",
        },
        {
            "metadata": {"creationTimestamp": "2026-10-03T01:00:30Z"},
            "lastTimestamp": "2026-10-03T01:00:35Z",
            "involvedObject": {"name": "keda-hpa-consumer"},
            "message": "New size: 8; reason: external metric above target",
        },
    ]
}


def test_quantity_parsing() -> None:
    assert cpu_millicores("150000000n") == pytest.approx(150)
    assert cpu_millicores("250m") == 250
    assert cpu_millicores("2") == 2000
    assert memory_mib("65536Ki") == 64
    assert memory_mib("1Gi") == 1024
    assert memory_mib(str(32 * 1024**2)) == 32


def run_monitor(handler: object) -> KubernetesMonitor:
    async def run() -> KubernetesMonitor:
        monitor = KubernetesMonitor(
            NS,
            base_url="https://k8s",
            token="t",
            transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
        )
        await monitor.refresh()
        await monitor.stop()
        return monitor

    return asyncio.run(run())


def test_monitor_builds_workload_and_scaling_view() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer t"
        path = request.url.path
        if path.endswith("/deployments"):
            assert request.url.params["labelSelector"] == "app.kubernetes.io/part-of=streampredict"
            return httpx.Response(200, json=DEPLOYMENTS)
        if path.endswith("/horizontalpodautoscalers"):
            return httpx.Response(200, json=HPAS)
        if path.endswith("/events"):
            return httpx.Response(200, json=EVENTS)
        return httpx.Response(200, json=POD_METRICS)

    status = run_monitor(handler).overview()

    assert status.status == "ok"
    api, consumer = status.workloads
    assert (consumer.replicas, consumer.ready, consumer.desired_replicas) == (4, 3, 8)
    assert (consumer.min_replicas, consumer.max_replicas) == (2, 8)
    assert consumer.scaling_metric == "lag/pod 212.5"
    assert consumer.cpu_millicores == 200
    assert consumer.memory_mib == 128
    assert api.scaling_metric == "cpu 63%"
    assert api.cpu_millicores is None  # no pod metrics for the api component
    assert [e.message[:11] for e in status.scaling_events] == ["New size: 8", "New size: 4"]


def test_monitor_degrades_without_metrics_server_or_api() -> None:
    def no_metrics(request: httpx.Request) -> httpx.Response:
        if "metrics.k8s.io" in request.url.path:
            return httpx.Response(404)
        return httpx.Response(200, json={"items": []})

    assert run_monitor(no_metrics).overview().status == "ok"

    def forbidden(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    assert run_monitor(forbidden).overview().status == "unavailable"
