"""Workload, autoscaler, and resource view of the StreamPredict namespace (in-cluster only).

Reads the Kubernetes API with the pod's service-account token (read-only RBAC, see
infra/kubernetes): deployments and their ready replicas, HorizontalPodAutoscalers (KEDA creates
one per ScaledObject), pod CPU/memory from metrics-server, and recent SuccessfulRescale events.
Polled in the background so the dashboard overview never waits on the API server.
"""

import asyncio
import contextlib
import logging
import os
import ssl
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from .schemas import KubernetesStatus, ScalingEvent, WorkloadStatus

logger = logging.getLogger(__name__)

SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
PART_OF = "app.kubernetes.io/part-of=streampredict"
COMPONENT_LABEL = "app.kubernetes.io/name"


def cpu_millicores(quantity: str) -> float:
    units = {"n": 1e-6, "u": 1e-3, "m": 1.0}
    if quantity and quantity[-1] in units:
        return float(quantity[:-1]) * units[quantity[-1]]
    return float(quantity) * 1000


def memory_mib(quantity: str) -> float:
    units = {"Ki": 1 / 1024, "Mi": 1.0, "Gi": 1024.0, "k": 1000 / 1024**2, "M": 1e6 / 1024**2}
    for suffix, factor in units.items():
        if quantity.endswith(suffix):
            return float(quantity[: -len(suffix)]) * factor
    return float(quantity) / 1024**2


def hpa_metric(hpa: dict[str, Any]) -> str | None:
    """Summarise every current metric of an autoscaling/v2 HPA status, e.g. "rps/pod 32 · cpu 41%".

    KEDA names external metrics after their trigger (`s0-kafka-<topic>`, `s1-prometheus`).
    """
    parts = []
    for metric in hpa.get("status", {}).get("currentMetrics") or []:
        if metric.get("type") == "Resource":
            utilization = metric["resource"]["current"].get("averageUtilization")
            if utilization is not None:
                parts.append(f"{metric['resource']['name']} {utilization}%")
        elif metric.get("type") == "External":
            current = metric["external"]["current"]
            value = current.get("averageValue") or current.get("value")
            if value is None:
                continue
            name = metric["external"].get("metric", {}).get("name", "")
            label = "lag/pod" if "kafka" in name else "rps/pod" if "prometheus" in name else name
            # Quantities may be milli-units ("154500m" = 154.5).
            parts.append(f"{label} {round(cpu_millicores(value) / 1000, 1):g}")
    return " · ".join(parts) or None


class KubernetesMonitor:
    def __init__(
        self,
        namespace: str,
        *,
        base_url: str | None = None,
        token: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        interval_seconds: float = 5.0,
    ) -> None:
        self._namespace = namespace
        self._interval = interval_seconds
        if base_url is None:
            host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
            port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
            base_url = f"https://{host}:{port}"
        verify: ssl.SSLContext | bool = True
        if transport is None and (SERVICE_ACCOUNT / "ca.crt").exists():
            verify = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / "ca.crt"))
        self._token = token
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=5, verify=verify, transport=transport
        )
        self._latest: KubernetesStatus | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="kubernetes-monitor")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._client.aclose()

    async def _loop(self) -> None:
        while not self._stopping:
            try:
                await self.refresh()
            except Exception:
                logger.exception("Kubernetes monitor refresh failed")
            await asyncio.sleep(self._interval)

    def _headers(self) -> dict[str, str]:
        # The projected token rotates; read it on every call.
        token = self._token or (SERVICE_ACCOUNT / "token").read_text().strip()
        return {"Authorization": f"Bearer {token}"}

    async def _get(self, path: str, **params: str) -> dict[str, Any]:
        response = await self._client.get(path, params=params, headers=self._headers())
        response.raise_for_status()
        body: dict[str, Any] = response.json()
        return body

    async def refresh(self) -> None:
        ns = self._namespace
        try:
            deployments = await self._get(
                f"/apis/apps/v1/namespaces/{ns}/deployments", labelSelector=PART_OF
            )
            hpas = await self._get(f"/apis/autoscaling/v2/namespaces/{ns}/horizontalpodautoscalers")
            events = await self._get(
                f"/api/v1/namespaces/{ns}/events", fieldSelector="reason=SuccessfulRescale"
            )
        except (httpx.HTTPError, OSError) as exc:
            if self._latest is None or self._latest.status == "ok":
                logger.warning("Kubernetes API unavailable", extra={"error": repr(exc)})
            self._latest = KubernetesStatus(status="unavailable", namespace=ns)
            return
        try:  # metrics-server is optional; workloads still render without usage figures
            pod_metrics = await self._get(
                f"/apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods", labelSelector=PART_OF
            )
        except (httpx.HTTPError, OSError):
            pod_metrics = {"items": []}
        self._latest = self._build(deployments, hpas, events, pod_metrics)

    def _build(
        self,
        deployments: dict[str, Any],
        hpas: dict[str, Any],
        events: dict[str, Any],
        pod_metrics: dict[str, Any],
    ) -> KubernetesStatus:
        cpu: dict[str, float] = defaultdict(float)
        memory: dict[str, float] = defaultdict(float)
        for pod in pod_metrics.get("items", []):
            component = pod["metadata"].get("labels", {}).get(COMPONENT_LABEL)
            if component is None:
                continue
            for container in pod.get("containers", []):
                cpu[component] += cpu_millicores(container["usage"]["cpu"])
                memory[component] += memory_mib(container["usage"]["memory"])
        by_target = {hpa["spec"]["scaleTargetRef"]["name"]: hpa for hpa in hpas.get("items", [])}
        workloads = []
        for deployment in sorted(deployments.get("items", []), key=lambda d: d["metadata"]["name"]):
            name = deployment["metadata"]["name"]
            component = deployment["metadata"].get("labels", {}).get(COMPONENT_LABEL, name)
            hpa = by_target.get(name)
            workloads.append(
                WorkloadStatus(
                    name=name,
                    replicas=deployment["spec"].get("replicas", 0),
                    ready=deployment.get("status", {}).get("readyReplicas", 0),
                    cpu_millicores=round(cpu[component], 1) if component in cpu else None,
                    memory_mib=round(memory[component], 1) if component in memory else None,
                    autoscaler=hpa["metadata"]["name"] if hpa else None,
                    min_replicas=hpa["spec"].get("minReplicas") if hpa else None,
                    max_replicas=hpa["spec"]["maxReplicas"] if hpa else None,
                    desired_replicas=hpa.get("status", {}).get("desiredReplicas") if hpa else None,
                    scaling_metric=hpa_metric(hpa) if hpa else None,
                )
            )
        scaling = []
        for event in events.get("items", []):
            stamp = (
                event.get("lastTimestamp")
                or event.get("eventTime")
                or event["metadata"].get("creationTimestamp")
            )
            target = event.get("involvedObject", {}).get("name", "")
            scaling.append(
                ScalingEvent(
                    at=datetime.fromisoformat(stamp.replace("Z", "+00:00")),
                    target=target,
                    message=event.get("message", ""),
                )
            )
        scaling.sort(key=lambda e: e.at, reverse=True)
        return KubernetesStatus(
            status="ok", namespace=self._namespace, workloads=workloads, scaling_events=scaling[:10]
        )

    def overview(self) -> KubernetesStatus:
        return self._latest or KubernetesStatus(status="unavailable", namespace=self._namespace)
