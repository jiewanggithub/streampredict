"""Standalone demo orchestrator: HTTP traffic to the gateway and proxying from gateway replicas."""

import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import fakeredis
import httpx
import uvicorn
from fastapi import FastAPI
from redis.asyncio import Redis

from streampredict_api.demo_service import create_app as create_orchestrator
from streampredict_api.main import create_app
from tests.conftest import build_settings

PAYLOAD = {"features": {"amount": 860, "events_per_hour": 4, "distance_km": 120}}


@contextmanager
def serve(app: FastAPI) -> Iterator[str]:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.02)
    try:
        yield url
    finally:
        server.should_exit = True
        thread.join(10)


def gateway(redis: Any, **overrides: Any) -> FastAPI:
    return create_app(build_settings(**overrides), redis_factory=lambda _: cast(Redis, redis))


def wait_state(url: str, states: set[str], timeout: float = 15) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status: dict[str, Any] = httpx.get(f"{url}/api/v1/demo/status").json()
        if status["state"] in states:
            return status
        time.sleep(0.1)
    raise AssertionError(f"demo did not reach {states}")


def test_orchestrator_drives_gateway_replicas_over_http() -> None:
    redis = fakeredis.FakeAsyncRedis()  # shared by both gateway replicas, like the real Redis
    with serve(gateway(redis)) as target:
        orchestrator = create_orchestrator(build_settings(demo_target_url=target, demo_max_rps=20))
        with serve(orchestrator) as orchestrator_url:
            replica_a = gateway(redis, demo_orchestrator_url=orchestrator_url, demo_max_rps=20)
            replica_b = gateway(redis, demo_orchestrator_url=orchestrator_url, demo_max_rps=20)
            with serve(replica_a) as a, serve(replica_b) as b:
                started = httpx.post(
                    f"{a}/api/v1/demo/traffic-spike",
                    json={"profile": "standard", "duration_seconds": 1},
                )
                assert started.status_code == 202
                # The one-session limit holds across replicas: B sees A's session.
                conflict = httpx.post(f"{b}/api/v1/demo/traffic-spike", json={})
                assert conflict.status_code == 409
                assert conflict.json()["error"]["code"] == "demo_already_running"

                status = wait_state(b, {"completed", "failed"})
                assert status["state"] == "completed"
                assert status["session_id"] == started.json()["session_id"]
                total = status["summary"]["total_requests"]
                assert 0 < total <= 20

                # The synthetic requests went through the target gateway over HTTP, and the
                # cluster counters make them visible from any replica.
                overview = httpx.get(f"{a}/api/v1/metrics/overview").json()
                assert overview["traffic"]["scope"] == "cluster"
                assert overview["traffic"]["requests_in_window"] >= total
                assert overview["demo"]["state"] == "completed"


def test_proxy_reports_unreachable_orchestrator() -> None:
    redis = fakeredis.FakeAsyncRedis()
    with serve(gateway(redis, demo_orchestrator_url="http://127.0.0.1:9")) as url:
        response = httpx.post(f"{url}/api/v1/demo/traffic-spike", json={})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "orchestrator_unavailable"
        assert httpx.get(f"{url}/api/v1/demo/status").json()["state"] == "idle"
