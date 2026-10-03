"""Deployment controller: gates, promotion, automatic and manual rollback, gateway follow-through.

A real model-serving process serves a temporary repository that the controller manages; only the
MLflow registry is replaced by an in-memory fake fed from the committed seed artifacts, so every
gate decision runs on real model outputs.
"""

import asyncio
import json
import shutil
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

from streampredict_api.inference import ServingInferenceClient
from streampredict_controller.config import ControllerSettings
from streampredict_controller.gates import (
    ProbeResult,
    ScoreProfile,
    contract_problems,
    fractions,
    offline_gate,
    psi,
    reference_batch,
    runtime_gate,
)
from streampredict_controller.pipeline import ReleaseConflict, ReleaseManager
from streampredict_controller.registry import ReleaseRecord, VersionInfo
from streampredict_controller.serving import FileServingAdmin
from streampredict_serving.config import ServingSettings
from streampredict_serving.server import create_app as create_serving_app

SEED = Path(__file__).resolve().parents[2] / "ml/artifacts/model_repository"
MODEL = "streampredict-demo"


class FakeRegistry:
    """In-memory stand-in for MLflow, backed by the committed seed artifacts."""

    def __init__(self, source: Path, champion: str = "1") -> None:
        self.source = source
        self._champion: str | None = champion
        self.status: dict[str, str] = {}
        self.reasons: dict[str, str] = {}
        self.records: list[ReleaseRecord] = []
        self.metric_overrides: dict[str, dict[str, float]] = {}

    def versions(self) -> list[VersionInfo]:
        infos = []
        for version_dir in sorted(
            (p for p in (self.source / MODEL).iterdir() if p.name.isdigit()),
            key=lambda p: int(p.name),
        ):
            meta = json.loads((version_dir / "metadata.json").read_text())
            infos.append(
                VersionInfo(
                    version=version_dir.name,
                    profile=meta["profile"],
                    description=meta.get("description", ""),
                    status=self.status.get(version_dir.name, "candidate"),
                    metrics=self.metric_overrides.get(version_dir.name, meta["metrics"]),
                    run_id=None,
                    created_at=0,
                )
            )
        return infos

    def champion(self) -> str | None:
        return self._champion

    def set_champion(self, version: str) -> None:
        self._champion = version

    def set_status(self, version: str, status: str, reason: str | None = None) -> None:
        self.status[version] = status
        if reason:
            self.reasons[version] = reason

    def download(self, version: str, destination: Path) -> Path:
        bundle = destination / "model"
        shutil.copytree(self.source / MODEL / version, bundle)
        shutil.copy(self.source / MODEL / "config.json", bundle / "config.json")
        return bundle

    def record_release(self, record: ReleaseRecord) -> None:
        self.records.append(record)

    def history(self, limit: int) -> list[ReleaseRecord]:
        return self.records[::-1][:limit]


@pytest.fixture
def serving(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    repository = tmp_path / "deployed"
    repository.mkdir()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    app = create_serving_app(ServingSettings(model_repository=repository))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            httpx.get(f"{url}/v2/health/live")
            break
        except httpx.HTTPError:
            time.sleep(0.05)
    yield url, repository
    server.should_exit = True
    thread.join(10)


def run_with_manager(
    serving: tuple[str, Path], registry: FakeRegistry, scenario: Any, **settings: Any
) -> Any:
    url, repository = serving

    async def run() -> Any:
        config = ControllerSettings(
            serving_url=url, deployed_repository=repository, soak_seconds=0, **settings
        )
        admin = FileServingAdmin(repository, MODEL, url)
        manager = ReleaseManager(config, registry, admin)
        try:
            await manager.reconcile()
            return await scenario(manager, admin, url)
        finally:
            await admin.close()

    return asyncio.run(run())


async def release(manager: ReleaseManager, version: str) -> ReleaseRecord:
    manager.start_release(version)
    await manager.wait()
    return manager.history[0]


async def served_version(url: str) -> str:
    async with httpx.AsyncClient(base_url=url) as client:
        metadata = (await client.get(f"/v2/models/{MODEL}")).json()
    return str(metadata["parameters"]["served_version"])


# --- end-to-end release flows -----------------------------------------------------------------


def test_reconcile_deploys_the_champion(serving: tuple[str, Path]) -> None:
    async def scenario(manager: ReleaseManager, admin: FileServingAdmin, url: str) -> None:
        assert manager.reconciled
        assert await admin.installed() == ["1"]
        assert await admin.default() == "1"
        assert await served_version(url) == "1"

    run_with_manager(serving, FakeRegistry(SEED), scenario)


def test_good_release_is_promoted_and_bad_release_rolls_back(serving: tuple[str, Path]) -> None:
    registry = FakeRegistry(SEED)

    async def scenario(manager: ReleaseManager, admin: FileServingAdmin, url: str) -> None:
        promoted = await release(manager, "2")
        assert promoted.outcome == "promoted", promoted.reasons
        assert registry.champion() == "2"
        assert await served_version(url) == "2"
        assert promoted.metrics["psi"] < 0.25

        # v3 has healthy offline metrics but a train/serve skew bug.
        failed = await release(manager, "3")
        assert failed.outcome == "rolled_back"
        assert failed.previous == "2"
        assert any("PSI" in reason for reason in failed.reasons), failed.reasons
        assert failed.metrics["psi"] > 0.25
        assert registry.champion() == "2"
        assert registry.status["3"] == "rolled_back"
        assert await served_version(url) == "2"
        assert "3" not in await admin.installed()

    run_with_manager(serving, registry, scenario)
    assert [r.outcome for r in registry.records] == ["promoted", "rolled_back"]


def test_offline_gate_rejects_before_touching_traffic(serving: tuple[str, Path]) -> None:
    registry = FakeRegistry(SEED)
    registry.metric_overrides["2"] = {"auc": 0.55}

    async def scenario(manager: ReleaseManager, admin: FileServingAdmin, url: str) -> None:
        record = await release(manager, "2")
        assert record.outcome == "rejected"
        assert "below the minimum" in record.reasons[0]
        assert await admin.installed() == ["1"]
        assert await served_version(url) == "1"

    run_with_manager(serving, registry, scenario)
    assert registry.status["2"] == "rejected"


def test_incompatible_contract_is_rejected(serving: tuple[str, Path], tmp_path: Path) -> None:
    source = tmp_path / "seed"
    shutil.copytree(SEED, source)
    config_path = source / MODEL / "config.json"
    config = json.loads(config_path.read_text())
    registry = FakeRegistry(source)

    async def scenario(manager: ReleaseManager, admin: FileServingAdmin, url: str) -> None:
        config["inputs"][0]["shape"] = [-1, 4]  # e.g. a model expecting a new feature
        config_path.write_text(json.dumps(config))
        record = await release(manager, "2")
        assert record.outcome == "rejected"
        assert "inputs differ" in record.reasons[0]
        assert await served_version(url) == "1"

    run_with_manager(serving, registry, scenario)


def test_manual_rollback_and_conflicts(serving: tuple[str, Path]) -> None:
    registry = FakeRegistry(SEED)

    async def scenario(manager: ReleaseManager, admin: FileServingAdmin, url: str) -> None:
        with pytest.raises(ReleaseConflict, match="no previous stable"):
            manager.start_rollback()
        manager.start_release("2")
        with pytest.raises(ReleaseConflict, match="in progress"):
            manager.start_release("3")
        await manager.wait()
        assert registry.champion() == "2"

        manager.start_rollback()
        await manager.wait()
        assert registry.champion() == "1"
        assert await served_version(url) == "1"
        assert manager.history[0].outcome == "manual_rollback"

    run_with_manager(serving, registry, scenario)


def test_gateway_follows_the_served_version(serving: tuple[str, Path]) -> None:
    registry = FakeRegistry(SEED)

    async def scenario(manager: ReleaseManager, admin: FileServingAdmin, url: str) -> list[str]:
        client = ServingInferenceClient(
            url, MODEL, "latest", timeout_seconds=5, follow_seconds=0.05
        )
        await client.start()
        seen = [client.model_version]
        await release(manager, "2")
        await asyncio.sleep(0.3)
        seen.append(client.model_version)
        await client.close()
        return seen

    assert run_with_manager(serving, registry, scenario) == ["1", "2"]


# --- gate functions ---------------------------------------------------------------------------


def test_psi_detects_distribution_shift() -> None:
    stable = fractions([0.1, 0.2, 0.3, 0.4, 0.5] * 100)
    assert psi(stable, stable) == pytest.approx(0, abs=1e-9)
    assert psi(stable, fractions([0.05] * 500)) > 1


def test_offline_gate_and_contract_checks() -> None:
    assert offline_gate({"auc": 0.78}, {"auc": 0.72}, min_auc=0.7, max_regression=0.02).passed
    regressed = offline_gate({"auc": 0.74}, {"auc": 0.78}, min_auc=0.7, max_regression=0.02)
    assert not regressed.passed and "regresses" in regressed.reasons[0]
    assert not offline_gate({}, None, min_auc=0.7, max_regression=0.02).passed

    contract = json.loads((SEED / MODEL / "config.json").read_text())
    assert contract_problems(contract, contract) == []
    renamed = {**contract, "parameters": {"feature_names": ["a", "b", "c"]}}
    assert "feature order" in contract_problems(renamed, contract)[0]


def test_runtime_gate_flags_latency_errors_and_missing_profile() -> None:
    scores = list(reference_batch(200, 1)[:, 1] / 40)
    expected = ScoreProfile(fractions(scores), high_risk_rate=0.0)
    slow = ProbeResult(scores, [80.0] * 10, errors=1, requests=10)
    limits: dict[str, Any] = {
        "max_psi": 0.25,
        "max_high_risk_rate_drop": 0.5,
        "max_p95_latency_ms": 50,
        "max_error_rate": 0.01,
    }
    result = runtime_gate(slow, expected, live_errors=5, live_requests=100, **limits)
    assert not result.passed
    assert {r.split(" ")[0] for r in result.reasons} == {"probe", "live", "p95"}

    healthy = ProbeResult(scores, [5.0] * 10, errors=0, requests=10)
    assert runtime_gate(healthy, expected, live_errors=0, live_requests=0, **limits).passed
    unprofiled = runtime_gate(healthy, None, live_errors=0, live_requests=0, **limits)
    assert unprofiled.reasons == ["candidate has no recorded training score profile"]


# --- HTTP surfaces: controller API and gateway proxy ------------------------------------------


def test_controller_api_answers_cheap_checks_synchronously(serving: tuple[str, Path]) -> None:
    from fastapi.testclient import TestClient

    from streampredict_controller.api import create_app as create_controller_app

    url, repository = serving
    registry = FakeRegistry(SEED)
    settings = ControllerSettings(serving_url=url, deployed_repository=repository, soak_seconds=0)
    with TestClient(create_controller_app(settings, registry=registry)) as client:
        deadline = time.monotonic() + 10
        while client.get("/ready").status_code != 200 and time.monotonic() < deadline:
            time.sleep(0.05)
        status = client.get(f"/models/{MODEL}/deployment").json()
        assert status["champion"] == "1"
        assert status["serving_default"] == "1"
        assert [v["profile"] for v in status["versions"]] == ["baseline", "improved", "skewed"]

        post = f"/models/{MODEL}/releases"
        assert client.post(post, json={"version": "1"}).status_code == 409
        assert client.post(post, json={"version": "9"}).status_code == 404
        assert client.post("/models/other/releases", json={"version": "2"}).status_code == 404
        assert client.post(post, json={"version": "2"}).status_code == 202
        deadline = time.monotonic() + 15
        while client.get(f"/models/{MODEL}/deployment").json()["active"] is not None:
            assert time.monotonic() < deadline
            time.sleep(0.05)
        assert client.get(f"/models/{MODEL}/deployment").json()["champion"] == "2"
        assert "streampredict_controller_champion_version 2.0" in client.get("/metrics").text


def test_gateway_proxies_releases_and_reports_deployment(make_client: Any) -> None:
    from streampredict_api.deployment import DeploymentMonitor

    status = {
        "model": MODEL,
        "champion": "2",
        "serving_default": "2",
        "installed": ["1", "2"],
        "versions": [{"version": "2", "profile": "improved", "status": "champion", "auc": 0.78}],
        "active": None,
        "history": [],
        "events": [],
    }
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(200, json=status)
        if request.url.path.endswith("/releases"):
            version = json.loads(request.content)["version"]
            if version == "2":
                return httpx.Response(409, json={"detail": "v2 is already the champion"})
            return httpx.Response(202, json={"version": version, "stage": "validating"})
        return httpx.Response(202, json={"version": "1", "stage": "deploying"})

    def factory(settings: Any) -> DeploymentMonitor:
        return DeploymentMonitor(
            "http://controller", settings.model_name, transport=httpx.MockTransport(handler)
        )

    client = make_client(deployment_factory=factory, demo_control_token="s3cret")
    token = {"X-Demo-Token": "s3cret"}
    assert client.post("/api/v1/models/releases", json={"version": "3"}).status_code == 401
    accepted = client.post("/api/v1/models/releases", json={"version": "3"}, headers=token)
    assert accepted.status_code == 202
    conflict = client.post("/api/v1/models/releases", json={"version": "2"}, headers=token)
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "release_conflict"
    invalid = client.post("/api/v1/models/releases", json={"version": "v3"}, headers=token)
    assert invalid.status_code == 422
    assert client.post("/api/v1/models/rollback", headers=token).status_code == 202
    deployment = client.get("/api/v1/metrics/overview").json()["deployment"]
    assert deployment["status"] == "ok"
    assert deployment["champion"] == "2"
    assert ("POST", f"/models/{MODEL}/rollback") in calls


def test_gateway_without_controller_disables_release_controls(make_client: Any) -> None:
    client = make_client()
    response = client.post("/api/v1/models/releases", json={"version": "2"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "controller_disabled"
    assert client.get("/api/v1/metrics/overview").json()["deployment"] is None
