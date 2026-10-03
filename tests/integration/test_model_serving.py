"""Model-serving service: protocol endpoints, validation, batching, hot reload, gateway client."""

import asyncio
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
from fastapi.testclient import TestClient

from streampredict_api.config import Settings
from streampredict_api.errors import AppError
from streampredict_api.inference import InferenceError, ServingInferenceClient
from streampredict_api.schemas import PredictionFeatures
from streampredict_serving.config import ServingSettings
from streampredict_serving.metrics import ServingMetrics
from streampredict_serving.repository import discover
from streampredict_serving.runtime import LoadedVersion
from streampredict_serving.server import create_app
from tests.conftest import ClientFactory

REPOSITORY = Path(__file__).resolve().parents[2] / "services/model-serving/model_repository"
MODEL = "streampredict-demo"
ROWS = [[860.0, 4.0, 120.0], [5000.0, 30.0, 2000.0]]


def infer_body(rows: list[list[float]], **overrides: Any) -> dict[str, Any]:
    tensor = {"name": "features", "shape": [len(rows), 3], "datatype": "FP32"}
    tensor["data"] = [value for row in rows for value in row]
    tensor.update(overrides)
    return {"id": "req-1", "inputs": [tensor]}


def serving_client(repository: Path = REPOSITORY, **settings: Any) -> TestClient:
    return TestClient(create_app(ServingSettings(model_repository=repository, **settings)))


@pytest.fixture
def serving() -> Iterator[TestClient]:
    with serving_client() as client:
        yield client


# --- protocol ---------------------------------------------------------------------------------


def test_health_and_metadata(serving: TestClient) -> None:
    assert serving.get("/v2/health/live").json() == {"live": True}
    assert serving.get("/v2/health/ready").json() == {"ready": True}

    metadata = serving.get(f"/v2/models/{MODEL}").json()
    assert metadata["versions"] == ["1", "2"]
    assert metadata["platform"] == "onnxruntime_onnx"
    assert metadata["inputs"] == [{"name": "features", "datatype": "FP32", "shape": [-1, 3]}]
    assert metadata["parameters"]["feature_names"] == ["amount", "events_per_hour", "distance_km"]
    assert metadata["parameters"]["served_version"] == "2"
    assert metadata["parameters"]["training"]["metrics"]["auc"] > 0.7

    v1 = serving.get(f"/v2/models/{MODEL}/versions/1").json()
    assert v1["parameters"]["served_version"] == "1"
    assert serving.get(f"/v2/models/{MODEL}/versions/1/ready").json()["ready"] is True


def test_infer_defaults_to_latest_version_and_supports_pinning(serving: TestClient) -> None:
    latest = serving.post(f"/v2/models/{MODEL}/infer", json=infer_body(ROWS)).json()
    pinned = serving.post(f"/v2/models/{MODEL}/versions/1/infer", json=infer_body(ROWS)).json()

    assert latest["model_version"] == "2"
    assert latest["id"] == "req-1"
    output = latest["outputs"][0]
    assert output["name"] == "probability"
    assert output["shape"] == [2, 1]
    assert all(0 <= p <= 1 for p in output["data"])
    assert output["data"][1] > output["data"][0]  # riskier transaction, higher probability
    assert pinned["model_version"] == "1"
    assert pinned["outputs"][0]["data"] != output["data"]


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        (infer_body(ROWS, name="wrong"), "unexpected input"),
        (infer_body(ROWS, datatype="INT64"), "datatype"),
        (infer_body(ROWS, shape=[2, 4]), "shape"),
        (infer_body(ROWS, shape=[3, 3]), "does not match"),
        (infer_body([[1.0, float("nan"), 2.0]]), "NaN"),
        ({"inputs": []}, "invalid request"),
    ],
)
def test_invalid_requests_return_protocol_errors(
    serving: TestClient, body: dict[str, Any], fragment: str
) -> None:
    response = serving.post(
        f"/v2/models/{MODEL}/infer",
        content=_json(body),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    assert fragment in response.json()["error"]


def test_unknown_models_and_versions_are_404(serving: TestClient) -> None:
    assert serving.post("/v2/models/nope/infer", json=infer_body(ROWS)).status_code == 404
    response = serving.post(f"/v2/models/{MODEL}/versions/9/infer", json=infer_body(ROWS))
    assert response.status_code == 404
    assert "version '9'" in response.json()["error"]
    assert serving.get(f"/v2/models/{MODEL}/versions/9/ready").status_code == 503


def test_request_row_limit(tmp_path: Path) -> None:
    with serving_client(max_request_rows=2) as client:
        response = client.post(f"/v2/models/{MODEL}/infer", json=infer_body(ROWS * 2))
    assert response.status_code == 400
    assert "between 1 and 2 rows" in response.json()["error"]


def test_metrics_are_exported(serving: TestClient) -> None:
    serving.post(f"/v2/models/{MODEL}/infer", json=infer_body(ROWS))
    metrics = serving.get("/metrics").text
    assert (
        'streampredict_serving_requests_total{model="streampredict-demo",outcome="success",'
        'version="2"} 1.0' in metrics
    )
    assert (
        'streampredict_serving_model_ready{model="streampredict-demo",version="1"} 1.0' in metrics
    )


# --- repository lifecycle ---------------------------------------------------------------------


def test_versions_hot_load_and_unload(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    model_dir = repository / MODEL
    model_dir.mkdir(parents=True)
    shutil.copy(REPOSITORY / MODEL / "config.json", model_dir)
    shutil.copytree(REPOSITORY / MODEL / "1", model_dir / "1")

    with serving_client(repository) as client:
        assert client.get(f"/v2/models/{MODEL}").json()["versions"] == ["1"]

        shutil.copytree(REPOSITORY / MODEL / "2", model_dir / "2")
        assert client.post(f"/v2/repository/models/{MODEL}/load").json() == {MODEL: ["1", "2"]}
        infer = client.post(f"/v2/models/{MODEL}/infer", json=infer_body(ROWS)).json()
        assert infer["model_version"] == "2"

        shutil.rmtree(model_dir / "2")  # e.g. a rollback removes the bad version
        assert client.post(f"/v2/repository/models/{MODEL}/load").json() == {MODEL: ["1"]}
        infer = client.post(f"/v2/models/{MODEL}/infer", json=infer_body(ROWS)).json()
        assert infer["model_version"] == "1"


def test_latest_version_policy(tmp_path: Path) -> None:
    with serving_client(version_policy="latest") as client:
        assert client.get(f"/v2/models/{MODEL}").json()["versions"] == ["2"]


# --- dynamic batching -------------------------------------------------------------------------


def test_concurrent_requests_are_batched() -> None:
    async def run() -> tuple[ServingMetrics, list[float]]:
        source = discover(REPOSITORY)[MODEL]
        metrics = ServingMetrics()
        version = LoadedVersion(
            source.config,
            source.versions[-1],
            metrics,
            max_batch_size=64,
            max_queue_delay_seconds=0.05,
            intra_op_threads=1,
        )
        await version.start()
        try:
            import numpy as np

            requests = [np.array([[100.0 + i, 1.0, 10.0]], dtype=np.float32) for i in range(20)]
            outputs = await asyncio.gather(*(version.infer(r) for r in requests))
        finally:
            await version.stop()
        return metrics, [float(o[0, 0]) for o in outputs]

    metrics, scores = asyncio.run(run())
    labels = {"model": MODEL, "version": "2"}
    batches = metrics.registry.get_sample_value("streampredict_serving_batch_rows_count", labels)
    rows = metrics.registry.get_sample_value("streampredict_serving_batch_rows_sum", labels)
    assert rows == 20
    assert batches is not None and batches <= 2  # 20 concurrent requests, at most 2 batches
    assert len(set(scores)) == 20  # each caller got its own row back


# --- gateway client ---------------------------------------------------------------------------


def mock_serving(handler: Any) -> ServingInferenceClient:
    return ServingInferenceClient(
        "http://serving", MODEL, "latest", timeout_seconds=1, transport=httpx.MockTransport(handler)
    )


def test_client_resolves_latest_version_and_maps_scores() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path == f"/v2/models/{MODEL}":
            return httpx.Response(200, json={"versions": ["1", "2", "10"]})
        return httpx.Response(200, json={"outputs": [{"data": [0.83]}]})

    async def run() -> tuple[str, float, str]:
        client = mock_serving(handler)
        await client.start()
        result = await client.predict(
            PredictionFeatures(amount=1, events_per_hour=2, distance_km=3)
        )
        await client.close()
        return client.model_version, result.score, result.label

    assert asyncio.run(run()) == ("10", 0.83, "high_risk")
    assert seen[-1] == f"/v2/models/{MODEL}/versions/10/infer"


@pytest.mark.parametrize(
    ("status", "error"),
    [(503, InferenceError), (404, InferenceError), (400, AppError), (None, InferenceError)],
)
def test_client_maps_failures(status: int | None, error: type[Exception]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/v2/models/{MODEL}":
            return httpx.Response(200, json={"versions": ["1"]})
        if status is None:
            raise httpx.ConnectError("refused")
        return httpx.Response(status, json={"error": "x"})

    async def run() -> None:
        client = mock_serving(handler)
        try:
            await client.predict(PredictionFeatures(amount=1, events_per_hour=2, distance_km=3))
        finally:
            await client.close()

    with pytest.raises(error):
        asyncio.run(run())


# --- end to end: gateway -> real serving process ----------------------------------------------


@pytest.fixture(scope="module")
def serving_url() -> Iterator[str]:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    app = create_app(ServingSettings(model_repository=REPOSITORY))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{url}/v2/health/ready").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    yield url
    server.should_exit = True
    thread.join(10)


def test_gateway_predicts_through_model_serving(
    make_client: ClientFactory, serving_url: str
) -> None:
    client = make_client(
        inference_backend="serving",
        serving_url=serving_url,
        model_version="latest",
        inference_timeout_seconds=5,
    )
    payload = {"features": {"amount": 5000, "events_per_hour": 30, "distance_km": 2000}}

    first = client.post("/api/v1/predict", json=payload).json()
    second = client.post("/api/v1/predict", json=payload).json()

    assert first["model_version"] == "2"
    assert first["cache"] == "miss"
    assert first["label"] == "high_risk"
    assert second["cache"] == "hit"
    assert second["score"] == first["score"]
    overview = client.get("/api/v1/metrics/overview").json()
    assert overview["model"]["backend"] == "onnxruntime"
    assert overview["model"]["version"] == "2"
    assert client.get("/ready").json()["checks"]["inference"] == "ok"


def test_gateway_reports_unavailable_serving(make_client: ClientFactory) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        closed_port = sock.getsockname()[1]
    client = make_client(
        inference_backend="serving",
        serving_url=f"http://127.0.0.1:{closed_port}",
        model_version="1",
    )
    payload = {"features": {"amount": 5000, "events_per_hour": 30, "distance_km": 2000}}
    response = client.post("/api/v1/predict", json=payload)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "inference_unavailable"
    ready = client.get("/ready")
    assert ready.status_code == 503
    assert ready.json()["checks"]["inference"] == "unavailable"


def _json(body: dict[str, Any]) -> str:
    import json

    # allow_nan so the NaN case reaches the server's own validation
    return json.dumps(body, allow_nan=True)


def test_settings_default_to_mock_backend() -> None:
    assert Settings(_env_file=None).inference_backend == "mock"


def test_default_version_pointer_routes_unversioned_requests(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    shutil.copytree(REPOSITORY, repository)
    (repository / MODEL / "serving.json").write_text('{"default_version": "1"}')

    with serving_client(repository) as client:
        assert client.get(f"/v2/models/{MODEL}").json()["parameters"]["served_version"] == "1"
        infer = client.post(f"/v2/models/{MODEL}/infer", json=infer_body(ROWS)).json()
        assert infer["model_version"] == "1"

        (repository / MODEL / "serving.json").write_text('{"default_version": "2"}')
        client.post(f"/v2/repository/models/{MODEL}/load")
        infer = client.post(f"/v2/models/{MODEL}/infer", json=infer_body(ROWS)).json()
        assert infer["model_version"] == "2"
        assert "streampredict_serving_output_score_bucket" in client.get("/metrics").text
