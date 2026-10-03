"""In-process integration tests for the API gateway with an in-memory Redis."""

import json
import time

from fastapi.testclient import TestClient

from streampredict_api.kafka_monitor import GroupMember, OffsetSnapshot
from tests.conftest import ClientFactory, FakeOffsetSource, FakePublisher, fake_kafka

PAYLOAD = {"features": {"amount": 860, "events_per_hour": 4, "distance_km": 120}}


def wait_for_state(
    client: TestClient, states: set[str], timeout: float = 10.0
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status: dict[str, object] = client.get("/api/v1/demo/status").json()
        if status["state"] in states:
            return status
        time.sleep(0.05)
    raise AssertionError(f"demo did not reach {states}")


def test_health_and_ready(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready", "checks": {"redis": "ok", "inference": "ok"}}


def test_predict_uses_cache_aside(client: TestClient) -> None:
    first = client.post("/api/v1/predict", json=PAYLOAD)
    second = client.post("/api/v1/predict", json=PAYLOAD)

    assert first.status_code == 200
    body = first.json()
    assert body["cache"] == "miss"
    assert body["model_version"] == "v-test"
    assert body["label"] == "low_risk"
    assert body["request_id"] == first.headers["X-Request-ID"]
    assert second.json()["cache"] == "hit"
    assert second.json()["score"] == body["score"]


def test_request_id_is_propagated_when_valid(client: TestClient) -> None:
    kept = client.post("/api/v1/predict", json=PAYLOAD, headers={"X-Request-ID": "abc-123"})
    replaced = client.get("/health", headers={"X-Request-ID": "bad id with spaces"})
    assert kept.json()["request_id"] == "abc-123"
    assert replaced.headers["X-Request-ID"] != "bad id with spaces"


def test_validation_errors_use_unified_format_without_echoing_input(client: TestClient) -> None:
    response = client.post(
        "/api/v1/predict",
        json={"features": {"amount": -5, "events_per_hour": 1, "distance_km": 1, "ssn": "secret"}},
    )
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert error["request_id"] == response.headers["X-Request-ID"]
    assert "secret" not in response.text
    assert {tuple(detail["loc"]) for detail in error["details"]} == {
        ("body", "features", "amount"),
        ("body", "features", "ssn"),
    }


def test_unknown_route_uses_unified_format(client: TestClient) -> None:
    response = client.get("/does-not-exist")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


def test_redis_outage_degrades_to_bypass(make_client: ClientFactory) -> None:
    client = make_client(redis_available=False)
    started = time.perf_counter()
    response = client.post("/api/v1/predict", json=PAYLOAD)
    assert time.perf_counter() - started < 1
    assert response.status_code == 200
    assert response.json()["cache"] == "bypass"

    ready = client.get("/ready").json()
    assert ready == {"status": "degraded", "checks": {"redis": "unavailable", "inference": "ok"}}


def test_overview_and_prometheus_metrics(client: TestClient) -> None:
    client.post("/api/v1/predict", json=PAYLOAD)
    client.post("/api/v1/predict", json=PAYLOAD)

    overview = client.get("/api/v1/metrics/overview").json()
    assert overview["model"] == {
        "name": "streampredict-demo",
        "version": "v-test",
        "backend": "mock",
        "predictions_in_window": 2,
        "error_rate": 0.0,
        "label_distribution": {"low_risk": 2, "review": 0, "high_risk": 0},
    }
    assert overview["traffic"]["requests_in_window"] == 2
    assert overview["traffic"]["success_rate"] == 100.0
    assert overview["cache"]["hit_rate"] == 50.0
    assert overview["demo"]["state"] == "idle"
    assert overview["kafka"] is None

    metrics = client.get("/metrics").text
    hit_series = 'streampredict_predictions_total{cache="hit",outcome="success",source="api"}'
    model_series = (
        'streampredict_model_info{model_name="streampredict-demo",model_version="v-test"}'
    )
    assert f"{hit_series} 1.0" in metrics
    assert f"{model_series} 1.0" in metrics
    label_series = 'streampredict_prediction_labels_total{label="low_risk",source="api"}'
    assert f"{label_series} 2.0" in metrics
    assert 'route="/api/v1/predict"' in metrics


def test_demo_runs_to_completion_within_limits(make_client: ClientFactory) -> None:
    client = make_client(demo_max_rps=40)
    started = client.post(
        "/api/v1/demo/traffic-spike",
        json={"profile": "standard", "target_rps": 500, "duration_seconds": 1},
    )
    assert started.status_code == 202
    assert started.json()["target_rps"] == 40

    conflict = client.post("/api/v1/demo/traffic-spike", json={"profile": "spike"})
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "demo_already_running"

    status = wait_for_state(client, {"completed", "failed"})
    assert status["state"] == "completed"
    assert status["stop_reason"] == "duration_reached"
    summary = status["summary"]
    assert isinstance(summary, dict)
    assert 0 < summary["total_requests"] <= 40
    assert summary["peak_rps"] <= 40
    assert summary["errors"] == 0


def test_demo_manual_stop_and_restart(make_client: ClientFactory) -> None:
    client = make_client()
    client.post("/api/v1/demo/traffic-spike", json={"profile": "spike", "duration_seconds": 60})
    wait_for_state(client, {"running"})

    client.post("/api/v1/demo/stop")
    status = wait_for_state(client, {"completed", "failed"})
    assert status["state"] == "completed"
    assert status["stop_reason"] == "manual"
    assert float(str(status["elapsed_seconds"])) < 10

    restarted = client.post("/api/v1/demo/traffic-spike", json={"duration_seconds": 1})
    assert restarted.status_code == 202


def test_demo_duration_is_capped(make_client: ClientFactory) -> None:
    client = make_client(demo_max_duration_seconds=2)
    response = client.post("/api/v1/demo/traffic-spike", json={"duration_seconds": 3600})
    assert response.json()["duration_seconds"] == 2
    client.post("/api/v1/demo/stop")


def test_demo_controls_require_token_when_configured(make_client: ClientFactory) -> None:
    client = make_client(demo_control_token="s3cret")
    assert client.post("/api/v1/demo/stop").status_code == 401
    assert client.post("/api/v1/demo/stop", headers={"X-Demo-Token": "wrong"}).status_code == 401
    assert client.post("/api/v1/demo/stop", headers={"X-Demo-Token": "s3cret"}).status_code == 200
    assert client.get("/api/v1/demo/status").status_code == 200


def test_empty_demo_token_disables_protection(make_client: ClientFactory) -> None:
    client = make_client(demo_control_token="")
    assert client.post("/api/v1/demo/stop").status_code == 200


def test_metrics_stream_emits_overview_events(make_client: ClientFactory) -> None:
    client = make_client(metrics_stream_interval_seconds=0.05, metrics_stream_max_seconds=0.2)
    client.post("/api/v1/predict", json=PAYLOAD)

    response = client.get("/api/v1/metrics/stream")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = [chunk for chunk in response.text.split("\n\n") if chunk.startswith("event:")]
    assert len(events) >= 2
    name, data = events[0].split("\n", 1)
    assert name == "event: overview"
    overview = json.loads(data.removeprefix("data: "))
    assert overview["model"]["version"] == "v-test"
    assert overview["traffic"]["requests_in_window"] == 1


def test_events_are_published_with_contract_fields(make_client: ClientFactory) -> None:
    publisher = FakePublisher()
    client = make_client(kafka=fake_kafka(publisher))

    response = client.post("/api/v1/events", json=PAYLOAD, headers={"X-Request-ID": "req-1"})

    assert response.status_code == 202
    body = response.json()
    assert body == {
        "event_id": publisher.events[0].event_id,
        "request_id": "req-1",
        "topic": "prediction-events",
        "partition": 0,
        "offset": 0,
    }
    event = publisher.events[0]
    assert event.schema_version == "1.0"
    assert event.model_version == "v-test"
    assert event.features.amount == 860
    assert event.metadata.source == "api"


def test_events_report_unavailable_kafka(make_client: ClientFactory) -> None:
    client = make_client(kafka=fake_kafka(FakePublisher(available=False)))

    response = client.post("/api/v1/events", json=PAYLOAD)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "kafka_unavailable"
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "degraded"
    assert ready.json()["checks"]["kafka"] == "unavailable"


def test_events_disabled_without_bootstrap_servers(client: TestClient) -> None:
    response = client.post("/api/v1/events", json=PAYLOAD)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "events_disabled"
    assert "kafka" not in client.get("/ready").json()["checks"]


def test_overview_includes_kafka_lag_and_consumer_replicas(make_client: ClientFactory) -> None:
    members = (
        GroupMember(member_id="aaa", host="10.0.0.2", partitions=(0,)),
        GroupMember(member_id="bbb", host="10.0.0.3", partitions=(1,)),
    )
    snapshot = OffsetSnapshot(
        end={0: 120, 1: 80}, committed={0: 100}, group_state="Stable", members=members
    )
    client = make_client(
        kafka=fake_kafka(FakePublisher(), FakeOffsetSource([snapshot])),
        deployment_platform="docker-compose",
    )

    kafka = client.get("/api/v1/metrics/overview").json()["kafka"]

    assert kafka["status"] == "ok"
    assert kafka["partitions"] == 2
    assert kafka["lag"] == 100
    assert kafka["lag_by_partition"] == [{"partition": 0, "lag": 20}, {"partition": 1, "lag": 80}]
    assert "streampredict_kafka_consumer_lag 100.0" in client.get("/metrics").text

    infrastructure = client.get("/api/v1/metrics/overview").json()["infrastructure"]
    assert infrastructure["platform"] == "docker-compose"
    assert infrastructure["group_state"] == "Stable"
    assert infrastructure["consumer_replicas"] == 2
    assert infrastructure["replicas_history"] == [2]
    assert infrastructure["members"][1] == {
        "member_id": "bbb",
        "host": "10.0.0.3",
        "partitions": [1],
    }


def test_demo_events_channel_publishes_to_kafka(make_client: ClientFactory) -> None:
    publisher = FakePublisher()
    client = make_client(kafka=fake_kafka(publisher), demo_max_rps=30)

    started = client.post(
        "/api/v1/demo/traffic-spike",
        json={"profile": "standard", "channel": "events", "duration_seconds": 1},
    )
    assert started.status_code == 202
    assert started.json()["channel"] == "events"

    status = wait_for_state(client, {"completed", "failed"})
    summary = status["summary"]
    assert isinstance(summary, dict)
    assert summary["total_requests"] == len(publisher.events) > 0
    assert summary["errors"] == 0
    assert {event.metadata.source for event in publisher.events} == {"dashboard-demo"}
    assert {event.metadata.demo_session_id for event in publisher.events} == {status["session_id"]}


def test_demo_events_channel_requires_kafka(client: TestClient) -> None:
    response = client.post("/api/v1/demo/traffic-spike", json={"channel": "events"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "kafka_unavailable"
    assert client.get("/api/v1/demo/status").json()["state"] == "idle"
