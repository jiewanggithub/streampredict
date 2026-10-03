"""Unit tests for scoring, cache keys, demo rate shaping, and the rolling window."""

import asyncio

import pytest

from streampredict_api.cache import prediction_key
from streampredict_api.demo import target_rate
from streampredict_api.inference import MockInferenceClient, label_for
from streampredict_api.metrics import RollingWindow, percentile
from streampredict_api.schemas import PredictionFeatures

FEATURES = PredictionFeatures(amount=860, events_per_hour=4, distance_km=120)


def test_mock_inference_is_deterministic_and_bounded() -> None:
    client = MockInferenceClient("demo", "v1")
    first = asyncio.run(client.predict(FEATURES))
    second = asyncio.run(client.predict(FEATURES))
    extreme = asyncio.run(
        client.predict(PredictionFeatures(amount=999_999, events_per_hour=10_000, distance_km=0))
    )
    assert first == second
    assert first.label == "low_risk"
    assert extreme.label == "high_risk"
    assert 0 <= extreme.score <= 1


@pytest.mark.parametrize(
    ("score", "label"), [(0.1, "low_risk"), (0.4, "review"), (0.69, "review"), (0.7, "high_risk")]
)
def test_label_thresholds(score: float, label: str) -> None:
    assert label_for(score) == label


def test_prediction_key_is_versioned_and_does_not_leak_features() -> None:
    key_v1 = prediction_key("demo", "v1", FEATURES)
    key_v2 = prediction_key("demo", "v2", FEATURES)
    reordered = PredictionFeatures(distance_km=120, events_per_hour=4, amount=860)
    assert key_v1 != key_v2
    assert key_v1 == prediction_key("demo", "v1", reordered)
    assert key_v1.startswith("sp:v1:prediction:demo:v1:")
    assert "860" not in key_v1


def test_target_rate_never_exceeds_target() -> None:
    for profile in ("standard", "spike"):
        rates = [target_rate(profile, step / 100, 80) for step in range(101)]
        assert max(rates) <= 80
        assert min(rates) >= 0
    assert target_rate("spike", 0.5, 80) == 80
    assert target_rate("spike", 0.1, 80) == pytest.approx(12)


def test_percentile_nearest_rank() -> None:
    assert percentile([], 0.5) is None
    assert percentile([5.0, 1.0, 3.0, 2.0, 4.0], 0.5) == 3.0
    assert percentile([float(value) for value in range(1, 101)], 0.99) == 99.0


def test_rolling_window_expires_old_samples() -> None:
    now = [1000.0]
    window = RollingWindow(window_seconds=60, clock=lambda: now[0])
    window.record_prediction(10, True, "miss")
    window.record_prediction(2, True, "hit")
    window.record_prediction(50, False, "bypass")

    snapshot = window.snapshot()
    assert snapshot.requests_in_window == 3
    assert snapshot.errors_in_window == 1
    assert snapshot.hit_rate == 50.0
    assert snapshot.bypass_count == 1
    assert snapshot.success_rate == pytest.approx(66.667)
    assert snapshot.rps_history[-1] == 3

    now[0] += 61
    assert window.snapshot().requests_in_window == 0
