"""Inference client interface and the mock backend used until TorchServe lands (M5)."""

import asyncio
import math
from dataclasses import dataclass
from typing import Protocol

from .schemas import PredictionFeatures, RiskLabel

HIGH_RISK_THRESHOLD = 0.7
REVIEW_THRESHOLD = 0.4


@dataclass(frozen=True)
class InferenceResult:
    score: float
    label: RiskLabel


class InferenceError(Exception):
    """Raised when the inference backend cannot produce a prediction."""


class InferenceClient(Protocol):
    model_name: str
    model_version: str

    async def predict(self, features: PredictionFeatures) -> InferenceResult: ...

    async def ready(self) -> bool: ...


def label_for(score: float) -> RiskLabel:
    if score >= HIGH_RISK_THRESHOLD:
        return "high_risk"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "low_risk"


class MockInferenceClient:
    """Deterministic logistic scorer with configurable latency.

    It stands in for TorchServe so the API, cache, and dashboard can be exercised end to end.
    """

    def __init__(self, model_name: str, model_version: str, latency_seconds: float = 0.0) -> None:
        self.model_name = model_name
        self.model_version = model_version
        self._latency_seconds = latency_seconds

    async def predict(self, features: PredictionFeatures) -> InferenceResult:
        if self._latency_seconds:
            await asyncio.sleep(self._latency_seconds)
        logit = (
            -4.0
            + 0.9 * math.log1p(features.amount / 100)
            + 0.08 * features.events_per_hour
            + 0.0009 * features.distance_km
        )
        score = round(1 / (1 + math.exp(-logit)), 6)
        return InferenceResult(score=score, label=label_for(score))

    async def ready(self) -> bool:
        return True
