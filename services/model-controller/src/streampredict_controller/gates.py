"""Release gates as pure functions, so every decision is testable without MLflow or serving.

Pre-deploy gates (contract, offline metrics) stop a release before it touches traffic.
The post-deploy gate scores a fixed batch of production-like inputs with the deployed candidate and
compares the output distribution with the one the same model produced on its own evaluation set
at training time (recorded in its metadata). Offline metrics share any bug in the training
pipeline, so they cannot reveal training/serving skew; this comparison can. Comparing against the
previous champion instead would wrongly penalise a better model whose scores are simply sharper.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

HIGH_RISK_THRESHOLD = 0.7
PSI_BINS = np.linspace(0.0, 1.0, 11)
PSI_FLOOR = 1e-4


@dataclass(frozen=True)
class GateResult:
    passed: bool
    reasons: list[str] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)


def contract_problems(candidate: dict[str, Any], serving: dict[str, Any]) -> list[str]:
    """Compare a candidate's serving config with the contract clients are built against."""
    problems = []
    for key in ("inputs", "outputs"):
        if candidate.get(key) != serving.get(key):
            problems.append(f"{key} differ: {candidate.get(key)} != {serving.get(key)}")
    candidate_features = candidate.get("parameters", {}).get("feature_names")
    serving_features = serving.get("parameters", {}).get("feature_names")
    if candidate_features != serving_features:
        problems.append(f"feature order differs: {candidate_features} != {serving_features}")
    return problems


def offline_gate(
    candidate: dict[str, float],
    champion: dict[str, float] | None,
    *,
    min_auc: float,
    max_regression: float,
) -> GateResult:
    auc = candidate.get("auc")
    if auc is None or math.isnan(auc):
        return GateResult(False, ["candidate has no recorded AUC"])
    reasons = []
    if auc < min_auc:
        reasons.append(f"AUC {auc:.3f} is below the minimum {min_auc:.3f}")
    champion_auc = (champion or {}).get("auc")
    if champion_auc is not None and auc < champion_auc - max_regression:
        reasons.append(f"AUC {auc:.3f} regresses from champion {champion_auc:.3f}")
    metrics = {"auc": auc} | ({"champion_auc": champion_auc} if champion_auc is not None else {})
    return GateResult(not reasons, reasons, metrics)


def fractions(scores: Sequence[float]) -> list[float]:
    counts, _ = np.histogram(np.asarray(scores, dtype=float), bins=PSI_BINS)
    return [float(c) / max(int(counts.sum()), 1) for c in counts]


def psi(expected: Sequence[float], actual: Sequence[float]) -> float:
    """Population Stability Index between two bin-fraction vectors (ten equal-width bins).

    Rule of thumb: < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 significant shift.
    """
    e_pct = np.maximum(np.asarray(expected, dtype=float), PSI_FLOOR)
    a_pct = np.maximum(np.asarray(actual, dtype=float), PSI_FLOOR)
    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def high_risk_rate(scores: Sequence[float]) -> float:
    values = np.asarray(scores, dtype=float)
    return float((values >= HIGH_RISK_THRESHOLD).mean()) if len(values) else 0.0


@dataclass(frozen=True)
class ProbeResult:
    scores: list[float]
    latencies_ms: list[float]
    errors: int
    requests: int

    @property
    def p95_ms(self) -> float:
        return float(np.percentile(self.latencies_ms, 95)) if self.latencies_ms else 0.0

    @property
    def error_rate(self) -> float:
        return self.errors / self.requests if self.requests else 1.0


@dataclass(frozen=True)
class ScoreProfile:
    """Output distribution a model produced on its evaluation set at training time."""

    fractions: list[float]
    high_risk_rate: float

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any]) -> "ScoreProfile | None":
        profile = metadata.get("score_profile")
        if not isinstance(profile, dict) or len(profile.get("fractions", [])) != len(PSI_BINS) - 1:
            return None
        return cls([float(x) for x in profile["fractions"]], float(profile["high_risk_rate"]))


def runtime_gate(
    candidate: ProbeResult,
    expected: ScoreProfile | None,
    *,
    live_errors: int,
    live_requests: int,
    max_psi: float,
    max_high_risk_rate_drop: float,
    max_p95_latency_ms: float,
    max_error_rate: float,
) -> GateResult:
    reasons = []
    candidate_high = high_risk_rate(candidate.scores)
    if expected is None:
        reasons.append("candidate has no recorded training score profile")
        shift, baseline_high = math.inf, 0.0
    else:
        shift = (
            psi(expected.fractions, fractions(candidate.scores)) if candidate.scores else math.inf
        )
        baseline_high = expected.high_risk_rate
    live_error_rate = live_errors / live_requests if live_requests else 0.0
    if candidate.error_rate > max_error_rate:
        reasons.append(f"probe error rate {candidate.error_rate:.1%}")
    if live_error_rate > max_error_rate:
        reasons.append(f"live error rate {live_error_rate:.1%} over {live_requests} requests")
    if candidate.p95_ms > max_p95_latency_ms:
        reasons.append(f"p95 latency {candidate.p95_ms:.1f} ms > {max_p95_latency_ms:.0f} ms")
    if expected is not None and shift > max_psi:
        reasons.append(f"score distribution shift PSI {shift:.2f} > {max_psi:.2f} vs training")
    if baseline_high > 0 and candidate_high < baseline_high * (1 - max_high_risk_rate_drop):
        reasons.append(
            f"high-risk rate {candidate_high:.1%} vs {baseline_high:.1%} expected from training"
        )
    metrics = {
        "psi": round(shift, 4) if math.isfinite(shift) else -1.0,
        "high_risk_rate": round(candidate_high, 4),
        "expected_high_risk_rate": round(baseline_high, 4),
        "p95_ms": round(candidate.p95_ms, 2),
        "probe_error_rate": round(candidate.error_rate, 4),
        "live_requests": float(live_requests),
        "live_error_rate": round(live_error_rate, 4),
    }
    return GateResult(not reasons, reasons, metrics)


def reference_batch(rows: int, seed: int) -> np.ndarray:
    """Production-like inputs drawn like the demo traffic (dollars, events/hour, km)."""
    rng = np.random.default_rng(seed)
    return np.stack(
        [
            np.minimum(rng.lognormal(5.5, 1.1, rows), 999_999),
            rng.integers(0, 41, rows).astype(np.float64),
            np.minimum(rng.exponential(300, rows), 19_999),
        ],
        axis=1,
    ).astype(np.float32)
