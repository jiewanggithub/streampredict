"""Train the demo risk model on synthetic data and export it into a model repository.

    python -m ml.training.train --version 1 --profile baseline
    python -m ml.training.train --version 2 --profile improved

The model is deliberately small: it exists to exercise the serving platform (packaging, versioning,
batching, monitoring, rollback), not to be a good fraud model. Feature transforms and
standardization are part of the exported graph, so serving only feeds raw features in order.

Output layout (KServe/Triton-style repository):

    <repository>/<model>/config.json          serving signature, shared by all versions
    <repository>/<model>/<version>/model.onnx
    <repository>/<model>/<version>/metadata.json  metrics, parameters, data seed, code version
"""

import argparse
import json
import math
import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import onnx
import torch
from torch import nn

FEATURES = ("amount", "events_per_hour", "distance_km")
DEFAULT_REPOSITORY = Path(__file__).resolve().parents[2] / "services/model-serving/model_repository"
DEFAULT_MODEL = "streampredict-demo"


@dataclass(frozen=True)
class Profile:
    samples: int
    hidden: int
    epochs: int
    learning_rate: float


PROFILES = {
    # A weaker first version, so v2 shows a measurable improvement.
    "baseline": Profile(samples=20_000, hidden=4, epochs=2, learning_rate=0.01),
    "improved": Profile(samples=120_000, hidden=32, epochs=12, learning_rate=0.005),
}


def synthetic_dataset(samples: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Transactions drawn like the demo load generator, labelled by a hidden noisy rule."""
    rng = np.random.default_rng(seed)
    amount = np.minimum(rng.lognormal(5.5, 1.1, samples), 999_999)
    events = rng.integers(0, 41, samples).astype(np.float64)
    distance = np.minimum(rng.exponential(300, samples), 19_999)
    logit = (
        -4.2
        + 0.9 * np.log1p(amount / 100)
        + 0.08 * events
        + 0.0009 * distance
        + 0.6 * (events > 25) * (distance > 800)  # an interaction a linear model would miss
        + rng.normal(0, 0.5, samples)
    )
    labels = (rng.random(samples) < 1 / (1 + np.exp(-logit))).astype(np.float32)
    features = np.stack([amount, events, distance], axis=1).astype(np.float32)
    return features, labels


class RiskModel(nn.Module):
    mean: torch.Tensor
    std: torch.Tensor

    def __init__(self, hidden: int, mean: torch.Tensor, std: torch.Tensor) -> None:
        super().__init__()
        self.register_buffer("mean", mean)
        self.register_buffer("std", std)
        self.net = nn.Sequential(nn.Linear(3, hidden), nn.ReLU(), nn.Linear(hidden, 1))

    @staticmethod
    def transform(features: torch.Tensor) -> torch.Tensor:
        amount, events, distance = features.unbind(dim=1)
        return torch.stack([torch.log1p(amount), events / 10, torch.log1p(distance)], dim=1)

    def forward(self, features: torch.Tensor) -> torch.Tensor:
        x = (self.transform(features) - self.mean) / self.std
        return torch.sigmoid(self.net(x))


def roc_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """Rank-based ROC AUC (Mann-Whitney U), without a scikit-learn dependency."""
    order = np.argsort(scores)
    ranks = np.empty(len(scores))
    ranks[order] = np.arange(1, len(scores) + 1)
    positives = labels == 1
    n_pos, n_neg = int(positives.sum()), int((~positives).sum())
    return float((ranks[positives].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def train(profile: Profile, seed: int) -> tuple[RiskModel, dict[str, float]]:
    torch.manual_seed(seed)
    features, labels = synthetic_dataset(profile.samples, seed)
    split = int(0.8 * len(features))
    x_train, x_test = torch.from_numpy(features[:split]), torch.from_numpy(features[split:])
    y_train, y_test = torch.from_numpy(labels[:split]), labels[split:]

    transformed = RiskModel.transform(x_train)
    model = RiskModel(profile.hidden, transformed.mean(0), transformed.std(0))
    optimizer = torch.optim.Adam(model.parameters(), lr=profile.learning_rate)
    loss_fn = nn.BCELoss()
    for _ in range(profile.epochs):
        permutation = torch.randperm(len(x_train))
        for start in range(0, len(x_train), 512):
            batch = permutation[start : start + 512]
            optimizer.zero_grad()
            loss = loss_fn(model(x_train[batch]).squeeze(1), y_train[batch])
            loss.backward()
            optimizer.step()

    model.eval()
    with torch.no_grad():
        scores = model(x_test).squeeze(1).numpy()
    clipped = np.clip(scores, 1e-7, 1 - 1e-7)
    metrics = {
        "auc": round(roc_auc(y_test, scores), 4),
        "log_loss": round(
            float(-np.mean(y_test * np.log(clipped) + (1 - y_test) * np.log(1 - clipped))), 4
        ),
        "accuracy": round(float(np.mean((scores >= 0.5) == y_test)), 4),
        "positive_rate": round(float(y_test.mean()), 4),
        "test_samples": len(y_test),
    }
    return model, metrics


def export(model: RiskModel, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    example = torch.tensor([[860.0, 4.0, 120.0], [5000.0, 30.0, 2000.0]])
    torch.onnx.export(
        model,
        (example,),
        str(path),
        input_names=["features"],
        output_names=["probability"],
        dynamic_shapes={"features": {0: torch.export.Dim("batch", min=1, max=4096)}},
        opset_version=18,
        dynamo=True,
        external_data=False,
    )
    onnx.checker.check_model(onnx.load(str(path)))


def git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def write_config(model_dir: Path, name: str) -> None:
    config = {
        "name": name,
        "platform": "onnxruntime_onnx",
        "max_batch_size": 64,
        "dynamic_batching": {"max_queue_delay_microseconds": 2000},
        "inputs": [{"name": "features", "datatype": "FP32", "shape": [-1, len(FEATURES)]}],
        "outputs": [{"name": "probability", "datatype": "FP32", "shape": [-1, 1]}],
        "parameters": {"feature_names": list(FEATURES)},
    }
    (model_dir / "config.json").write_text(json.dumps(config, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--version", required=True, type=int, help="positive integer version")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="improved")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--repository", type=Path, default=DEFAULT_REPOSITORY)
    args = parser.parse_args()
    if args.version < 1:
        parser.error("--version must be >= 1")

    profile = PROFILES[args.profile]
    model, metrics = train(profile, args.seed)
    model_dir = args.repository / args.model
    version_dir = model_dir / str(args.version)
    export(model, version_dir / "model.onnx")
    write_config(model_dir, args.model)
    metadata = {
        "model": args.model,
        "version": str(args.version),
        "profile": args.profile,
        "parameters": {**asdict(profile), "seed": args.seed},
        "metrics": metrics,
        "features": list(FEATURES),
        "data": f"synthetic(seed={args.seed}, samples={profile.samples})",
        "code_version": git_commit(),
        "trained_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "framework": f"torch {torch.__version__}",
    }
    (version_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    finite = {k: v for k, v in metrics.items() if not (isinstance(v, float) and math.isnan(v))}
    print(json.dumps({"version": args.version, "profile": args.profile, **finite}))


if __name__ == "__main__":
    main()
