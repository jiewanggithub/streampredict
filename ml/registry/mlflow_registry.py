"""MLflow integration: log trained artifacts as runs and register them as model versions.

Conventions shared by training, the seed job, and the deployment controller:

- Training runs live in the `streampredict-training` experiment; each logs the serving artifact
  directory (`model.onnx`, `metadata.json`, `config.json`) under the `model` artifact path.
- Registered model versions carry tags: `profile`, `auc`, and `status`
  (`candidate` -> `champion` | `rejected` | `rolled_back` | `superseded`).
- The `champion` alias always points at the version serving production traffic.
- Every release attempt is a run in the `streampredict-deployments` experiment.
"""

import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from mlflow import MlflowClient
from mlflow.entities import Metric, Param, RunTag
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException

TRAINING_EXPERIMENT = "streampredict-training"
DEPLOYMENT_EXPERIMENT = "streampredict-deployments"
CHAMPION_ALIAS = "champion"
ARTIFACT_PATH = "model"


def ensure_experiment(client: MlflowClient, name: str) -> str:
    experiment = client.get_experiment_by_name(name)
    if experiment is not None:
        return str(experiment.experiment_id)
    return str(client.create_experiment(name))


def ensure_registered_model(client: MlflowClient, name: str) -> None:
    try:
        client.get_registered_model(name)
    except MlflowException:
        client.create_registered_model(
            name, description="StreamPredict demo risk model (ONNX, served by model-serving)."
        )


def _flatten(prefix: str, value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        flat: dict[str, str] = {}
        for key, item in value.items():
            flat.update(_flatten(f"{prefix}.{key}" if prefix else str(key), item))
        return flat
    return {prefix: str(value)}


def log_and_register(
    client: MlflowClient,
    model_name: str,
    config_path: Path,
    version_dir: Path,
    *,
    origin: str,
) -> ModelVersion:
    """Log one exported model version as a training run and register it.

    `origin` records how the artifact was produced (`trained` on this machine, or `seed` when a
    committed pre-trained artifact is imported for the demo).
    """
    metadata: dict[str, Any] = json.loads((version_dir / "metadata.json").read_text())
    experiment_id = ensure_experiment(client, TRAINING_EXPERIMENT)
    run = client.create_run(
        experiment_id,
        run_name=f"{model_name}-{metadata.get('profile', 'model')}",
        tags={"origin": origin, "model": model_name},
    )
    run_id = run.info.run_id
    now = int(time.time() * 1000)
    params = _flatten("", metadata.get("parameters", {}))
    params["features"] = ",".join(metadata.get("features", []))
    tags = {
        key: str(metadata[key])
        for key in ("profile", "data", "code_version", "framework", "trained_at")
        if metadata.get(key) is not None
    }
    client.log_batch(
        run_id,
        metrics=[
            Metric(key, float(value), now, 0)
            for key, value in metadata.get("metrics", {}).items()
            if isinstance(value, int | float)
        ],
        params=[Param(key, value) for key, value in params.items()],
        tags=[RunTag(key, value) for key, value in tags.items()],
    )
    with tempfile.TemporaryDirectory() as staging:
        bundle = Path(staging) / ARTIFACT_PATH
        bundle.mkdir()
        shutil.copy(version_dir / "model.onnx", bundle)
        shutil.copy(version_dir / "metadata.json", bundle)
        shutil.copy(config_path, bundle / "config.json")
        client.log_artifacts(run_id, str(bundle), artifact_path=ARTIFACT_PATH)
    client.set_terminated(run_id)

    ensure_registered_model(client, model_name)
    metrics = metadata.get("metrics", {})
    version = client.create_model_version(
        model_name,
        source=f"{run.info.artifact_uri}/{ARTIFACT_PATH}",
        run_id=run_id,
        tags={
            "profile": str(metadata.get("profile", "")),
            "auc": str(metrics.get("auc", "")),
            "status": "candidate",
            "origin": origin,
        },
        description=metadata.get("description") or f"{metadata.get('profile', '')} profile",
    )
    return version
