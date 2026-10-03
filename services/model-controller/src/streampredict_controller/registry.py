"""Model registry access (MLflow). Calls are blocking; the controller runs them in threads."""

import json
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from mlflow import MlflowClient
from mlflow.entities import Metric, Param, RunTag
from mlflow.exceptions import MlflowException

from ml.registry.mlflow_registry import (
    ARTIFACT_PATH,
    CHAMPION_ALIAS,
    DEPLOYMENT_EXPERIMENT,
    ensure_experiment,
)


@dataclass(frozen=True)
class VersionInfo:
    version: str
    profile: str
    description: str
    status: str
    metrics: dict[str, float]
    run_id: str | None
    created_at: float


@dataclass
class ReleaseRecord:
    version: str
    previous: str | None
    outcome: str  # promoted | rejected | rolled_back | manual_rollback
    stage: str
    reasons: list[str] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)
    started_at: float = 0.0
    finished_at: float = 0.0


class Registry(Protocol):
    def versions(self) -> list[VersionInfo]: ...

    def champion(self) -> str | None: ...

    def set_champion(self, version: str) -> None: ...

    def set_status(self, version: str, status: str, reason: str | None = None) -> None: ...

    def download(self, version: str, destination: Path) -> Path: ...

    def record_release(self, record: ReleaseRecord) -> None: ...

    def history(self, limit: int) -> list[ReleaseRecord]: ...


class MlflowRegistry:
    def __init__(self, tracking_uri: str, model_name: str) -> None:
        self._client = MlflowClient(tracking_uri=tracking_uri, registry_uri=tracking_uri)
        self._model = model_name

    def versions(self) -> list[VersionInfo]:
        found = self._client.search_model_versions(f"name='{self._model}'")
        infos = []
        for mv in found:
            metrics: dict[str, float] = {}
            if mv.run_id:
                metrics = dict(self._client.get_run(mv.run_id).data.metrics)
            infos.append(
                VersionInfo(
                    version=str(mv.version),
                    profile=mv.tags.get("profile", ""),
                    description=mv.description or "",
                    status=mv.tags.get("status", "candidate"),
                    metrics=metrics,
                    run_id=mv.run_id,
                    created_at=mv.creation_timestamp / 1000,
                )
            )
        return sorted(infos, key=lambda info: int(info.version))

    def champion(self) -> str | None:
        try:
            return str(self._client.get_model_version_by_alias(self._model, CHAMPION_ALIAS).version)
        except MlflowException:
            return None

    def set_champion(self, version: str) -> None:
        self._client.set_registered_model_alias(self._model, CHAMPION_ALIAS, version)

    def set_status(self, version: str, status: str, reason: str | None = None) -> None:
        self._client.set_model_version_tag(self._model, version, "status", status)
        if reason is not None:
            self._client.set_model_version_tag(self._model, version, "status_reason", reason[:500])

    def download(self, version: str, destination: Path) -> Path:
        mv = self._client.get_model_version(self._model, version)
        if not mv.run_id:
            raise ValueError(f"version {version} has no source run")
        path = self._client.download_artifacts(mv.run_id, ARTIFACT_PATH, str(destination))
        return Path(path)

    def record_release(self, record: ReleaseRecord) -> None:
        experiment_id = ensure_experiment(self._client, DEPLOYMENT_EXPERIMENT)
        run = self._client.create_run(
            experiment_id,
            run_name=f"release-v{record.version}",
            start_time=int(record.started_at * 1000),
            tags={"model": self._model},
        )
        now = int(record.finished_at * 1000)
        self._client.log_batch(
            run.info.run_id,
            metrics=[Metric(k, float(v), now, 0) for k, v in record.metrics.items()],
            params=[
                Param("version", record.version),
                Param("previous_version", record.previous or ""),
            ],
            tags=[
                RunTag("outcome", record.outcome),
                RunTag("stage", record.stage),
                RunTag("reasons", json.dumps(record.reasons)[:5000]),
            ],
        )
        self._client.set_terminated(run.info.run_id, end_time=now)

    def history(self, limit: int) -> list[ReleaseRecord]:
        experiment = self._client.get_experiment_by_name(DEPLOYMENT_EXPERIMENT)
        if experiment is None:
            return []
        runs = self._client.search_runs(
            [experiment.experiment_id],
            filter_string=f"tags.model = '{self._model}'",
            max_results=limit,
            order_by=["attributes.start_time DESC"],
        )
        return [
            ReleaseRecord(
                version=run.data.params.get("version", ""),
                previous=run.data.params.get("previous_version") or None,
                outcome=run.data.tags.get("outcome", ""),
                stage=run.data.tags.get("stage", ""),
                reasons=json.loads(run.data.tags.get("reasons", "[]")),
                metrics=dict(run.data.metrics),
                started_at=run.info.start_time / 1000,
                finished_at=(run.info.end_time or run.info.start_time) / 1000,
            )
            for run in runs
        ]


def read_bundle(path: Path) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    """Return (serving config, model file, training metadata) from a downloaded bundle."""
    config = json.loads((path / "config.json").read_text())
    metadata_path = path / "metadata.json"
    metadata = json.loads(metadata_path.read_text()) if metadata_path.is_file() else {}
    return config, path / "model.onnx", metadata


def scratch_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="release-")).resolve()


def utc_now() -> float:
    return time.time()
