"""Model repository layout: `<root>/<model>/config.json` and `<root>/<model>/<version>/model.onnx`.

The layout matches Triton and KServe model repositories, so the same directory could be served by
either if the platform ever outgrows this service.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class TensorSpec(BaseModel):
    name: str
    datatype: str
    shape: list[int]


class DynamicBatching(BaseModel):
    max_queue_delay_microseconds: int = Field(default=2_000, ge=0)


class ModelConfig(BaseModel):
    name: str
    platform: str = "onnxruntime_onnx"
    max_batch_size: int = Field(default=64, ge=1)
    dynamic_batching: DynamicBatching = Field(default_factory=DynamicBatching)
    inputs: list[TensorSpec]
    outputs: list[TensorSpec]
    parameters: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True)
class VersionSource:
    version: str
    model_path: Path
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ModelSource:
    config: ModelConfig
    versions: list[VersionSource]
    # Version that requests without an explicit version go to. Written by the deployment
    # controller to `<model>/serving.json`; None means "highest loaded version".
    default_version: str | None = None


class RepositoryError(Exception):
    pass


def discover(root: Path) -> dict[str, ModelSource]:
    """Return every model with a readable config and at least one numeric version directory."""
    if not root.is_dir():
        raise RepositoryError(f"model repository {root} does not exist")
    models: dict[str, ModelSource] = {}
    for model_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        config_path = model_dir / "config.json"
        if not config_path.is_file():
            continue
        config = ModelConfig.model_validate_json(config_path.read_text())
        if config.name != model_dir.name:
            raise RepositoryError(f"{config_path}: name {config.name!r} != directory name")
        versions = [
            VersionSource(
                version=version_dir.name,
                model_path=version_dir / "model.onnx",
                metadata=_read_metadata(version_dir / "metadata.json"),
            )
            for version_dir in model_dir.iterdir()
            if version_dir.is_dir()
            and version_dir.name.isdigit()
            and (version_dir / "model.onnx").is_file()
        ]
        if versions:
            versions.sort(key=lambda v: int(v.version))
            models[config.name] = ModelSource(
                config=config,
                versions=versions,
                default_version=_read_default(model_dir / "serving.json"),
            )
    return models


def _read_default(path: Path) -> str | None:
    if not path.is_file():
        return None
    value = json.loads(path.read_text()).get("default_version")
    return str(value) if value is not None else None


def _read_metadata(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text())
    return data if isinstance(data, dict) else {}
