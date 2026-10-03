"""Model-serving settings, read from SERVING_* environment variables."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SERVING_", extra="ignore")

    model_repository: Path = Path("model_repository")
    # `all` loads every version found; `latest` only the highest (like Triton's version_policy).
    version_policy: Literal["all", "latest"] = "all"
    # Overrides for the per-model dynamic batching settings in config.json.
    max_batch_size: int | None = Field(default=None, ge=1, le=4_096)
    max_queue_delay_ms: float | None = Field(default=None, ge=0, le=1_000)
    # Hard cap on rows per request, independent of batching.
    max_request_rows: int = Field(default=1_024, ge=1, le=65_536)
    # ONNX Runtime threads per session; small values keep many replicas per node efficient.
    intra_op_threads: int = Field(default=1, ge=1, le=64)
    log_level: str = "INFO"


@lru_cache
def get_settings() -> ServingSettings:
    return ServingSettings()
