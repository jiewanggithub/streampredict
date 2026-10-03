"""Controller settings, read from CONTROLLER_* environment variables."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ControllerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CONTROLLER_", extra="ignore")

    mlflow_tracking_uri: str = "http://localhost:5000"
    model_name: str = "streampredict-demo"
    serving_url: str = "http://localhost:8001"
    # Shared with the serving service: the controller writes deployed versions here.
    deployed_repository: Path = Path("deployed_models")
    log_level: str = "INFO"

    # Pre-deploy gate: offline metrics recorded with the training run.
    min_auc: float = Field(default=0.70, ge=0, le=1)
    max_auc_regression: float = Field(default=0.02, ge=0, le=1)

    # Post-deploy gate: a fixed reference batch is scored by the candidate and the previous
    # champion, and live traffic to the candidate is watched for a soak period.
    reference_rows: int = Field(default=2_000, ge=100, le=100_000)
    reference_seed: int = 20_260_101
    max_psi: float = Field(default=0.25, gt=0)
    max_high_risk_rate_drop: float = Field(default=0.5, ge=0, le=1)
    max_p95_latency_ms: float = Field(default=50.0, gt=0)
    max_error_rate: float = Field(default=0.01, ge=0, le=1)
    soak_seconds: float = Field(default=10.0, ge=0, le=600)


@lru_cache
def get_settings() -> ControllerSettings:
    return ControllerSettings()
