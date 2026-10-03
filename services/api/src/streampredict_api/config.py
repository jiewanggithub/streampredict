"""Runtime configuration loaded from environment variables and an optional `.env` file."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """API gateway settings. Field names map to upper-case environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        protected_namespaces=(),
    )

    app_env: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"
    cors_allow_origins: str = Field(
        default="http://localhost:3000",
        description="Comma-separated list of browser origins allowed to call the API.",
    )

    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl_seconds: int = Field(default=300, ge=1, le=86_400)
    redis_timeout_seconds: float = Field(default=0.05, gt=0, le=5)
    redis_circuit_open_seconds: float = Field(default=5.0, ge=0, le=300)

    # `serving` calls the model-serving service; `mock` scores in-process (tests, no-model dev).
    inference_backend: Literal["mock", "serving"] = "mock"
    serving_url: str = "http://localhost:8001"
    model_name: str = "streampredict-demo"
    # With the serving backend, `latest` pins the highest version available at startup.
    model_version: str = "development"
    inference_timeout_seconds: float = Field(default=1.0, gt=0, le=30)
    mock_inference_latency_ms: float = Field(default=15.0, ge=0, le=5_000)
    api_max_in_flight_predictions: int = Field(default=256, ge=1, le=10_000)
    metrics_stream_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    # Streams are recycled so idle tabs cannot hold connections forever; EventSource reconnects.
    metrics_stream_max_seconds: float = Field(default=300.0, gt=0, le=3_600)

    # Empty disables the event pipeline: /api/v1/events answers 503 and readiness omits Kafka.
    # Deployment controller (M6); empty disables release controls and deployment status.
    controller_url: str = ""

    # In-cluster only: namespace whose workloads, HPAs, and pod metrics the dashboard shows.
    kubernetes_namespace: str = ""

    # Shown on the dashboard's infrastructure panel: local, docker-compose, kubernetes.
    deployment_platform: str = "local"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_prediction_topic: str = "prediction-events"
    kafka_result_topic: str = "prediction-results"
    kafka_dead_letter_topic: str = "prediction-events-dlq"
    kafka_consumer_group: str = "streampredict-consumers"
    kafka_publish_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    kafka_reconnect_interval_seconds: float = Field(default=5.0, ge=0, le=300)

    demo_max_rps: int = Field(default=100, ge=1, le=1_000)
    demo_max_duration_seconds: int = Field(default=300, ge=1, le=300)
    # The in-process orchestrator runs exactly one session at a time.
    demo_max_concurrent_sessions: int = Field(default=1, ge=1, le=1)
    # Event-channel demos stay in cooling_down until consumer lag drains, at most this long.
    demo_recovery_timeout_seconds: float = Field(default=120, ge=0, le=600)
    # When set, demo endpoints proxy to this standalone orchestrator (multi-replica gateways).
    demo_orchestrator_url: str = ""
    # Orchestrator side: the gateway URL its synthetic traffic is sent to.
    demo_target_url: str = "http://localhost:8000"
    demo_control_token: SecretStr | None = None

    @field_validator("demo_control_token", mode="before")
    @classmethod
    def _empty_token_disables_protection(cls, value: object) -> object:
        return None if value == "" else value

    @property
    def cors_origins(self) -> list[str]:
        """Return the parsed CORS origin list."""
        return [origin.strip() for origin in self.cors_allow_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    """Return process-wide settings."""
    return Settings()
