"""Seed the registry with the committed demo versions (idempotent).

    python -m streampredict_controller.seed --repository ml/artifacts/model_repository

Imports each `<model>/<version>/` directory as a training run plus a registered model version, in
version order, and makes version 1 the champion if none is set. Artifacts trained locally can be
registered the same way with `python -m ml.training.train --register`.
"""

import argparse
import logging
import time
from pathlib import Path

from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException

from ml.registry.mlflow_registry import CHAMPION_ALIAS, log_and_register

from .config import get_settings
from .logs import configure_logging

logger = logging.getLogger("streampredict.seed")


def seed(client: MlflowClient, model: str, repository: Path) -> None:
    model_dir = repository / model
    seeded = {
        mv.tags.get("seed_version")
        for mv in _versions(client, model)
        if mv.tags.get("seed_version") is not None
    }
    for version_dir in sorted(
        (p for p in model_dir.iterdir() if p.is_dir() and p.name.isdigit()),
        key=lambda p: int(p.name),
    ):
        if version_dir.name in seeded:
            continue
        mv = log_and_register(client, model, model_dir / "config.json", version_dir, origin="seed")
        client.set_model_version_tag(model, mv.version, "seed_version", version_dir.name)
        logger.info(
            "Registered seed version", extra={"seed": version_dir.name, "version": mv.version}
        )
    try:
        client.get_model_version_by_alias(model, CHAMPION_ALIAS)
    except MlflowException:
        first = min(_versions(client, model), key=lambda mv: int(mv.version))
        client.set_registered_model_alias(model, CHAMPION_ALIAS, first.version)
        client.set_model_version_tag(model, first.version, "status", "champion")
        logger.info("Initial champion set", extra={"version": first.version})


def _versions(client: MlflowClient, model: str) -> list[ModelVersion]:
    try:
        return list(client.search_model_versions(f"name='{model}'"))
    except MlflowException:
        return []


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--repository", type=Path, default=Path("ml/artifacts/model_repository"))
    parser.add_argument("--wait-seconds", type=float, default=120)
    args = parser.parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    client = MlflowClient(tracking_uri=settings.mlflow_tracking_uri)
    deadline = time.monotonic() + args.wait_seconds
    while True:
        try:
            client.search_experiments(max_results=1)
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    seed(client, settings.model_name, args.repository)


if __name__ == "__main__":
    main()
