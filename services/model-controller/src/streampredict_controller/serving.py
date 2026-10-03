"""Deployment side of the controller: the shared model repository and the serving admin API.

The repository directory is the deployed state. Installing a version copies its artifacts into
`<repo>/<model>/<version>/`; `<repo>/<model>/serving.json` names the default version that
unversioned requests (and therefore the gateway) use. Every change is followed by a repository
load call, so serving picks it up without a restart.
"""

import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Protocol

import httpx
import numpy as np

from .gates import ProbeResult

PROBE_CHUNK_ROWS = 64


class ServingError(Exception):
    pass


class ServingAdmin(Protocol):
    async def contract(self) -> dict[str, Any] | None: ...

    async def installed(self) -> list[str]: ...

    async def install(self, version: str, config: dict[str, Any], model_file: Path) -> None: ...

    async def uninstall(self, version: str) -> None: ...

    async def set_default(self, version: str) -> None: ...

    async def default(self) -> str | None: ...

    async def probe(self, version: str, batch: np.ndarray) -> ProbeResult: ...

    async def counters(self, version: str) -> tuple[int, int]: ...


class FileServingAdmin:
    def __init__(
        self,
        repository: Path,
        model_name: str,
        serving_url: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._model_dir = repository / model_name
        self._model = model_name
        self._client = httpx.AsyncClient(base_url=serving_url, timeout=10, transport=transport)

    async def close(self) -> None:
        await self._client.aclose()

    async def contract(self) -> dict[str, Any] | None:
        path = self._model_dir / "config.json"
        return json.loads(path.read_text()) if path.is_file() else None

    async def installed(self) -> list[str]:
        if not self._model_dir.is_dir():
            return []
        return sorted(
            (p.name for p in self._model_dir.iterdir() if p.is_dir() and p.name.isdigit()),
            key=int,
        )

    async def install(self, version: str, config: dict[str, Any], model_file: Path) -> None:
        self._model_dir.mkdir(parents=True, exist_ok=True)
        config_path = self._model_dir / "config.json"
        if not config_path.exists():
            _atomic_write(config_path, json.dumps(config, indent=2) + "\n")
        target = self._model_dir / version
        staging = self._model_dir / f".staging-{version}"
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir()
        shutil.copy(model_file, staging / "model.onnx")
        metadata = model_file.with_name("metadata.json")
        if metadata.exists():
            shutil.copy(metadata, staging / "metadata.json")
        shutil.rmtree(target, ignore_errors=True)
        staging.rename(target)  # serving never sees a half-copied version directory
        await self._reload()

    async def uninstall(self, version: str) -> None:
        shutil.rmtree(self._model_dir / version, ignore_errors=True)
        await self._reload()

    async def set_default(self, version: str) -> None:
        _atomic_write(
            self._model_dir / "serving.json", json.dumps({"default_version": version}) + "\n"
        )
        await self._reload()

    async def default(self) -> str | None:
        path = self._model_dir / "serving.json"
        if not path.is_file():
            return None
        value = json.loads(path.read_text()).get("default_version")
        return str(value) if value is not None else None

    async def _reload(self) -> None:
        try:
            response = await self._client.post(f"/v2/repository/models/{self._model}/load")
        except httpx.HTTPError as exc:
            raise ServingError(f"serving unreachable: {exc!r}") from exc
        if response.status_code != 200:
            raise ServingError(f"serving load failed ({response.status_code}): {response.text}")

    async def probe(self, version: str, batch: np.ndarray) -> ProbeResult:
        scores: list[float] = []
        latencies: list[float] = []
        errors = requests = 0
        path = f"/v2/models/{self._model}/versions/{version}/infer"
        for start in range(0, len(batch), PROBE_CHUNK_ROWS):
            chunk = batch[start : start + PROBE_CHUNK_ROWS]
            body = {
                "inputs": [
                    {
                        "name": "features",
                        "shape": list(chunk.shape),
                        "datatype": "FP32",
                        "data": chunk.ravel().tolist(),
                    }
                ]
            }
            requests += 1
            started = time.perf_counter()
            try:
                response = await self._client.post(path, json=body)
            except httpx.HTTPError:
                errors += 1
                continue
            latencies.append((time.perf_counter() - started) * 1000)
            if response.status_code != 200:
                errors += 1
                continue
            scores.extend(float(x) for x in response.json()["outputs"][0]["data"])
        return ProbeResult(scores=scores, latencies_ms=latencies, errors=errors, requests=requests)

    async def counters(self, version: str) -> tuple[int, int]:
        """Return (errors, total) inference requests the serving service saw for a version."""
        response = await self._client.get("/metrics")
        errors = total = 0
        for line in response.text.splitlines():
            if not line.startswith("streampredict_serving_requests_total{"):
                continue
            labels, _, value = line.rpartition(" ")
            if f'model="{self._model}"' in labels and f'version="{version}"' in labels:
                count = int(float(value))
                total += count
                if 'outcome="error"' in labels:
                    errors += count
        return errors, total


def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(content)
    os.replace(tmp, path)
