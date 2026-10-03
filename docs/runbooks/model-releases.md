# Runbook: model releases and rollback

## Moving parts

| Component | Role |
| --- | --- |
| MLflow (`:5001` on the host) | Training runs, the model registry, deployment records. Postgres holds metadata; SeaweedFS (S3 API) holds artifacts, proxied by MLflow. |
| `model-controller` | Release authority. Reads the registry, writes the deployed repository, runs the gates, and rolls back. |
| `deployed-models` volume | Deployed state: `<model>/<version>/model.onnx` plus `<model>/serving.json` naming the default version. Shared with `model-serving`. |
| `model-serving` | Serves every installed version; unversioned requests go to the default version. |
| Gateway (`api`) | With `MODEL_VERSION=latest`, follows the serving default every 2 s; proxies release requests. |

The `champion` alias in MLflow is the source of truth. On startup the controller reconciles the
volume to it, so wiping the volume only causes a re-download.

## Releasing a version

From the dashboard (**Model releases** panel) or:

```bash
curl -X POST localhost:8000/api/v1/models/releases -H 'Content-Type: application/json' \
  -d '{"version": "3"}'          # add -H 'X-Demo-Token: …' when DEMO_CONTROL_TOKEN is set
```

| Stage | Check | On failure |
| --- | --- | --- |
| validating | Serving contract (inputs, outputs, feature order) matches the deployed model | `rejected`, traffic untouched |
| validating | Offline AUC ≥ `CONTROLLER_MIN_AUC` (0.70) and within `CONTROLLER_MAX_AUC_REGRESSION` (0.02) of the champion | `rejected`, traffic untouched |
| deploying | Artifact installed next to the champion and loaded | release fails, champion keeps serving |
| verifying | Default switched to the candidate. A fixed reference batch of production-like inputs is scored; for `CONTROLLER_SOAK_SECONDS` (10 s) live traffic is watched | automatic rollback |
| promoted | `champion` alias moves; the previous champion stays loaded as a warm standby | — |

The verification gate compares the candidate's output distribution with the distribution **the
same model produced on its evaluation set at training time** (`score_profile` in its metadata):

- PSI above `CONTROLLER_MAX_PSI` (0.25), or
- high-risk rate below half of the training-time rate, or
- p95 probe latency above `CONTROLLER_MAX_P95_LATENCY_MS` (50 ms), or
- probe or live error rate above `CONTROLLER_MAX_ERROR_RATE` (1 %).

Offline metrics are computed by the training pipeline, so they share its bugs. Comparing against
the previous champion would instead penalise a better model whose scores are sharper.

Rollback is a pointer flip (`serving.json`): the old version is still loaded, so there is no cold
start. The failed version is unloaded and tagged `rolled_back` with the reasons.

## Manual rollback

```bash
curl -X POST localhost:8000/api/v1/models/rollback
```

Returns to the champion that the most recent promotion replaced.

## Demo versions

| Version | Profile | Offline AUC | Behaviour |
| --- | --- | --- | --- |
| v1 | baseline | 0.72 | Initial champion |
| v2 | improved | 0.78 | Passes every gate |
| v3 | skewed | 0.78 | Trained on amounts in cents while serving sends dollars: offline metrics look healthy, production high-risk rate collapses → rolled back |

Register a newly trained version: `python -m ml.training.train --version 4 --repository <dir>`
exports it, and `ml.registry.mlflow_registry.log_and_register` (as used by
`python -m streampredict_controller.seed --repository <dir>`) logs and registers it.

## Investigating a release

- Dashboard: **Model releases** shows each attempt with its reasons and gate metrics.
- MLflow: experiment `streampredict-deployments` has one run per attempt (`outcome`, `reasons`,
  `psi`, `high_risk_rate`, `p95_ms`, …); registry version tags carry `status` and `status_reason`.
- Controller: `GET :8000/models/streampredict-demo/deployment` inside the network, and its logs.
