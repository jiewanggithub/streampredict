# Runbook: local Kubernetes (kind)

## Bring-up

```bash
make down       # Compose holds host ports 3000/8000/5001
make k8s-up     # kind cluster + metrics-server + KEDA, build and load images, deploy, wait
make k8s-status # pods, HPAs, ScaledObject, recent SuccessfulRescale events
make k8s-deploy # after code changes: rebuild, load, apply, restart, wait (undo on failure)
make k8s-down   # delete the cluster
```

Dashboard <http://localhost:3000>, API <http://localhost:8000/docs>, MLflow <http://localhost:5001>
(NodePorts mapped by `infra/kubernetes/kind-config.yaml`). The whole stack fits in Docker's default
7.7 GB (the node uses ~3.7 GB with 8 consumers).

| Path | Contents |
| --- | --- |
| `infra/kubernetes/base` | Namespace, ConfigMap/Secret, every workload, Services, HPAs, PDBs, RBAC |
| `infra/kubernetes/autoscaling` | KEDA ScaledObject, applied after Kafka and its topics are ready |
| `infra/prometheus` | Prometheus (pod discovery, cAdvisor, kube-state-metrics), shared rules; applied before the autoscalers |
| `infra/kubernetes/scripts` | `up.sh` (cluster + add-ons + images), `deploy.sh` (apply, roll out, undo on failure) |

## Autoscaling

| Workload | Scaler | Range | Signal |
| --- | --- | --- | --- |
| `consumer` | KEDA ScaledObject → HPA `keda-hpa-consumer` | 2–8 | Consumer-group lag, target 50 per pod; may double every 15 s, scales down after 60 s stable |
| `api` | KEDA ScaledObject → HPA `keda-hpa-api` | 2–4 | CPU 70 % of requests, or > 40 prediction RPS per pod (Prometheus) |
| `model-serving` | HPA | 2–4 | CPU 70 % of requests |

Consumers run with `CONSUMER_CONCURRENCY=1` and `CONSUMER_SIMULATED_WORK_MS=40` (~23 events/s per
pod, a stand-in for slow enrichment), so a 100 RPS spike outruns two pods and lag builds visibly.
Set the work to 0 for real throughput.

## Design notes

- **Deployed models volume.** `model-controller` writes `deployed-models`; serving pods mount it
  read-only. ReadWriteOnce is enough on one node; a multi-node cluster needs ReadWriteMany storage
  or an object-store sync.
- **Reloading every serving replica.** A Service only routes to ready pods, and a fresh serving pod
  is not ready until a model is loaded. The controller resolves the headless Service
  `model-serving-peers` (`publishNotReadyAddresses`) and calls each replica's load endpoint.
- **One demo session per cluster.** Gateways proxy demo controls to the single `demo-orchestrator`
  (`DEMO_ORCHESTRATOR_URL`), which sends synthetic traffic through the `api` Service.
- **Cluster-wide traffic numbers.** Gateway replicas add per-second counts to Redis; the overview
  reports cluster RPS and success rate (`traffic.scope: cluster`). Latency percentiles are sampled
  from the replica that answers.
- **Read-only cluster view.** The `api` ServiceAccount may list deployments, HPAs, events, and pod
  metrics in its namespace only.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| ScaledObject `READY False`, "error creating kafka client" | KEDA caches a failed client when Kafka was down at creation. `deploy.sh` applies it after Kafka; otherwise `kubectl -n keda rollout restart deploy/keda-operator`. |
| KEDA cannot resolve `kafka` | KEDA runs in the `keda` namespace; the scaler and Kafka's advertised listener use `kafka.streampredict.svc.cluster.local`. |
| HPA targets `<unknown>` | metrics-server is still warming up (first minute) or missing `--kubelet-insecure-tls` (patched by `up.sh`). |
| `mlflow` OOMKilled | MLflow 3 starts ~8 GenAI job runners (~220 MB each) unless `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`. |
| API pods not ready | `/ready` fails only when inference is unavailable: check `model-controller` logs (reconcile) and that MLflow has a champion (`registry-seed` job). |
| Rollout timed out | `deploy.sh` runs `kubectl rollout undo` for that deployment and exits non-zero; inspect with `kubectl -n streampredict describe deploy/<name>`. |
