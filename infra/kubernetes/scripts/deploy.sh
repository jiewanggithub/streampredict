#!/usr/bin/env bash
# Apply the manifests and wait for every rollout. A rollout that does not become ready within its
# progress deadline is undone, so a bad image or config never stays half-deployed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NS=streampredict

kubectl apply -k "$ROOT/infra/kubernetes/base"
echo "==> waiting for Kafka and its topics before enabling lag-based autoscaling"
kubectl -n "$NS" rollout status statefulset/kafka --timeout=300s
kubectl -n "$NS" wait --for=condition=complete job/kafka-init --timeout=300s
kubectl apply -k "$ROOT/infra/kubernetes/autoscaling"
# Pods only pick up a reloaded image when the template changes; restart app workloads so a
# rebuilt :latest image is used.
if [[ "${RESTART:-1}" == "1" ]]; then
  kubectl -n "$NS" rollout restart deployment/api deployment/consumer deployment/model-serving \
    deployment/model-controller deployment/dashboard deployment/mlflow \
    deployment/demo-orchestrator >/dev/null 2>&1 || true
fi

failed=()
for deployment in redis mlflow model-serving model-controller api consumer demo-orchestrator dashboard; do
  echo "==> waiting for $deployment"
  if ! kubectl -n "$NS" rollout status "deployment/$deployment" --timeout=300s; then
    echo "!! $deployment did not become ready; rolling back" >&2
    kubectl -n "$NS" rollout undo "deployment/$deployment" || true
    failed+=("$deployment")
  fi
done

kubectl -n "$NS" get pods -o wide
if ((${#failed[@]})); then
  echo "Rolled back: ${failed[*]}" >&2
  exit 1
fi
echo
echo "Dashboard http://localhost:3000 · API http://localhost:8000/docs · MLflow http://localhost:5001"
