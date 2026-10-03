#!/usr/bin/env bash
# Create (or reuse) the local kind cluster, install cluster add-ons, and deploy StreamPredict.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
CLUSTER=streampredict
METRICS_SERVER_VERSION=v0.9.0
KEDA_VERSION=2.21.0
APP_IMAGES=(api consumer model-serving model-controller mlflow dashboard)
THIRD_PARTY_IMAGES=(redis:7.4.7-alpine apache/kafka:4.3.1 postgres:18.6-alpine chrislusf/seaweedfs:4.48)

if docker compose -f "$ROOT/infra/docker/compose.yaml" ps -q 2>/dev/null | grep -q .; then
  echo "Docker Compose stack is running and holds ports 3000/8000/5001; run 'make down' first." >&2
  exit 1
fi

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config "$ROOT/infra/kubernetes/kind-config.yaml"
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

echo "==> metrics-server $METRICS_SERVER_VERSION (HPA CPU metrics, dashboard pod usage)"
kubectl apply -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/$METRICS_SERVER_VERSION/components.yaml" >/dev/null
# kind kubelets use self-signed serving certificates.
kubectl -n kube-system patch deployment metrics-server --type=json -p \
  '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' \
  >/dev/null 2>&1 || true

echo "==> KEDA $KEDA_VERSION (Kafka-lag autoscaling)"
kubectl apply --server-side --force-conflicts \
  -f "https://github.com/kedacore/keda/releases/download/v$KEDA_VERSION/keda-$KEDA_VERSION.yaml" >/dev/null
kubectl -n keda rollout status deployment/keda-operator --timeout=300s
kubectl -n keda rollout status deployment/keda-metrics-apiserver --timeout=300s

echo "==> building application images"
docker compose -f "$ROOT/infra/docker/compose.yaml" build "${APP_IMAGES[@]}"
echo "==> loading images into the cluster"
for image in "${APP_IMAGES[@]}"; do
  kind load docker-image "streampredict-$image:latest" --name "$CLUSTER"
done
for image in "${THIRD_PARTY_IMAGES[@]}"; do
  # Reuse locally pulled images when possible; otherwise the node pulls them itself.
  kind load docker-image "$image" --name "$CLUSTER" >/dev/null 2>&1 || true
done

"$ROOT/infra/kubernetes/scripts/deploy.sh"
