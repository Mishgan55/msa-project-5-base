#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="shipment-exporter:local"
NAMESPACE="logistics-export"
MINIKUBE_BIN="${MINIKUBE_BIN:-minikube}"
MANUAL_JOB="shipment-exporter-manual-$(date +%Y%m%d%H%M%S)"

"$MINIKUBE_BIN" start --driver=docker
eval "$("$MINIKUBE_BIN" docker-env)"
docker build -t "$IMAGE" "$SCRIPT_DIR/app"
kubectl apply -f "$SCRIPT_DIR/k8s/namespace.yaml"

if kubectl -n "$NAMESPACE" get secret postgres-demo-auth >/dev/null 2>&1 && kubectl -n "$NAMESPACE" get secret shipment-exporter-auth >/dev/null 2>&1; then
  echo "Using existing local demo secrets in namespace $NAMESPACE"
elif ! kubectl -n "$NAMESPACE" get secret postgres-demo-auth >/dev/null 2>&1 && ! kubectl -n "$NAMESPACE" get secret shipment-exporter-auth >/dev/null 2>&1; then
  LOCAL_DEMO_PASSWORD="$(openssl rand -hex 16)"

  kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: postgres-demo-auth
  namespace: $NAMESPACE
  labels:
    environment: local-demo
type: Opaque
stringData:
  POSTGRES_USER: logistics_demo
  POSTGRES_PASSWORD: "$LOCAL_DEMO_PASSWORD"
  POSTGRES_DB: logistics
EOF

  kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: shipment-exporter-auth
  namespace: $NAMESPACE
  labels:
    environment: local-demo
type: Opaque
stringData:
  DB_USER: logistics_demo
  DB_PASSWORD: "$LOCAL_DEMO_PASSWORD"
EOF
else
  echo "Only one of the demo secrets exists. Remove the local demo namespace or recreate the missing secret consistently." >&2
  exit 1
fi

kubectl apply -f "$SCRIPT_DIR/k8s/postgres-init-configmap.yaml"
kubectl apply -f "$SCRIPT_DIR/k8s/postgres.yaml"
kubectl apply -f "$SCRIPT_DIR/k8s/exporter-configmap.yaml"
kubectl apply -f "$SCRIPT_DIR/k8s/exports-pvc.yaml"
kubectl apply -f "$SCRIPT_DIR/k8s/cronjob.yaml"
kubectl -n "$NAMESPACE" rollout status deployment/postgres --timeout=180s
kubectl -n "$NAMESPACE" create job "$MANUAL_JOB" --from=cronjob/shipment-exporter
kubectl -n "$NAMESPACE" wait --for=condition=complete "job/$MANUAL_JOB" --timeout=180s
for pod in $(kubectl -n "$NAMESPACE" get pods -l "job-name=$MANUAL_JOB" -o name); do
  echo "--- logs $pod ---"
  kubectl -n "$NAMESPACE" logs "$pod" || true
done
kubectl -n "$NAMESPACE" get cronjob,job,pod,pvc
