#!/usr/bin/env bash
# deploy.sh — Build images and deploy the full weather observability platform to Minikube.
#
# Usage:
#   ./k8s/deploy.sh           # deploy everything
#   ./k8s/deploy.sh --delete  # tear down the whole namespace
#
# Prerequisites:
#   - minikube start  (already running)
#   - kubectl configured to talk to minikube

set -euo pipefail

NAMESPACE="weather"
K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$K8S_DIR")"

# ── Helpers ────────────────────────────────────────────────────────────────────
info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[ OK ]\033[0m  $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }

# ── Tear-down mode ─────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--delete" ]]; then
  info "Deleting all resources in namespace '$NAMESPACE'..."
  kubectl delete namespace "$NAMESPACE" --ignore-not-found
  ok "Done. Namespace '$NAMESPACE' removed."
  exit 0
fi

# ── Step 1: Point Docker CLI at Minikube's daemon ─────────────────────────────
info "Switching Docker context to Minikube's daemon..."
eval "$(minikube docker-env)"
ok "Docker now points at Minikube's daemon."

# ── Step 2: Build custom images inside Minikube ───────────────────────────────
info "Building weather-station-mock:latest..."
docker build -t weather-station-mock:latest "$PROJECT_ROOT/weather-station-mock"

info "Building central-station:latest..."
docker build -t central-station:latest "$PROJECT_ROOT/central-station"

info "Building parquet-es-bridge:latest..."
docker build -t parquet-es-bridge:latest "$PROJECT_ROOT/parquet-es-bridge"

ok "All custom images built inside Minikube."

# ── Step 3: Apply manifests in dependency order ───────────────────────────────

info "Creating namespace..."
kubectl apply -f "$K8S_DIR/00-namespace.yaml"

# --- Infrastructure: Kafka ---
info "Deploying Kafka..."
kubectl apply -f "$K8S_DIR/kafka/"
info "Waiting for Kafka rollout..."
kubectl rollout status deployment/kafka -n "$NAMESPACE" --timeout=300s
ok "Kafka ready."

# --- Infrastructure: Elasticsearch ---
info "Deploying Elasticsearch..."
kubectl apply -f "$K8S_DIR/elasticsearch/"
info "Waiting for Elasticsearch to be ready..."
info "(The elasticsearch:9.4.0 image is ~1.2 GB — first pull can take several minutes on a slow connection)"
info "Tip: watch pull progress with:  kubectl describe pod -n $NAMESPACE -l app=elasticsearch | grep -A5 Events"
kubectl rollout status deployment/elasticsearch -n "$NAMESPACE" --timeout=600s
ok "Elasticsearch ready."

# --- Infrastructure: Kibana ---
info "Deploying Kibana..."
kubectl apply -f "$K8S_DIR/kibana/"

# --- Infrastructure: Kafka UI ---
info "Deploying Kafka UI..."
kubectl apply -f "$K8S_DIR/kafka-ui/"

# --- Application: Parquet → ES Bridge (PVC must exist before central-station mounts it) ---
info "Deploying Parquet-ES Bridge (creates shared parquet-pvc)..."
kubectl apply -f "$K8S_DIR/parquet-es-bridge/"

# --- Application: Central Station ---
info "Deploying Central Station..."
kubectl apply -f "$K8S_DIR/central-station/"
info "Waiting for Central Station rollout..."
kubectl rollout status deployment/central-station -n "$NAMESPACE" --timeout=300s
ok "Central Station ready."

# --- Data: Weather Stations (random mode, 1-5) ---
info "Deploying weather stations 1-5 (random)..."
kubectl apply -f "$K8S_DIR/weather-stations/"

# --- Data: Weather Stations (open-meteo mode, 6-10) ---
info "Deploying weather stations 6-10 (open-meteo)..."
kubectl apply -f "$K8S_DIR/weather-stations-api/"

# ── Step 4: Show status ────────────────────────────────────────────────────────
echo ""
ok "All manifests applied! Pod status:"
kubectl get pods -n "$NAMESPACE" -o wide

echo ""
info "─── Access URLs ────────────────────────────────────────────────────────"
info "Kafka UI:"
echo "  minikube service kafka-ui -n $NAMESPACE --url"
info "Kibana:"
echo "  minikube service kibana -n $NAMESPACE --url"
info "Elasticsearch:"
echo "  minikube service elasticsearch -n $NAMESPACE --url"
info "Central Station API:"
echo "  minikube service central-station -n $NAMESPACE --url"

echo ""
info "─── Useful commands ─────────────────────────────────────────────────────"
echo "  kubectl get pods -n $NAMESPACE -w"
echo "  kubectl logs -n $NAMESPACE deploy/central-station -f"
echo "  kubectl logs -n $NAMESPACE deploy/parquet-es-bridge -f"
echo "  kubectl logs -n $NAMESPACE deploy/weather-station-1 -f"
