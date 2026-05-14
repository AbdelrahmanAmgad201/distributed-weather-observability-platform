#!/usr/bin/env bash
# deploy.sh — Deploy the full weather observability platform to Minikube.
#
# Usage:
#   ./k8s/deploy.sh          # deploy everything
#   ./k8s/deploy.sh --delete  # tear down everything
#
# Prerequisites:
#   - minikube start
#   - Images built inside Minikube's Docker daemon (see step 1 below)

set -euo pipefail

NAMESPACE="weather"
K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# ── Step 1: point Docker CLI at Minikube's daemon ──────────────────────────────
info "Configuring shell to use Minikube's Docker daemon..."
eval "$(minikube docker-env)"

# ── Step 2: build images inside Minikube ───────────────────────────────────────
PROJECT_ROOT="$(dirname "$K8S_DIR")"

info "Building weather-station-mock image..."
docker build -t weather-station-mock:latest "$PROJECT_ROOT/weather-station-mock"

info "Building central-station image..."
docker build -t central-station:latest "$PROJECT_ROOT/central-station"

ok "Images built inside Minikube's Docker daemon."

# ── Step 3: apply manifests in order ───────────────────────────────────────────
info "Creating namespace..."
kubectl apply -f "$K8S_DIR/00-namespace.yaml"

info "Deploying Kafka..."
kubectl apply -f "$K8S_DIR/kafka/"

info "Waiting for Kafka to be ready..."
kubectl rollout status deployment/kafka -n "$NAMESPACE" --timeout=300s

info "Deploying central-station..."
kubectl apply -f "$K8S_DIR/central-station/"

info "Deploying mock weather stations (1-10)..."
kubectl apply -f "$K8S_DIR/weather-stations/"

info "Deploying API weather station (11)..."
kubectl apply -f "$K8S_DIR/weather-stations-api/"

info "Deploying Kafka UI..."
kubectl apply -f "$K8S_DIR/kafka-ui/"

# ── Step 4: show status ────────────────────────────────────────────────────────
echo ""
ok "All manifests applied! Current pod status:"
kubectl get pods -n "$NAMESPACE"

echo ""
info "To watch pods come up:"
echo "  kubectl get pods -n $NAMESPACE -w"
echo ""
info "To access the central-station API:"
echo "  minikube service central-station -n $NAMESPACE --url"
echo ""
info "To access Kafka UI (topic browser + consumer groups):"
echo "  minikube service kafka-ui -n $NAMESPACE --url"
echo ""
info "To view logs:"
echo "  kubectl logs -n $NAMESPACE deploy/central-station -f"
echo "  kubectl logs -n $NAMESPACE deploy/weather-station-1 -f"
