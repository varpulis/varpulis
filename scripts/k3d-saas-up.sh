#!/usr/bin/env bash
# k3d SaaS Environment Setup
#
# Creates a local k3d cluster with the full Varpulis SaaS stack:
# PostgreSQL, Varpulis Server (SaaS mode), Coordinator, Web UI,
# Kafka, Mosquitto, Prometheus, and Grafana.
#
# Usage: ./scripts/k3d-saas-up.sh [--skip-build] [--test]
#   --skip-build  Reuse existing Docker images (skip cargo build + docker build)
#   --test        Run smoke tests after deployment
#
# Prerequisites: k3d, kubectl, docker, cargo, jq

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLUSTER_NAME="varpulis-saas"
NAMESPACE="varpulis-k3d-saas"
OVERLAY_DIR="$PROJECT_ROOT/deploy/kubernetes/overlays/k3d-saas"

SKIP_BUILD=false
RUN_TESTS=false

for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        --test) RUN_TESTS=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# Colors
if [[ -z "${NO_COLOR:-}" && -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' BOLD='' NC=''
fi

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

# ─── Check prerequisites ───────────────────────────────────────────────────
info "Checking prerequisites..."
for cmd in k3d kubectl docker cargo jq; do
    if ! command -v "$cmd" &>/dev/null; then
        fail "Required command not found: $cmd"
    fi
done
ok "All prerequisites found"

# ─── Create k3d cluster ────────────────────────────────────────────────────
if k3d cluster list 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    warn "Cluster '$CLUSTER_NAME' already exists"
    info "Use ./scripts/k3d-saas-down.sh to remove it first, or --skip-build to reuse"
else
    info "Creating k3d cluster '$CLUSTER_NAME'..."
    k3d cluster create "$CLUSTER_NAME" \
        --port "8080:30080@server:0" \
        --port "9000:30900@server:0" \
        --port "3000:30300@server:0" \
        --agents 0 \
        --servers 1 \
        --wait
    ok "Cluster created"
fi

# Switch kubectl context
kubectl config use-context "k3d-$CLUSTER_NAME"

# ─── Build images ──────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == "false" ]]; then
    info "Building varpulis binary (release, saas features)..."
    cd "$PROJECT_ROOT"
    cargo build -p varpulis-cli --release --features "kafka,raft,persistent,saas"
    ok "Binary built"

    info "Building varpulis-saas-local Docker image..."
    docker build -t varpulis-saas-local:latest -f deploy/docker/Dockerfile.prebuilt .
    ok "Server image built"

    info "Building varpulis-web-ui-local Docker image..."
    docker build -t varpulis-web-ui-local:latest -f web-ui/Dockerfile web-ui/
    ok "Web UI image built"
else
    info "Skipping build (--skip-build)"
fi

# ─── Import images into k3d ───────────────────────────────────────────────
info "Importing images into k3d cluster..."
k3d image import varpulis-saas-local:latest varpulis-web-ui-local:latest -c "$CLUSTER_NAME"
ok "Images imported"

# ─── Deploy ────────────────────────────────────────────────────────────────
info "Applying kustomize overlay..."
kubectl apply -k "$OVERLAY_DIR"
ok "Manifests applied"

# ─── Wait for pods ─────────────────────────────────────────────────────────
info "Waiting for PostgreSQL..."
kubectl wait --for=condition=ready pod -l app=varpulis-postgres -n "$NAMESPACE" --timeout=120s
ok "PostgreSQL ready"

info "Waiting for coordinator..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=coordinator -n "$NAMESPACE" --timeout=120s
ok "Coordinator ready"

info "Waiting for worker..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=worker -n "$NAMESPACE" --timeout=180s
ok "Worker ready"

info "Waiting for web UI..."
kubectl wait --for=condition=ready pod -l app=varpulis-webui -n "$NAMESPACE" --timeout=120s
ok "Web UI ready"

# ─── Summary ──────────────────────────────────────────────────────────────
ADMIN_PASSWORD=$(kubectl get secret varpulis-k3d-saas-secrets -n "$NAMESPACE" \
    -o jsonpath='{.data.admin-password}' 2>/dev/null | base64 -d || echo "admin")

echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Varpulis SaaS Local Environment${NC}"
echo -e "${BOLD}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${GREEN}Web UI:${NC}      http://localhost:8080"
echo -e "  ${GREEN}API:${NC}         http://localhost:9000"
echo -e "  ${GREEN}Grafana:${NC}     http://localhost:3000  (admin/admin)"
echo ""
echo -e "  ${GREEN}Admin user:${NC}  admin"
echo -e "  ${GREEN}Admin pass:${NC}  $ADMIN_PASSWORD"
echo ""
echo -e "  ${BLUE}API key:${NC}     k3d-saas-test-key"
echo -e "  ${BLUE}Namespace:${NC}   $NAMESPACE"
echo ""
echo -e "  Teardown:  ${YELLOW}./scripts/k3d-saas-down.sh${NC}"
echo -e "  Tests:     ${YELLOW}./scripts/k3d-saas-test.sh${NC}"
echo -e "${BOLD}════════════════════════════════════════════════════════════${NC}"
echo ""

# ─── Run smoke tests if requested ──────────────────────────────────────────
if [[ "$RUN_TESTS" == "true" ]]; then
    info "Running smoke tests..."
    "$SCRIPT_DIR/k3d-saas-test.sh"
fi
