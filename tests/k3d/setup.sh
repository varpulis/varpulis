#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLUSTER_NAME="varpulis-test"
IMAGE_NAME="varpulis-local:latest"
NAMESPACE="varpulis-k3d"

usage() {
    echo "Usage: $0 [--rebuild] [--teardown]"
    echo ""
    echo "Flags:"
    echo "  --rebuild    Delete existing cluster and rebuild from scratch"
    echo "  --teardown   Delete the cluster and exit"
    echo ""
    echo "With no flags, creates the cluster if it doesn't exist."
    exit 1
}

teardown() {
    echo "==> Tearing down k3d cluster '$CLUSTER_NAME'..."
    k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true
    echo "==> Cluster deleted."
}

build_image() {
    echo "==> Building Docker image '$IMAGE_NAME'..."
    docker build -t "$IMAGE_NAME" -f "$REPO_ROOT/deploy/docker/Dockerfile" "$REPO_ROOT"
    echo "==> Image built."
}

create_cluster() {
    echo "==> Creating k3d cluster '$CLUSTER_NAME'..."
    k3d cluster create "$CLUSTER_NAME" \
        --port "9000:9000@loadbalancer" \
        --port "9100:9100@loadbalancer" \
        --port "3000:3000@loadbalancer" \
        --wait
    echo "==> Cluster created."
}

import_image() {
    echo "==> Importing image '$IMAGE_NAME' into cluster..."
    k3d image import "$IMAGE_NAME" --cluster "$CLUSTER_NAME"
    echo "==> Image imported."
}

deploy() {
    echo "==> Creating namespace '$NAMESPACE'..."
    kubectl create namespace "$NAMESPACE" 2>/dev/null || true

    echo "==> Deploying Varpulis via kustomize..."
    kubectl apply -k "$REPO_ROOT/deploy/kubernetes/overlays/k3d"

    echo "==> Deploying infrastructure (Kafka, Mosquitto, Prometheus, Grafana)..."
    kubectl apply -f "$REPO_ROOT/deploy/kubernetes/overlays/k3d/infra/" --namespace "$NAMESPACE"

    echo "==> Waiting for coordinator StatefulSet rollout..."
    kubectl rollout status statefulset/k3d-varpulis-coordinator \
        --namespace "$NAMESPACE" \
        --timeout=120s

    echo "==> Waiting for coordinator leader election..."
    for i in $(seq 1 30); do
        if kubectl logs statefulset/k3d-varpulis-coordinator \
            --namespace "$NAMESPACE" 2>/dev/null | grep -q "Leader"; then
            echo "    Leader elected."
            break
        fi
        echo "    Waiting for leader election... ($i/30)"
        sleep 2
    done

    echo "==> Waiting for Varpulis worker deployment rollout..."
    kubectl rollout status deployment/k3d-varpulis \
        --namespace "$NAMESPACE" \
        --timeout=120s

    echo "==> Waiting for Kafka deployment rollout..."
    kubectl rollout status deployment/kafka \
        --namespace "$NAMESPACE" \
        --timeout=120s

    echo "==> Waiting for Mosquitto deployment rollout..."
    kubectl rollout status deployment/mosquitto \
        --namespace "$NAMESPACE" \
        --timeout=120s

    echo "==> Waiting for Prometheus deployment rollout..."
    kubectl rollout status deployment/prometheus \
        --namespace "$NAMESPACE" \
        --timeout=120s

    echo "==> Waiting for Grafana deployment rollout..."
    kubectl rollout status deployment/grafana \
        --namespace "$NAMESPACE" \
        --timeout=120s

    echo "==> Deployment complete."
}

# Parse arguments
REBUILD=false
TEARDOWN=false

for arg in "$@"; do
    case "$arg" in
        --rebuild)  REBUILD=true ;;
        --teardown) TEARDOWN=true ;;
        --help|-h)  usage ;;
        *)          echo "Unknown argument: $arg"; usage ;;
    esac
done

# Teardown only
if [ "$TEARDOWN" = true ]; then
    teardown
    exit 0
fi

# Rebuild: teardown first
if [ "$REBUILD" = true ]; then
    teardown
fi

# Check if cluster exists
if k3d cluster list 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    echo "==> Cluster '$CLUSTER_NAME' already exists."
    if [ "$REBUILD" = false ]; then
        echo "    Use --rebuild to recreate. Updating deployment only."
        build_image
        import_image
        deploy
        exit 0
    fi
fi

# Full setup
build_image
create_cluster
import_image
deploy

echo ""
echo "==> k3d cluster '$CLUSTER_NAME' is ready."
echo "    Coordinator API: http://localhost:9100"
echo "    Worker API:      http://localhost:9000"
echo "    Grafana:         http://localhost:3000 (anonymous admin, no login required)"
echo "    Admin key:       k3d-test-admin-key"
echo ""
echo "    Internal endpoints (within cluster):"
echo "      Kafka:      kafka.${NAMESPACE}.svc.cluster.local:9092"
echo "      MQTT:       mosquitto.${NAMESPACE}.svc.cluster.local:1883"
echo "      Prometheus: prometheus.${NAMESPACE}.svc.cluster.local:9090"
echo ""
echo "    Run integration tests:"
echo "      ./tests/k3d/run_tests.sh --port-forward"
