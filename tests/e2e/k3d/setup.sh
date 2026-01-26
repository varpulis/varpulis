#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

echo "=== Varpulis E2E Test Environment Setup ==="

# Check prerequisites
command -v k3d >/dev/null 2>&1 || { echo "k3d is required but not installed"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "docker is required but not installed"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required but not installed"; exit 1; }

# Build Varpulis extension VSIX
echo "Building Varpulis VSCode extension..."
cd "${PROJECT_ROOT}/vscode-varpulis"
if command -v npm >/dev/null 2>&1; then
    npm install
    npm run compile
    npx vsce package -o "${SCRIPT_DIR}/varpulis.vsix" || echo "VSIX packaging skipped (vsce not available)"
fi

# Create k3d cluster
echo "Creating k3d cluster..."
k3d cluster delete varpulis-test 2>/dev/null || true
k3d cluster create --config "${SCRIPT_DIR}/cluster.yaml"

# Wait for cluster
echo "Waiting for cluster to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=120s

# Apply manifests
echo "Deploying test infrastructure..."
kubectl apply -f "${SCRIPT_DIR}/manifests/namespace.yaml"
kubectl apply -f "${SCRIPT_DIR}/manifests/mosquitto.yaml"
kubectl apply -f "${SCRIPT_DIR}/manifests/code-server.yaml"

# Wait for deployments
echo "Waiting for deployments..."
kubectl -n varpulis-test wait --for=condition=Available deployment/mosquitto --timeout=120s
kubectl -n varpulis-test wait --for=condition=Available deployment/code-server --timeout=180s

# Get access info
echo ""
echo "=== Test Environment Ready ==="
echo "Code-Server: http://localhost:8080 (password: varpulis-test)"
echo "MQTT Broker: localhost:1883"
echo ""
echo "Run tests with: cd ${SCRIPT_DIR}/.. && npm test"
