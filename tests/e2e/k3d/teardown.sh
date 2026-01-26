#!/bin/bash
set -euo pipefail

echo "=== Tearing down Varpulis E2E Test Environment ==="

# Delete k3d cluster
k3d cluster delete varpulis-test 2>/dev/null || true

echo "Cleanup complete."
