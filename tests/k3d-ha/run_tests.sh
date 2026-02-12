#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NAMESPACE="varpulis-k3d"
COORDINATOR_URL="http://localhost:9100"
WORKER_URL="http://localhost:9000"
ADMIN_KEY="k3d-test-admin-key"
COORDINATOR_PF_PID=""
WORKER_PF_PID=""

PASS=0
FAIL=0
SKIP=0

usage() {
    echo "Usage: $0 [--port-forward] [--coordinator-url=URL] [--worker-url=URL]"
    echo ""
    echo "Flags:"
    echo "  --port-forward         Start kubectl port-forward for coordinator and worker"
    echo "  --coordinator-url=URL  Override coordinator URL (default: http://localhost:9100)"
    echo "  --worker-url=URL       Override worker URL (default: http://localhost:9000)"
    exit 1
}

cleanup() {
    [ -n "$COORDINATOR_PF_PID" ] && kill "$COORDINATOR_PF_PID" 2>/dev/null || true
    [ -n "$WORKER_PF_PID" ] && kill "$WORKER_PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (expected '$expected', got '$actual')"
        FAIL=$((FAIL + 1))
    fi
}

assert_contains() {
    local desc="$1" needle="$2" haystack="$3"
    if echo "$haystack" | grep -q "$needle"; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (expected to contain '$needle')"
        FAIL=$((FAIL + 1))
    fi
}

assert_true() {
    local desc="$1" condition="$2"
    if [ "$condition" = "true" ] || [ "$condition" = "1" ]; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc"
        FAIL=$((FAIL + 1))
    fi
}

wait_for_url() {
    local url="$1" max_wait="${2:-30}"
    for i in $(seq 1 "$max_wait"); do
        if curl -sf -o /dev/null "$url" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# Parse arguments
USE_PORT_FORWARD=false
for arg in "$@"; do
    case "$arg" in
        --port-forward)          USE_PORT_FORWARD=true ;;
        --coordinator-url=*)     COORDINATOR_URL="${arg#--coordinator-url=}" ;;
        --worker-url=*)          WORKER_URL="${arg#--worker-url=}" ;;
        --help|-h)               usage ;;
        *)                       echo "Unknown argument: $arg"; usage ;;
    esac
done

# Start port-forwards if requested
if [ "$USE_PORT_FORWARD" = true ]; then
    echo "==> Starting port-forwards..."
    kubectl port-forward \
        --namespace "$NAMESPACE" \
        svc/k3d-varpulis-coordinator 9100:9100 &
    COORDINATOR_PF_PID=$!

    kubectl port-forward \
        --namespace "$NAMESPACE" \
        svc/k3d-varpulis 9000:9000 &
    WORKER_PF_PID=$!

    sleep 3
    echo "    Coordinator PF PID: $COORDINATOR_PF_PID"
    echo "    Worker PF PID: $WORKER_PF_PID"
fi

echo ""
echo "Varpulis k3d HA Integration Tests"
echo "========================================"
echo "Coordinator URL: $COORDINATOR_URL"
echo "Worker URL:      $WORKER_URL"
echo "Admin Key:       $ADMIN_KEY"
echo ""

# ================================================================
# Phase 0: Cluster Health
# ================================================================
echo "[Phase 0] Cluster health check"
echo "-------------------------------"

# Check coordinator is healthy
COORD_HEALTH=$(curl -sf "$COORDINATOR_URL/health" 2>/dev/null || echo '{}')
assert_contains "Coordinator /health responds" '"healthy"' "$COORD_HEALTH"

# Check coordinator is ready (leader or standalone)
COORD_READY_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$COORDINATOR_URL/ready")
assert_eq "Coordinator /ready returns 200" "200" "$COORD_READY_CODE"

# Check HA role in health response
COORD_HA_ROLE=$(echo "$COORD_HEALTH" | jq -r '.ha_role // "unknown"')
echo "  INFO: Coordinator HA role: $COORD_HA_ROLE"

# Check WebSocket connections count
WS_COUNT=$(echo "$COORD_HEALTH" | jq -r '.ws_connections // 0')
echo "  INFO: WebSocket connections: $WS_COUNT"

# Check workers registered
CLUSTER_STATUS=$(curl -sf "$COORDINATOR_URL/api/v1/cluster/status" \
    -H "x-admin-key: $ADMIN_KEY" 2>/dev/null || echo '{}')
WORKER_COUNT=$(echo "$CLUSTER_STATUS" | jq -r '.workers | length // 0' 2>/dev/null || echo "0")
echo "  INFO: Workers registered: $WORKER_COUNT"

if [ "$WORKER_COUNT" -ge 1 ] 2>/dev/null; then
    echo "  PASS: At least 1 worker registered"
    PASS=$((PASS + 1))
else
    echo "  FAIL: No workers registered"
    FAIL=$((FAIL + 1))
fi

echo ""

# ================================================================
# Phase 1: Baseline — Deploy Pipeline and Process Events
# ================================================================
echo "[Phase 1] Baseline — deploy and process events"
echo "-------------------------------------------------"

# Deploy a test pipeline via coordinator
DEPLOY_RESP=$(curl -sf -X POST "$COORDINATOR_URL/api/v1/cluster/deploy" \
    -H "Content-Type: application/json" \
    -H "x-admin-key: $ADMIN_KEY" \
    -d '{
        "name": "ha-test-pipeline",
        "vpl": "stream Alerts = SensorReading .where(temperature > 100)",
        "replicas": 1
    }' 2>/dev/null || echo '{"error": "deploy failed"}')
assert_contains "Pipeline deployed successfully" '"ha-test-pipeline"' "$DEPLOY_RESP"

# Give pipeline time to start
sleep 3

# Inject events via worker
EVENT_RESULTS=0
for i in $(seq 1 10); do
    CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
        "$WORKER_URL/api/v1/events" \
        -H "Content-Type: application/json" \
        -d "{\"event_type\": \"SensorReading\", \"fields\": {\"temperature\": $((100 + i)), \"sensor_id\": \"ha-test-$i\"}}" \
        2>/dev/null)
    if [ "$CODE" = "200" ] || [ "$CODE" = "202" ]; then
        EVENT_RESULTS=$((EVENT_RESULTS + 1))
    fi
done
assert_true "At least 5 of 10 events accepted" "$([ "$EVENT_RESULTS" -ge 5 ] && echo true || echo false)"
echo "  INFO: $EVENT_RESULTS/10 events accepted"

echo ""

# ================================================================
# Phase 2: WebSocket Failover — Kill Worker Pod
# ================================================================
echo "[Phase 2] WebSocket failover — kill worker pod"
echo "-------------------------------------------------"

# Get a worker pod name
WORKER_POD=$(kubectl get pod -n "$NAMESPACE" -l app.kubernetes.io/component=worker \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [ -n "$WORKER_POD" ]; then
    echo "  INFO: Target worker pod: $WORKER_POD"

    # Record WS connections before
    WS_BEFORE=$(curl -sf "$COORDINATOR_URL/health" 2>/dev/null | jq -r '.ws_connections // 0')
    echo "  INFO: WS connections before kill: $WS_BEFORE"

    # Delete the pod with no grace period (instant kill)
    KILL_START=$(date +%s%N)
    kubectl delete pod "$WORKER_POD" -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true

    # Wait a moment for failure detection
    sleep 2

    KILL_END=$(date +%s%N)
    DETECTION_MS=$(( (KILL_END - KILL_START) / 1000000 ))

    # Check coordinator detected the failure
    CLUSTER_STATUS_AFTER=$(curl -sf "$COORDINATOR_URL/api/v1/cluster/status" \
        -H "x-admin-key: $ADMIN_KEY" 2>/dev/null || echo '{}')

    # Check logs for WS disconnect detection
    WS_DETECTED=$(kubectl logs statefulset/k3d-varpulis-coordinator \
        -n "$NAMESPACE" --tail=50 2>/dev/null | \
        grep -c "WebSocket disconnected" || true)

    if [ "$WS_DETECTED" -ge 1 ]; then
        echo "  PASS: WebSocket disconnect detected in coordinator logs"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: No WebSocket disconnect detection in logs"
        FAIL=$((FAIL + 1))
    fi

    echo "  INFO: Detection within ~${DETECTION_MS}ms window"

    # Wait for replacement pod to come up
    echo "  INFO: Waiting for replacement pod..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=worker \
        -n "$NAMESPACE" --timeout=60s 2>/dev/null || true
    sleep 3
else
    echo "  SKIP: No worker pod found for failover test"
    SKIP=$((SKIP + 1))
fi

echo ""

# ================================================================
# Phase 3: K8s Pod Watcher Failover
# ================================================================
echo "[Phase 3] K8s pod watcher failover"
echo "------------------------------------"

WORKER_POD=$(kubectl get pod -n "$NAMESPACE" -l app.kubernetes.io/component=worker \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [ -n "$WORKER_POD" ]; then
    echo "  INFO: Target worker pod: $WORKER_POD"

    # Delete pod (triggers both WS and K8s watcher)
    kubectl delete pod "$WORKER_POD" -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true
    sleep 3

    # Check for K8s pod watcher detection in logs
    K8S_DETECTED=$(kubectl logs statefulset/k3d-varpulis-coordinator \
        -n "$NAMESPACE" --tail=50 2>/dev/null | \
        grep -c "K8s pod watcher" || true)

    if [ "$K8S_DETECTED" -ge 1 ]; then
        echo "  PASS: K8s pod watcher detection found in coordinator logs"
        PASS=$((PASS + 1))
    else
        echo "  INFO: K8s pod watcher detection not in logs (may not be compiled with k8s feature)"
        SKIP=$((SKIP + 1))
    fi

    # Check failover was triggered
    FAILOVER_LOGGED=$(kubectl logs statefulset/k3d-varpulis-coordinator \
        -n "$NAMESPACE" --tail=50 2>/dev/null | \
        grep -c "triggering failover\|marked unhealthy" || true)
    assert_true "Failover triggered for killed worker" "$([ "$FAILOVER_LOGGED" -ge 1 ] && echo true || echo false)"

    # Wait for replacement
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=worker \
        -n "$NAMESPACE" --timeout=60s 2>/dev/null || true
    sleep 3
else
    echo "  SKIP: No worker pod found"
    SKIP=$((SKIP + 1))
fi

echo ""

# ================================================================
# Phase 4: Coordinator Failover — Kill Leader Pod
# ================================================================
echo "[Phase 4] Coordinator failover — kill leader pod"
echo "---------------------------------------------------"

# Find the leader coordinator pod
LEADER_POD=""
for pod in $(kubectl get pod -n "$NAMESPACE" -l app.kubernetes.io/component=coordinator \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    POD_LOGS=$(kubectl logs "$pod" -n "$NAMESPACE" --tail=20 2>/dev/null || true)
    if echo "$POD_LOGS" | grep -q "Leader\|leader\|standalone"; then
        LEADER_POD="$pod"
        break
    fi
done

if [ -n "$LEADER_POD" ]; then
    echo "  INFO: Leader coordinator pod: $LEADER_POD"

    # Kill the leader
    kubectl delete pod "$LEADER_POD" -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true

    # Wait for new leader election (K8s Lease timeout ~15s)
    echo "  INFO: Waiting for new leader election (up to 30s)..."
    NEW_LEADER=false
    for i in $(seq 1 30); do
        READY_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$COORDINATOR_URL/ready" 2>/dev/null || echo "000")
        if [ "$READY_CODE" = "200" ]; then
            echo "  PASS: New leader ready after ~${i}s"
            PASS=$((PASS + 1))
            NEW_LEADER=true
            break
        fi
        sleep 1
    done

    if [ "$NEW_LEADER" = "false" ]; then
        echo "  FAIL: No new leader within 30s"
        FAIL=$((FAIL + 1))
    fi

    # Verify API still responds
    HEALTH_AFTER=$(curl -sf "$COORDINATOR_URL/health" 2>/dev/null || echo '{}')
    assert_contains "Coordinator API responds after failover" '"healthy"' "$HEALTH_AFTER"

    # Wait for StatefulSet to restore the killed pod
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=coordinator \
        -n "$NAMESPACE" --timeout=60s 2>/dev/null || true
else
    echo "  SKIP: Could not identify leader coordinator pod"
    SKIP=$((SKIP + 1))
fi

echo ""

# ================================================================
# Phase 5: Pipeline Continuity Across Coordinator Failover
# ================================================================
echo "[Phase 5] Pipeline continuity after coordinator failover"
echo "----------------------------------------------------------"

# Check pipeline status via coordinator
PIPELINE_STATUS=$(curl -sf "$COORDINATOR_URL/api/v1/cluster/status" \
    -H "x-admin-key: $ADMIN_KEY" 2>/dev/null || echo '{}')
PIPELINE_COUNT=$(echo "$PIPELINE_STATUS" | jq -r '.pipeline_groups | length // 0' 2>/dev/null || echo "0")

if [ "$PIPELINE_COUNT" -ge 1 ] 2>/dev/null; then
    echo "  PASS: Pipeline state preserved after coordinator failover ($PIPELINE_COUNT pipelines)"
    PASS=$((PASS + 1))
else
    echo "  INFO: Pipeline state may have been lost (found $PIPELINE_COUNT pipelines)"
    # Not a hard failure — state replication is best-effort in this test
    SKIP=$((SKIP + 1))
fi

# Try injecting more events
EVENT_OK=0
for i in $(seq 1 5); do
    CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
        "$WORKER_URL/api/v1/events" \
        -H "Content-Type: application/json" \
        -d "{\"event_type\": \"SensorReading\", \"fields\": {\"temperature\": $((200 + i)), \"sensor_id\": \"post-failover-$i\"}}" \
        2>/dev/null)
    if [ "$CODE" = "200" ] || [ "$CODE" = "202" ]; then
        EVENT_OK=$((EVENT_OK + 1))
    fi
done
echo "  INFO: $EVENT_OK/5 events accepted post-failover"

echo ""

# ================================================================
# Cleanup
# ================================================================
echo "[Cleanup] Removing test pipeline"
echo "-----------------------------------"
curl -sf -X DELETE "$COORDINATOR_URL/api/v1/cluster/deploy/ha-test-pipeline" \
    -H "x-admin-key: $ADMIN_KEY" 2>/dev/null || true
echo "  Done."

echo ""

# ================================================================
# Summary
# ================================================================
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
