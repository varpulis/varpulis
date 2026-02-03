#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="varpulis-k3d"
BASE_URL="http://localhost:9000"
ADMIN_KEY="k3d-test-admin-key"
PORT_FORWARD_PID=""

PASS=0
FAIL=0

usage() {
    echo "Usage: $0 [--port-forward] [--base-url=URL]"
    echo ""
    echo "Flags:"
    echo "  --port-forward     Start kubectl port-forward before running tests"
    echo "  --base-url=URL     Override the base URL (default: http://localhost:9000)"
    exit 1
}

cleanup() {
    if [ -n "$PORT_FORWARD_PID" ]; then
        kill "$PORT_FORWARD_PID" 2>/dev/null || true
        wait "$PORT_FORWARD_PID" 2>/dev/null || true
    fi
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

assert_http_code() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "  PASS: $desc (HTTP $actual)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (expected HTTP $expected, got HTTP $actual)"
        FAIL=$((FAIL + 1))
    fi
}

# Parse arguments
USE_PORT_FORWARD=false
for arg in "$@"; do
    case "$arg" in
        --port-forward)    USE_PORT_FORWARD=true ;;
        --base-url=*)      BASE_URL="${arg#--base-url=}" ;;
        --help|-h)         usage ;;
        *)                 echo "Unknown argument: $arg"; usage ;;
    esac
done

# Start port-forward if requested
if [ "$USE_PORT_FORWARD" = true ]; then
    echo "==> Starting port-forward to $NAMESPACE/k3d-varpulis on port 9000..."
    kubectl port-forward \
        --namespace "$NAMESPACE" \
        svc/k3d-varpulis 9000:9000 &
    PORT_FORWARD_PID=$!
    sleep 3
    echo "    Port-forward PID: $PORT_FORWARD_PID"
fi

echo ""
echo "Varpulis k3d Integration Tests"
echo "================================"
echo "Base URL:  $BASE_URL"
echo "Admin Key: $ADMIN_KEY"
echo ""

# ---------------------------------------------------------------
# Step 1: Health check
# ---------------------------------------------------------------
echo "[1/9] Health check"
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/health")
assert_http_code "/health returns 200" "200" "$HTTP_CODE"

BODY=$(curl -s "$BASE_URL/health")
assert_contains "/health returns healthy status" '"healthy"' "$BODY"

# ---------------------------------------------------------------
# Step 2: Readiness check
# ---------------------------------------------------------------
echo "[2/9] Readiness check"
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/ready")
# Server may return 200 or 503 depending on engine state; both are valid responses
if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "503" ]; then
    echo "  PASS: /ready returns valid status (HTTP $HTTP_CODE)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: /ready returned unexpected HTTP $HTTP_CODE"
    FAIL=$((FAIL + 1))
fi

# ---------------------------------------------------------------
# Step 3: Create tenant
# ---------------------------------------------------------------
echo "[3/9] Create tenant"
CREATE_RESP=$(curl -s -X POST "$BASE_URL/api/v1/tenants" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $ADMIN_KEY" \
    -d '{"name": "k3d-test-tenant", "quota": "free"}')
TENANT_ID=$(echo "$CREATE_RESP" | jq -r '.id // empty')
if [ -n "$TENANT_ID" ]; then
    echo "  PASS: Tenant created (id=$TENANT_ID)"
    PASS=$((PASS + 1))
    TENANT_KEY=$(echo "$CREATE_RESP" | jq -r '.api_key // empty')
else
    echo "  FAIL: Could not create tenant: $CREATE_RESP"
    FAIL=$((FAIL + 1))
    TENANT_KEY=""
fi

# ---------------------------------------------------------------
# Step 4: List tenants
# ---------------------------------------------------------------
echo "[4/9] List tenants"
LIST_RESP=$(curl -s "$BASE_URL/api/v1/tenants" \
    -H "Authorization: Bearer $ADMIN_KEY")
TENANT_COUNT=$(echo "$LIST_RESP" | jq -r '.total // 0')
if [ "$TENANT_COUNT" -ge 1 ] 2>/dev/null; then
    echo "  PASS: Tenant list contains $TENANT_COUNT tenant(s)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Expected at least 1 tenant, got: $LIST_RESP"
    FAIL=$((FAIL + 1))
fi

# ---------------------------------------------------------------
# Step 5: Deploy pipeline
# ---------------------------------------------------------------
echo "[5/9] Deploy pipeline"
AUTH_HEADER="Authorization: Bearer ${TENANT_KEY:-$ADMIN_KEY}"
DEPLOY_RESP=$(curl -s -X POST "$BASE_URL/api/v1/pipelines" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"name": "k3d-test-pipeline", "source": "stream Alerts = SensorReading .where(temperature > 100)"}')
PIPELINE_ID=$(echo "$DEPLOY_RESP" | jq -r '.id // empty')
if [ -n "$PIPELINE_ID" ]; then
    echo "  PASS: Pipeline deployed (id=$PIPELINE_ID)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Could not deploy pipeline: $DEPLOY_RESP"
    FAIL=$((FAIL + 1))
fi

# ---------------------------------------------------------------
# Step 6: Inject events
# ---------------------------------------------------------------
echo "[6/9] Inject events"
EVENT_RESP=$(curl -s -X POST "$BASE_URL/api/v1/pipelines/${PIPELINE_ID:-none}/events" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"event_type": "SensorReading", "data": {"temperature": 150.0, "sensor_id": "k3d-test"}}')
EVENT_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    "$BASE_URL/api/v1/pipelines/${PIPELINE_ID:-none}/events" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"event_type": "SensorReading", "data": {"temperature": 150.0, "sensor_id": "k3d-test"}}')
if [ "$EVENT_CODE" = "200" ] || [ "$EVENT_CODE" = "202" ]; then
    echo "  PASS: Event injected (HTTP $EVENT_CODE)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Event injection returned HTTP $EVENT_CODE: $EVENT_RESP"
    FAIL=$((FAIL + 1))
fi

# ---------------------------------------------------------------
# Step 7: Check metrics / usage
# ---------------------------------------------------------------
echo "[7/9] Check usage"
USAGE_RESP=$(curl -s "$BASE_URL/api/v1/usage" \
    -H "$AUTH_HEADER")
EVENTS_PROCESSED=$(echo "$USAGE_RESP" | jq -r '.events_processed // 0')
if [ "$EVENTS_PROCESSED" -ge 1 ] 2>/dev/null; then
    echo "  PASS: Usage reports $EVENTS_PROCESSED event(s) processed"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Expected events_processed >= 1, got: $USAGE_RESP"
    FAIL=$((FAIL + 1))
fi

# ---------------------------------------------------------------
# Step 8: Delete pipeline
# ---------------------------------------------------------------
echo "[8/9] Delete pipeline"
if [ -n "${PIPELINE_ID:-}" ]; then
    DEL_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
        "$BASE_URL/api/v1/pipelines/$PIPELINE_ID" \
        -H "$AUTH_HEADER")
    assert_http_code "DELETE pipeline returns 200" "200" "$DEL_CODE"
else
    echo "  SKIP: No pipeline to delete"
fi

# ---------------------------------------------------------------
# Step 9: Delete tenant
# ---------------------------------------------------------------
echo "[9/9] Delete tenant"
if [ -n "${TENANT_ID:-}" ]; then
    DEL_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
        "$BASE_URL/api/v1/tenants/$TENANT_ID" \
        -H "Authorization: Bearer $ADMIN_KEY")
    assert_http_code "DELETE tenant returns 200" "200" "$DEL_CODE"
else
    echo "  SKIP: No tenant to delete"
fi

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo ""
echo "================================"
echo "Results: $PASS passed, $FAIL failed"
echo "================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
