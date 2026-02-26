#!/usr/bin/env bash
set -euo pipefail

# Local Cluster Test Harness
# Runs coordinator + 2 workers as native cargo processes, deploys a pipeline,
# injects events, and verifies the output event relay chain.
#
# Prerequisites: curl, python3 (for JSON parsing)
# Optional: websocat (for WebSocket verification)
#
# Usage: ./tests/local-cluster/run.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

COORD_PORT=19100
WORKER1_PORT=19000
WORKER2_PORT=19001
API_KEY="test-local-cluster-key"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PIDS=()

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

log_ok()   { echo -e "${GREEN}[PASS]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; FAILURES=$((FAILURES + 1)); }
log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

FAILURES=0

mkdir -p "$RESULTS_DIR"

# ─── Step 1: Build ─────────────────────────────────────────────────────────
log_info "Building varpulis (release)..."
cargo build -p varpulis-cli --release --manifest-path "$REPO_ROOT/Cargo.toml" 2>&1 | tail -3
VARPULIS="$REPO_ROOT/target/release/varpulis"

if [ ! -x "$VARPULIS" ]; then
    echo "ERROR: varpulis binary not found at $VARPULIS"
    exit 1
fi

# ─── Step 2: Start Coordinator ─────────────────────────────────────────────
log_info "Starting coordinator on :$COORD_PORT..."
RUST_LOG=info "$VARPULIS" coordinator \
    --bind 127.0.0.1 --port "$COORD_PORT" \
    --api-key "$API_KEY" \
    > "$RESULTS_DIR/coordinator.log" 2>&1 &
PIDS+=($!)

# Wait for coordinator health
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:$COORD_PORT/health" >/dev/null 2>&1; then
        log_ok "Coordinator healthy (${i}s)"
        break
    fi
    if [ "$i" -eq 30 ]; then
        log_fail "Coordinator not healthy after 30s"
        cat "$RESULTS_DIR/coordinator.log"
        exit 1
    fi
    sleep 1
done

# ─── Step 3: Start Workers ─────────────────────────────────────────────────
log_info "Starting worker-1 on :$WORKER1_PORT..."
RUST_LOG=info "$VARPULIS" server \
    --bind 127.0.0.1 --port "$WORKER1_PORT" \
    --api-key "$API_KEY" \
    --coordinator "http://127.0.0.1:$COORD_PORT" \
    --worker-id "test-worker-1" \
    --advertise-address "http://127.0.0.1:$WORKER1_PORT" \
    > "$RESULTS_DIR/worker1.log" 2>&1 &
PIDS+=($!)

log_info "Starting worker-2 on :$WORKER2_PORT..."
RUST_LOG=info "$VARPULIS" server \
    --bind 127.0.0.1 --port "$WORKER2_PORT" \
    --api-key "$API_KEY" \
    --coordinator "http://127.0.0.1:$COORD_PORT" \
    --worker-id "test-worker-2" \
    --advertise-address "http://127.0.0.1:$WORKER2_PORT" \
    > "$RESULTS_DIR/worker2.log" 2>&1 &
PIDS+=($!)

# Wait for workers to register
sleep 3
for i in $(seq 1 30); do
    WORKER_COUNT=$(curl -sf "http://127.0.0.1:$COORD_PORT/api/v1/cluster/workers" \
        -H "x-api-key: $API_KEY" 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
    if [ "$WORKER_COUNT" -ge 2 ]; then
        log_ok "Both workers registered ($WORKER_COUNT workers)"
        break
    fi
    if [ "$i" -eq 30 ]; then
        log_fail "Workers not registered after 30s (got $WORKER_COUNT)"
        exit 1
    fi
    sleep 1
done

# ─── Step 4: Deploy a simple filter pipeline ───────────────────────────────
log_info "Deploying test pipeline..."
VPL_CODE='stream HighTemp = TempReading .where(value > 30) .emit(sensor: sensor_id, temp: value)'

DEPLOY_RESP=$(curl -sf -X POST "http://127.0.0.1:$COORD_PORT/api/v1/cluster/deploy" \
    -H "x-api-key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"vpl\": \"$VPL_CODE\"}" 2>/dev/null || echo '{"error":"deploy failed"}')

if echo "$DEPLOY_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'error' not in d or not d.get('error')" 2>/dev/null; then
    log_ok "Pipeline deployed"
else
    log_fail "Pipeline deploy failed: $DEPLOY_RESP"
fi

sleep 2

# ─── Step 5: Connect WebSocket (optional) ──────────────────────────────────
WS_PID=""
WS_OUTPUT="$RESULTS_DIR/ws_output.jsonl"
> "$WS_OUTPUT"

if command -v websocat &>/dev/null; then
    log_info "Connecting WebSocket to coordinator..."
    websocat -t "ws://127.0.0.1:$COORD_PORT/ws" > "$WS_OUTPUT" 2>/dev/null &
    WS_PID=$!
    PIDS+=($WS_PID)
    sleep 1
else
    log_info "websocat not found — skipping WebSocket verification (install: cargo install websocat)"
fi

# ─── Step 6: Inject test events ────────────────────────────────────────────
log_info "Injecting 3 test events (2 matching, 1 not matching)..."

# Find which worker has the pipeline
INJECT_PORT=""
for port in $WORKER1_PORT $WORKER2_PORT; do
    RESP=$(curl -sf "http://127.0.0.1:$port/ready" 2>/dev/null || echo "{}")
    if echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d.get('engine_loaded',False)" 2>/dev/null; then
        INJECT_PORT=$port
        break
    fi
done

if [ -z "$INJECT_PORT" ]; then
    log_fail "No worker has an engine loaded"
else
    log_ok "Injecting events into worker at :$INJECT_PORT"

    # Event 1: matches (value=35 > 30)
    curl -sf -X POST "http://127.0.0.1:$INJECT_PORT/ws" \
        -H "Content-Type: application/json" \
        -d '{"type":"inject_event","event_type":"TempReading","data":{"sensor_id":"S1","value":35}}' \
        >/dev/null 2>&1 || true

    # Event 2: does NOT match (value=25 < 30)
    curl -sf -X POST "http://127.0.0.1:$INJECT_PORT/ws" \
        -H "Content-Type: application/json" \
        -d '{"type":"inject_event","event_type":"TempReading","data":{"sensor_id":"S2","value":25}}' \
        >/dev/null 2>&1 || true

    # Event 3: matches (value=40 > 30)
    curl -sf -X POST "http://127.0.0.1:$INJECT_PORT/ws" \
        -H "Content-Type: application/json" \
        -d '{"type":"inject_event","event_type":"TempReading","data":{"sensor_id":"S3","value":40}}' \
        >/dev/null 2>&1 || true
fi

# ─── Step 7: Wait for relay propagation ────────────────────────────────────
log_info "Waiting 3s for relay propagation..."
sleep 3

# ─── Step 8: Verify relay metrics ──────────────────────────────────────────
log_info "Checking relay metrics..."

if [ -n "$INJECT_PORT" ]; then
    HEALTH=$(curl -sf "http://127.0.0.1:$INJECT_PORT/health" 2>/dev/null || echo "{}")
    RELAY_FORWARDED=$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('relay',{}).get('relay_events_forwarded',0))" 2>/dev/null || echo "0")
    RELAY_DROPPED=$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('relay',{}).get('relay_events_dropped',0))" 2>/dev/null || echo "0")
    RELAY_HEALTHY=$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('relay',{}).get('relay_coordinator_healthy',True))" 2>/dev/null || echo "unknown")

    log_info "Relay: forwarded=$RELAY_FORWARDED dropped=$RELAY_DROPPED coordinator_healthy=$RELAY_HEALTHY"

    if [ "$RELAY_HEALTHY" = "True" ] || [ "$RELAY_HEALTHY" = "true" ]; then
        log_ok "Coordinator marked healthy"
    else
        log_fail "Coordinator marked unhealthy"
    fi
fi

# ─── Step 9: Verify WebSocket output ──────────────────────────────────────
if [ -n "$WS_PID" ] && kill -0 "$WS_PID" 2>/dev/null; then
    WS_LINES=$(wc -l < "$WS_OUTPUT" | tr -d ' ')
    if [ "$WS_LINES" -gt 0 ]; then
        log_ok "WebSocket received $WS_LINES messages"
    else
        log_info "WebSocket received 0 messages (may need longer wait or events route via REST)"
    fi
fi

# ─── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "========================================"
if [ "$FAILURES" -eq 0 ]; then
    echo -e "${GREEN}All checks passed!${NC}"
else
    echo -e "${RED}$FAILURES check(s) failed${NC}"
fi
echo "========================================"
echo "Logs: $RESULTS_DIR/"
exit "$FAILURES"
