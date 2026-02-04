#!/usr/bin/env bash
# Integration test runner for the README HVAC example.
#
# Usage:
#   bash tests/integration/run_readme_test.sh
#
# This script:
# 1. Builds varpulis in release mode
# 2. Starts the API server with a test admin key
# 3. Runs the Python integration test
# 4. Kills the server and reports results

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PORT=19876
ADMIN_KEY="test-admin-key-$(date +%s)"
PID_FILE=$(mktemp)
LOG_FILE=$(mktemp)

cleanup() {
    if [ -f "$PID_FILE" ] && [ -s "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping varpulis server (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    fi
    rm -f "$PID_FILE" "$LOG_FILE"
}

trap cleanup EXIT

echo "============================================================"
echo "Varpulis README Integration Test"
echo "============================================================"

# Step 1: Build
echo ""
echo "[1/4] Building varpulis..."
cargo build --release -p varpulis-cli --manifest-path "$PROJECT_ROOT/Cargo.toml" 2>&1 | tail -3

BINARY="$PROJECT_ROOT/target/release/varpulis"
if [ ! -x "$BINARY" ]; then
    # Try with cargo's default binary name
    BINARY="$PROJECT_ROOT/target/release/varpulis-cli"
fi
if [ ! -x "$BINARY" ]; then
    echo "ERROR: Could not find varpulis binary"
    exit 1
fi

# Step 2: Start server
echo ""
echo "[2/4] Starting varpulis server on port $PORT..."
"$BINARY" server --port "$PORT" --admin-key "$ADMIN_KEY" --api-key "bootstrap-key" > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

# Wait for server to be ready
echo "  Waiting for server..."
for i in $(seq 1 15); do
    if curl -sf "http://localhost:$PORT/ready" > /dev/null 2>&1; then
        echo "  Server ready after ${i}s"
        break
    fi
    if [ "$i" -eq 15 ]; then
        echo "  ERROR: Server failed to start. Log:"
        cat "$LOG_FILE"
        exit 1
    fi
    sleep 1
done

# Step 3: Run test
echo ""
echo "[3/4] Running integration test..."
python3 "$SCRIPT_DIR/test_readme_example.py" \
    --base-url "http://localhost:$PORT" \
    --admin-key "$ADMIN_KEY"
TEST_EXIT=$?

# Step 4: Report
echo ""
echo "[4/4] Server log (last 20 lines):"
tail -20 "$LOG_FILE"

exit $TEST_EXIT
