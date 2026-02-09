#!/bin/bash
# E2E Showcase: Start Varpulis cluster + infrastructure
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=============================================="
echo "  Varpulis E2E Showcase"
echo "=============================================="
echo "  Project root: $PROJECT_ROOT"
echo "=============================================="

# 1. Start infrastructure
echo ""
echo ">>> Starting Docker infrastructure (Mosquitto + Kafka)..."
cd "$SCRIPT_DIR"
docker compose up -d
echo "    Waiting for services to be ready..."
sleep 5

# 2. Build Varpulis (if needed)
echo ""
echo ">>> Building Varpulis..."
cd "$PROJECT_ROOT"
cargo build --release -p varpulis 2>&1 | tail -1

VARPULIS="$PROJECT_ROOT/target/release/varpulis"
if [ ! -f "$VARPULIS" ]; then
    echo "ERROR: Build failed, varpulis binary not found"
    exit 1
fi

# 3. Start coordinator
echo ""
echo ">>> Starting coordinator on port 9100..."
$VARPULIS coordinator --port 9100 --api-key dev-key &
COORD_PID=$!
echo "    PID: $COORD_PID"
sleep 2

# 4. Start workers
echo ""
echo ">>> Starting worker w0 on port 9000..."
$VARPULIS server --port 9000 --api-key dev-key --coordinator http://localhost:9100 --worker-id w0 &
W0_PID=$!
echo "    PID: $W0_PID"

echo ">>> Starting worker w1 on port 9001..."
$VARPULIS server --port 9001 --api-key dev-key --coordinator http://localhost:9100 --worker-id w1 &
W1_PID=$!
echo "    PID: $W1_PID"
sleep 3

# 5. Start web UI dev server (optional)
echo ""
echo ">>> Starting web UI dev server..."
cd "$PROJECT_ROOT/web-ui"
if [ -f "node_modules/.bin/vite" ]; then
    npx vite --port 5173 &
    UI_PID=$!
    echo "    PID: $UI_PID"
else
    echo "    Skipping (run 'npm install' in web-ui/ first)"
    UI_PID=""
fi

echo ""
echo "=============================================="
echo "  All services running!"
echo "=============================================="
echo "  Coordinator: http://localhost:9100"
echo "  Worker w0:   http://localhost:9000"
echo "  Worker w1:   http://localhost:9001"
echo "  Web UI:      http://localhost:5173"
echo "  MQTT:        localhost:1883"
echo "  Kafka:       localhost:9092"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. python setup.py          # Create connectors + deploy pipeline"
echo "  2. python generator.py      # Send market events"
echo "  3. python consumer.py       # Watch trading signals"
echo ""
echo "Press Ctrl+C to stop all services"

# Trap and cleanup
cleanup() {
    echo ""
    echo ">>> Stopping services..."
    kill $COORD_PID $W0_PID $W1_PID $UI_PID 2>/dev/null || true
    cd "$SCRIPT_DIR"
    docker compose down
    echo "Done."
}
trap cleanup EXIT INT TERM

# Wait for any child to exit
wait
