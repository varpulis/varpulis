#!/usr/bin/env bash
# Run the Mandelbrot web demo
#
# This starts:
#   1. MQTT broker with WebSocket support (port 1883 for MQTT, 9001 for WS)
#   2. Varpulis server (port 9000)
#   3. Python HTTP server for the web app (port 8080)
#
# Then open http://localhost:8080 in your browser
#
# Prerequisites:
#   - Docker
#   - Python 3.8+
#
# Usage:
#   ./examples/mandelbrot/run-web.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MQTT_CONTAINER=mandelbrot-mosquitto
VARPULIS_PORT=9000
WEB_PORT=8080
API_KEY=test

cleanup() {
    echo ""
    echo "Cleaning up..."
    [ -n "${HTTP_PID:-}" ] && kill "$HTTP_PID" 2>/dev/null || true
    [ -n "${SERVER_PID:-}" ] && kill "$SERVER_PID" 2>/dev/null || true
    docker rm -f "$MQTT_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

# Build varpulis if needed
BINARY="$REPO_ROOT/target/release/varpulis"
if [ ! -f "$BINARY" ]; then
    echo "Building varpulis (release)..."
    cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml"
fi

# Create Mosquitto config with WebSocket listener
MQTT_CONF=$(mktemp)
cat > "$MQTT_CONF" << 'EOF'
listener 1883
listener 9001
protocol websockets
allow_anonymous true
EOF

echo "Starting MQTT broker with WebSocket support..."
docker rm -f "$MQTT_CONTAINER" 2>/dev/null || true
docker run -d --name "$MQTT_CONTAINER" \
    -p 1883:1883 \
    -p 9001:9001 \
    -v "$MQTT_CONF:/mosquitto/config/mosquitto.conf:ro" \
    eclipse-mosquitto >/dev/null
rm "$MQTT_CONF"
sleep 2

echo "Starting Varpulis server on port $VARPULIS_PORT..."
"$BINARY" server --port "$VARPULIS_PORT" --api-key "$API_KEY" 2>&1 &
SERVER_PID=$!
sleep 2

echo "Starting web server on port $WEB_PORT..."
cd "$SCRIPT_DIR/web"
python3 -m http.server "$WEB_PORT" --bind 127.0.0.1 &
HTTP_PID=$!

echo ""
echo "=============================================="
echo "  Mandelbrot Web Demo Ready!"
echo "=============================================="
echo ""
echo "  Open: http://localhost:$WEB_PORT"
echo ""
echo "  Click 'Start Computation' to begin"
echo "  Press Ctrl+C to stop"
echo ""

wait
