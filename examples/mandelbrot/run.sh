#!/usr/bin/env bash
# Run the Mandelbrot demo end-to-end.
#
# Starts an MQTT broker (via Docker), the Python renderer, the Varpulis server,
# and deploys mandelbrot.vpl. The renderer saves mandelbrot.png when all
# 1,000,000 pixels (16 tiles x 250x250) have been received.
#
# Prerequisites:
#   - Docker
#   - Python 3.8+ with: pip install -r examples/mandelbrot/requirements.txt
#
# Usage:
#   ./examples/mandelbrot/run.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PORT=9000
API_KEY=test
MQTT_PORT=1883
MQTT_CONTAINER=mandelbrot-mosquitto

cleanup() {
    echo ""
    echo "Cleaning up..."
    [ -n "${RENDERER_PID:-}" ] && kill "$RENDERER_PID" 2>/dev/null || true
    [ -n "${SERVER_PID:-}" ] && kill "$SERVER_PID" 2>/dev/null || true
    docker rm -f "$MQTT_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

# 1. Build varpulis if needed
BINARY="$REPO_ROOT/target/release/varpulis"
if [ ! -f "$BINARY" ]; then
    echo "Building varpulis (release)..."
    cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml"
fi

# 2. Start MQTT broker
echo "Starting MQTT broker..."
docker rm -f "$MQTT_CONTAINER" 2>/dev/null || true
docker run -d --name "$MQTT_CONTAINER" -p "$MQTT_PORT:1883" eclipse-mosquitto >/dev/null
sleep 1

# 3. Start the Python renderer
echo "Starting renderer (saves to $SCRIPT_DIR/mandelbrot.png)..."
python3 "$SCRIPT_DIR/render.py" --output "$SCRIPT_DIR/mandelbrot.png" &
RENDERER_PID=$!
sleep 1

# 4. Start the Varpulis server
echo "Starting Varpulis server on port $PORT..."
"$BINARY" server --port "$PORT" --api-key "$API_KEY" &
SERVER_PID=$!
sleep 2

# 5. Deploy mandelbrot.vpl
echo "Deploying mandelbrot.vpl (16 tiles, 1000x1000, 256 max iterations)..."
START=$(date +%s%N)

# Build JSON payload with proper escaping via python
JSON_PAYLOAD=$(python3 -c "
import json, sys
with open('$SCRIPT_DIR/mandelbrot.vpl') as f:
    source = f.read()
print(json.dumps({'name': 'mandelbrot', 'source': source}))
")

curl -s -X POST "http://localhost:$PORT/api/v1/pipelines" \
    -H "x-api-key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d "$JSON_PAYLOAD"

echo ""
echo "Program deployed. Waiting for renderer to finish..."

# 6. Wait for the renderer to complete
wait "$RENDERER_PID" 2>/dev/null || true

END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
ELAPSED_S=$(echo "scale=2; $ELAPSED_MS / 1000" | bc)

echo ""
echo "Done! Total time: ${ELAPSED_S}s"
echo "Image saved to: $SCRIPT_DIR/mandelbrot.png"
