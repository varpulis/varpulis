#!/bin/bash
# Distributed Mandelbrot Deployment Script
#
# Starts a coordinator + 4 workers and deploys a pipeline group
# that computes a 1000x1000 Mandelbrot set across 4 processes.
#
# Prerequisites:
#   - MQTT broker running on localhost:1883
#     docker run -d -p 1883:1883 eclipse-mosquitto
#   - Built binary: cargo build --release
#
# Usage:
#   ./examples/mandelbrot/distributed/deploy.sh
#
# To stop: kill the process group or Ctrl+C

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="${SCRIPT_DIR}/../../../target/release/varpulis"
API_KEY="test"
COORDINATOR_PORT=9100
BASE_WORKER_PORT=9000

# Check binary exists
if [ ! -f "$BINARY" ]; then
    echo "Binary not found at $BINARY"
    echo "Run: cargo build --release"
    exit 1
fi

# Cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down..."
    kill $(jobs -p) 2>/dev/null
    wait 2>/dev/null
    echo "Done."
}
trap cleanup EXIT

echo "=== Distributed Mandelbrot ==="
echo ""

# Start coordinator
echo "Starting coordinator on port $COORDINATOR_PORT..."
$BINARY coordinator --port $COORDINATOR_PORT --api-key "$API_KEY" &
sleep 1

# Start 4 workers
for i in 0 1 2 3; do
    PORT=$((BASE_WORKER_PORT + i))
    echo "Starting worker-$i on port $PORT..."
    $BINARY server --port $PORT --api-key "$API_KEY" \
        --coordinator "http://127.0.0.1:$COORDINATOR_PORT" \
        --worker-id "worker-$i" &
done

echo ""
echo "Waiting for workers to register..."
sleep 3

# Check workers registered
echo ""
echo "Registered workers:"
curl -s "http://127.0.0.1:$COORDINATOR_PORT/api/v1/cluster/workers" \
    -H "x-api-key: $API_KEY" | python3 -m json.tool 2>/dev/null || \
    curl -s "http://127.0.0.1:$COORDINATOR_PORT/api/v1/cluster/workers" \
    -H "x-api-key: $API_KEY"
echo ""

# Read VPL sources
W0_SOURCE=$(cat "$SCRIPT_DIR/mandelbrot_worker_0.vpl")
W1_SOURCE=$(cat "$SCRIPT_DIR/mandelbrot_worker_1.vpl")
W2_SOURCE=$(cat "$SCRIPT_DIR/mandelbrot_worker_2.vpl")
W3_SOURCE=$(cat "$SCRIPT_DIR/mandelbrot_worker_3.vpl")

# Deploy pipeline group
echo ""
echo "Deploying pipeline group..."
DEPLOY_RESPONSE=$(curl -s -X POST \
    "http://127.0.0.1:$COORDINATOR_PORT/api/v1/cluster/pipeline-groups" \
    -H "x-api-key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d "$(cat <<EOF
{
    "name": "mandelbrot-distributed",
    "pipelines": [
        {"name": "row0", "source": $(echo "$W0_SOURCE" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'), "worker_affinity": "worker-0"},
        {"name": "row1", "source": $(echo "$W1_SOURCE" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'), "worker_affinity": "worker-1"},
        {"name": "row2", "source": $(echo "$W2_SOURCE" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'), "worker_affinity": "worker-2"},
        {"name": "row3", "source": $(echo "$W3_SOURCE" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'), "worker_affinity": "worker-3"}
    ],
    "routes": [
        {"from_pipeline": "_external", "to_pipeline": "row0", "event_types": ["ComputeTile0*"]},
        {"from_pipeline": "_external", "to_pipeline": "row1", "event_types": ["ComputeTile1*"]},
        {"from_pipeline": "_external", "to_pipeline": "row2", "event_types": ["ComputeTile2*"]},
        {"from_pipeline": "_external", "to_pipeline": "row3", "event_types": ["ComputeTile3*"]}
    ]
}
EOF
)")

echo "$DEPLOY_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$DEPLOY_RESPONSE"

# Extract group ID
GROUP_ID=$(echo "$DEPLOY_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null)

if [ -z "$GROUP_ID" ]; then
    echo "ERROR: Failed to deploy pipeline group"
    exit 1
fi

echo ""
echo "Pipeline group deployed: $GROUP_ID"
echo ""

# Inject events
echo "Injecting 16 ComputeTile events..."
START_TIME=$(date +%s%N)

for row in 0 1 2 3; do
    for col in 0 1 2 3; do
        curl -s -X POST \
            "http://127.0.0.1:$COORDINATOR_PORT/api/v1/cluster/pipeline-groups/$GROUP_ID/inject" \
            -H "x-api-key: $API_KEY" \
            -H "Content-Type: application/json" \
            -d "{\"event_type\": \"ComputeTile${row}${col}\", \"fields\": {}}" > /dev/null &
    done
done

wait

END_TIME=$(date +%s%N)
ELAPSED=$(( (END_TIME - START_TIME) / 1000000 ))

echo "All 16 events injected in ${ELAPSED}ms"
echo ""
echo "Topology:"
curl -s "http://127.0.0.1:$COORDINATOR_PORT/api/v1/cluster/topology" \
    -H "x-api-key: $API_KEY" | python3 -m json.tool 2>/dev/null || \
    curl -s "http://127.0.0.1:$COORDINATOR_PORT/api/v1/cluster/topology" \
    -H "x-api-key: $API_KEY"

echo ""
echo "Cluster is running. Press Ctrl+C to stop."
wait
