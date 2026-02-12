#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== E2E Horizontal Scaling + Coordinator HA Test ==="
echo

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
echo "=== Building Varpulis Docker image ==="
docker compose build
echo

# ---------------------------------------------------------------------------
# Start infrastructure + coordinators + workers
# ---------------------------------------------------------------------------
echo "=== Starting cluster (2 coordinators, 3 workers) ==="
docker compose up -d mosquitto coordinator-1 coordinator-2 worker-1 worker-2 worker-3
echo

echo "=== Waiting for coordinator-1 health ==="
for i in $(seq 1 30); do
    if docker compose exec -T coordinator-1 curl -sf http://localhost:9100/health > /dev/null 2>&1; then
        echo "  Coordinator-1 healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: coordinator-1 did not become healthy within 30s"
        docker compose logs coordinator-1
        docker compose down -v
        exit 1
    fi
    sleep 1
done

echo "=== Waiting for coordinator-2 health ==="
for i in $(seq 1 30); do
    if docker compose exec -T coordinator-2 curl -sf http://localhost:9100/health > /dev/null 2>&1; then
        echo "  Coordinator-2 healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: coordinator-2 did not become healthy within 30s"
        docker compose logs coordinator-2
        docker compose down -v
        exit 1
    fi
    sleep 1
done

echo "=== Waiting for workers to register with coordinator-1 ==="
for i in $(seq 1 30); do
    READY=$(docker compose exec -T coordinator-1 curl -sf http://localhost:9100/api/v1/cluster/workers 2>/dev/null \
        | python3 -c "import sys,json; w=json.load(sys.stdin).get('workers',[]); print(sum(1 for x in w if x.get('status','').lower()=='ready'))" 2>/dev/null || echo 0)
    if [ "$READY" -ge 2 ]; then
        echo "  $READY workers ready on coordinator-1"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  WARNING: only $READY/2 workers ready on coordinator-1 after 30s, proceeding anyway"
        break
    fi
    sleep 1
done

echo "=== Waiting for workers to register with coordinator-2 ==="
for i in $(seq 1 30); do
    READY=$(docker compose exec -T coordinator-2 curl -sf http://localhost:9100/api/v1/cluster/workers 2>/dev/null \
        | python3 -c "import sys,json; w=json.load(sys.stdin).get('workers',[]); print(sum(1 for x in w if x.get('status','').lower()=='ready'))" 2>/dev/null || echo 0)
    if [ "$READY" -ge 1 ]; then
        echo "  $READY worker(s) ready on coordinator-2"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  WARNING: only $READY/1 workers ready on coordinator-2 after 30s, proceeding anyway"
        break
    fi
    sleep 1
done

echo "=== Checking WebSocket connections ==="
WS_1=$(docker compose exec -T coordinator-1 curl -sf http://localhost:9100/health 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('ws_connections',0))" 2>/dev/null || echo 0)
WS_2=$(docker compose exec -T coordinator-2 curl -sf http://localhost:9100/health 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('ws_connections',0))" 2>/dev/null || echo 0)
echo "  Coordinator-1 WS connections: $WS_1"
echo "  Coordinator-2 WS connections: $WS_2"
echo

# ---------------------------------------------------------------------------
# Run test driver
# ---------------------------------------------------------------------------
echo "=== Running test driver ==="
EXIT_CODE=0
docker compose run --rm test-driver || EXIT_CODE=$?
echo

# ---------------------------------------------------------------------------
# Collect logs
# ---------------------------------------------------------------------------
echo "=== Collecting logs ==="
mkdir -p results
docker compose logs > results/docker-logs.txt 2>&1
echo "  Saved to results/docker-logs.txt"
echo

# ---------------------------------------------------------------------------
# Tear down
# ---------------------------------------------------------------------------
echo "=== Tearing down ==="
docker compose down -v
echo

if [ "$EXIT_CODE" -eq 0 ]; then
    echo "=== ALL TESTS PASSED ==="
else
    echo "=== TESTS FAILED (exit code $EXIT_CODE) ==="
fi

exit "$EXIT_CODE"
