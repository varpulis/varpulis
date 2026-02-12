#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== E2E Raft Consensus Cluster Test ==="
echo

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
echo "=== Building Varpulis Docker image (with raft feature) ==="
docker compose build
echo

# ---------------------------------------------------------------------------
# Start infrastructure + coordinators + workers
# ---------------------------------------------------------------------------
echo "=== Starting cluster (3 Raft coordinators, 4 workers) ==="
docker compose up -d mosquitto coordinator-1 coordinator-2 coordinator-3 \
    worker-1 worker-2 worker-3 worker-4
echo

# Wait for all 3 coordinators
for N in 1 2 3; do
    echo "=== Waiting for coordinator-${N} health ==="
    for i in $(seq 1 45); do
        if docker compose exec -T "coordinator-${N}" curl -sf http://localhost:9100/health > /dev/null 2>&1; then
            echo "  Coordinator-${N} healthy"
            break
        fi
        if [ "$i" -eq 45 ]; then
            echo "  ERROR: coordinator-${N} did not become healthy within 45s"
            docker compose logs "coordinator-${N}"
            docker compose down -v
            exit 1
        fi
        sleep 1
    done
done

# Wait for Raft leader election
echo "=== Waiting for Raft leader election ==="
for i in $(seq 1 30); do
    LEADER=$(docker compose exec -T coordinator-1 curl -sf http://localhost:9100/raft/metrics 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('current_leader',''))" 2>/dev/null || echo "")
    if [ -n "$LEADER" ] && [ "$LEADER" != "null" ] && [ "$LEADER" != "None" ]; then
        echo "  Raft leader elected: node $LEADER"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  WARNING: Raft leader not detected after 30s"
        for N in 1 2 3; do
            echo "  coordinator-${N} raft metrics:"
            docker compose exec -T "coordinator-${N}" curl -sf http://localhost:9100/raft/metrics 2>/dev/null || echo "  (unreachable)"
        done
    fi
    sleep 1
done

# Wait for workers on each coordinator
echo "=== Waiting for workers to register ==="
for COORD_N in 1 2 3; do
    EXPECTED=2
    [ "$COORD_N" -eq 2 ] && EXPECTED=1
    [ "$COORD_N" -eq 3 ] && EXPECTED=1
    for i in $(seq 1 30); do
        READY=$(docker compose exec -T "coordinator-${COORD_N}" \
            curl -sf "http://localhost:9100/api/v1/cluster/workers" 2>/dev/null \
            | python3 -c "import sys,json; w=json.load(sys.stdin).get('workers',[]); print(sum(1 for x in w if x.get('status','').lower()=='ready'))" 2>/dev/null || echo 0)
        if [ "$READY" -ge "$EXPECTED" ]; then
            echo "  $READY worker(s) ready on coordinator-${COORD_N}"
            break
        fi
        if [ "$i" -eq 30 ]; then
            echo "  WARNING: only $READY/$EXPECTED workers ready on coordinator-${COORD_N} after 30s"
            break
        fi
        sleep 1
    done
done

echo "=== Checking WebSocket connections ==="
for N in 1 2 3; do
    WS=$(docker compose exec -T "coordinator-${N}" curl -sf http://localhost:9100/health 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('ws_connections',0))" 2>/dev/null || echo 0)
    echo "  Coordinator-${N} WS connections: $WS"
done
echo

# ---------------------------------------------------------------------------
# Run test driver
# ---------------------------------------------------------------------------
echo "=== Running Raft E2E test driver ==="
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
    echo "=== ALL RAFT E2E TESTS PASSED ==="
else
    echo "=== RAFT E2E TESTS FAILED (exit code $EXIT_CODE) ==="
fi

exit "$EXIT_CODE"
