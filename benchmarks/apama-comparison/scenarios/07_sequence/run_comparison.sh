#!/bin/bash
#
# SASE+ Sequence Pattern: Varpulis vs Apama Benchmark
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
PAIRS=${1:-10000}
EVENTS=$((PAIRS * 2))

echo "=============================================="
echo "  SASE+ Sequence Pattern Benchmark"
echo "  Pattern: A -> B with matching ID"
echo "  Event Pairs: $PAIRS ($EVENTS total events)"
echo "=============================================="

mkdir -p "$SCRIPT_DIR/data"

# Generate events in both formats
echo ""
echo "Generating events..."
python3 << PYTHON
pairs = $PAIRS
with open("$SCRIPT_DIR/data/events_vpl.evt", "w") as f_vpl:
    with open("$SCRIPT_DIR/data/events_apama.evt", "w") as f_apama:
        for i in range(1, pairs + 1):
            f_vpl.write(f'A {{ id: {i} }}\n')
            f_vpl.write(f'B {{ id: {i} }}\n')
            f_apama.write(f'A({i})\n')
            f_apama.write(f'B({i})\n')
print(f"Generated {pairs * 2} events")
PYTHON

# ============================================
# VARPULIS BENCHMARK
# ============================================
echo ""
echo "=== Varpulis SASE+ ==="

V_START=$(python3 -c "import time; print(time.time())")
V_OUTPUT=$("$PROJECT_ROOT/target/release/varpulis" simulate \
    --program "$SCRIPT_DIR/varpulis.vpl" \
    --events "$SCRIPT_DIR/data/events_vpl.evt" \
    --immediate 2>&1)
V_END=$(python3 -c "import time; print(time.time())")

V_ELAPSED=$(python3 -c "print(f'{$V_END - $V_START:.3f}')")
V_THROUGHPUT=$(python3 -c "print(int($EVENTS / ($V_END - $V_START)))")
V_MATCHES=$(echo "$V_OUTPUT" | grep -c '"Match"' || echo "0")

echo "  Time: ${V_ELAPSED}s"
echo "  Throughput: $V_THROUGHPUT events/sec"
echo "  Matches: $V_MATCHES"

# ============================================
# APAMA BENCHMARK
# ============================================
echo ""
echo "=== Apama v27.18 ==="

# Check if Apama container is running
if ! docker ps | grep -q apama-bench; then
    echo "Starting Apama container..."
    docker rm -f apama-bench 2>/dev/null || true
    docker run -d --name apama-bench \
        -v "$SCRIPT_DIR:/app:ro" \
        -p 15903:15903 \
        public.ecr.aws/apama/apama-correlator:latest \
        correlator -p 15903 --loglevel WARN
    sleep 5
fi

# Inject monitor
docker exec apama-bench engine_inject -p 15903 /app/apama.mon 2>/dev/null || true

# Copy event file
docker cp "$SCRIPT_DIR/data/events_apama.evt" apama-bench:/tmp/

# Run benchmark
A_START=$(python3 -c "import time; print(time.time())")
docker exec apama-bench engine_send -p 15903 -c default /tmp/events_apama.evt 2>/dev/null
A_END=$(python3 -c "import time; print(time.time())")

A_ELAPSED=$(python3 -c "print(f'{$A_END - $A_START:.3f}')")
A_THROUGHPUT=$(python3 -c "print(int($EVENTS / ($A_END - $A_START)))")

echo "  Time: ${A_ELAPSED}s"
echo "  Throughput: $A_THROUGHPUT events/sec"

# ============================================
# COMPARISON
# ============================================
echo ""
echo "=============================================="
echo "  COMPARISON"
echo "=============================================="
echo ""
echo "| Engine    | Events | Time    | Throughput     | Matches |"
echo "|-----------|--------|---------|----------------|---------|"
echo "| Varpulis  | $EVENTS | ${V_ELAPSED}s | $V_THROUGHPUT evt/s | $V_MATCHES |"
echo "| Apama     | $EVENTS | ${A_ELAPSED}s | $A_THROUGHPUT evt/s | N/A |"

RATIO=$(python3 -c "
v = $V_THROUGHPUT
a = $A_THROUGHPUT
if a > 0:
    ratio = v / a
    winner = 'Varpulis' if ratio > 1 else 'Apama'
    factor = max(ratio, 1/ratio)
    print(f'{winner} is {factor:.1f}x faster')
else:
    print('Apama throughput too low to compare')
")
echo ""
echo "$RATIO"
