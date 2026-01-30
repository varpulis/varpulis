#!/bin/bash
#
# Apama vs Varpulis Benchmark
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NUM_EVENTS=${1:-10000}

echo "=============================================="
echo "  Apama vs Varpulis Benchmark"
echo "  Events: $NUM_EVENTS"
echo "=============================================="

# Create data directory
mkdir -p "$SCRIPT_DIR/data"

# Generate events file
echo "Generating $NUM_EVENTS events..."
cat > /tmp/gen_events.py << 'PYTHON'
import sys
import json

count = int(sys.argv[1])
output = sys.argv[2]

with open(output, 'w') as f:
    for i in range(count):
        symbol = "A" if i % 2 == 0 else "B"
        price = 50.0 + (i % 100)
        volume = 1000 + (i % 500)
        evt = {
            "event_type": "Tick",
            "timestamp": f"2024-01-01T00:00:{i % 60:02d}Z",
            "data": {
                "symbol": symbol,
                "price": price,
                "volume": volume
            }
        }
        f.write(json.dumps(evt) + "\n")
PYTHON

python3 /tmp/gen_events.py $NUM_EVENTS "$SCRIPT_DIR/data/events.jsonl"

# Create Varpulis test program
cat > "$SCRIPT_DIR/varpulis/benchmark.vpl" << 'VPL'
event Tick:
    symbol: str
    price: float
    volume: int

# Simple filter - count events with price > 50
stream FilterBench = Tick
    .where(price > 50.0)
    .window(100)
    .aggregate(event_count: count())
    .emit(event_type: "FilterResult", count: event_count)
VPL

# Create event file in Varpulis .evt format
# Format: EventType { field: value, field2: value2 }
echo "Converting to .evt format..."
cat > /tmp/convert_evt.py << 'PYTHON'
import sys
import json

with open(sys.argv[1]) as f:
    lines = f.readlines()

with open(sys.argv[2], 'w') as f:
    for i, line in enumerate(lines):
        evt = json.loads(line)
        d = evt["data"]
        # Varpulis format: EventType { field: value, ... }
        symbol = d["symbol"]
        price = d["price"]
        volume = int(d["volume"])
        f.write(f'Tick {{ symbol: "{symbol}", price: {price}, volume: {volume} }}\n')
PYTHON

python3 /tmp/convert_evt.py "$SCRIPT_DIR/data/events.jsonl" "$SCRIPT_DIR/data/events.evt"

echo ""
echo "=== Varpulis Benchmark ==="

# Build if needed
if [ ! -f "$PROJECT_ROOT/target/release/varpulis" ]; then
    echo "Building Varpulis..."
    cargo build --release -p varpulis-cli --manifest-path "$PROJECT_ROOT/Cargo.toml"
fi

# Run Varpulis benchmark
VARPULIS_START=$(date +%s.%N)
"$PROJECT_ROOT/target/release/varpulis" simulate \
    --program "$SCRIPT_DIR/varpulis/benchmark.vpl" \
    --events "$SCRIPT_DIR/data/events.evt" \
    --immediate 2>&1 | tail -5
VARPULIS_END=$(date +%s.%N)

VARPULIS_ELAPSED=$(echo "$VARPULIS_END - $VARPULIS_START" | bc)
VARPULIS_THROUGHPUT=$(echo "scale=0; $NUM_EVENTS / $VARPULIS_ELAPSED" | bc)

echo "Time: ${VARPULIS_ELAPSED}s"
echo "Throughput: $VARPULIS_THROUGHPUT events/sec"

echo ""
echo "=== Apama Benchmark ==="

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Docker not available, skipping Apama benchmark"
    exit 0
fi

# Stop any existing container
docker rm -f apama-bench 2>/dev/null || true

# Start Apama correlator
echo "Starting Apama correlator..."
docker run -d --name apama-bench \
    -v "$SCRIPT_DIR/apama:/app:ro" \
    public.ecr.aws/apama/apama-correlator:latest \
    correlator -p 15903 --loglevel WARN > /dev/null

# Wait for startup
sleep 3

# Check if correlator is running
if ! docker ps | grep -q apama-bench; then
    echo "Failed to start Apama correlator"
    docker logs apama-bench 2>&1 | tail -20
    exit 1
fi

# Inject monitor
echo "Injecting monitor..."
docker exec apama-bench engine_inject -p 15903 /app/benchmark.mon 2>&1 | grep -v "^$" || true

sleep 1

# Send events
echo "Sending $NUM_EVENTS events..."
APAMA_START=$(date +%s.%N)

# Create event sending script
cat > /tmp/send_apama_events.py << 'PYTHON'
import socket
import sys
import json

count = int(sys.argv[1])
event_file = sys.argv[2]

# Read events
events = []
with open(event_file) as f:
    for line in f:
        evt = json.loads(line)
        d = evt["data"]
        events.append(f'Tick("{d["symbol"]}", {d["price"]}, {int(d["volume"])})')

# Connect to correlator
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 15903))

# Send events via the raw protocol
# Note: This is simplified - real Apama uses a binary protocol
for evt in events:
    msg = f'&FLUSHING(true)\n{evt}\n'
    sock.send(msg.encode())

sock.close()
PYTHON

# Use engine_send for accurate measurement
for i in $(seq 1 100); do
    docker exec apama-bench engine_send -p 15903 -c default \
        "Tick(\"A\", 100.0, 1000)" > /dev/null 2>&1
done

# For larger benchmarks, we need a faster method
# Using engine_send in a loop is slow due to process overhead
# Let's measure what we can

APAMA_END=$(date +%s.%N)
APAMA_ELAPSED=$(echo "$APAMA_END - $APAMA_START" | bc)

# Get correlator stats
docker exec apama-bench engine_inspect -p 15903 2>&1 | grep -E "(numProcessed|numReceived)" || true

echo "Time for 100 events: ${APAMA_ELAPSED}s"
APAMA_THROUGHPUT=$(echo "scale=0; 100 / $APAMA_ELAPSED" | bc)
echo "Throughput: ~$APAMA_THROUGHPUT events/sec (extrapolated)"

# Cleanup
docker stop apama-bench > /dev/null 2>&1
docker rm apama-bench > /dev/null 2>&1

echo ""
echo "=============================================="
echo "  Results Summary"
echo "=============================================="
echo ""
echo "Varpulis: $VARPULIS_THROUGHPUT events/sec ($NUM_EVENTS events)"
echo "Apama:    ~$APAMA_THROUGHPUT events/sec (100 events, via engine_send)"
echo ""
echo "Note: Apama benchmark limited by engine_send overhead."
echo "      For accurate comparison, use Apama's native event injection."
