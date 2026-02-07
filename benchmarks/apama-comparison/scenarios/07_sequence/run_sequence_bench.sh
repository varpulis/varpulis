#!/bin/bash
#
# SASE+ Sequence Benchmark
# Tests A -> B sequence pattern matching correctness and performance
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
NUM_PAIRS=${1:-1000}

echo "=============================================="
echo "  SASE+ Sequence Pattern Benchmark"
echo "  Event Pairs: $NUM_PAIRS"
echo "=============================================="

# Create data directory
mkdir -p "$SCRIPT_DIR/data"

# Generate A/B event pairs
echo "Generating $NUM_PAIRS A/B event pairs..."
cat > /tmp/gen_seq_events.py << 'PYTHON'
import sys
count = int(sys.argv[1])
output = sys.argv[2]

with open(output, 'w') as f:
    for i in range(1, count + 1):
        # Write A event followed by matching B event
        f.write(f'A {{ id: {i} }}\n')
        f.write(f'B {{ id: {i} }}\n')

print(f"Generated {count * 2} events ({count} pairs)")
PYTHON

python3 /tmp/gen_seq_events.py $NUM_PAIRS "$SCRIPT_DIR/data/sequence_events.evt"

echo ""
echo "=== Running Varpulis SASE+ ==="

# Build if needed
if [ ! -f "$PROJECT_ROOT/target/release/varpulis" ]; then
    echo "Building Varpulis..."
    cargo build --release -p varpulis-cli --manifest-path "$PROJECT_ROOT/Cargo.toml"
fi

# Run Varpulis with simulate command to see actual outputs
VARPULIS_OUTPUT=$("$PROJECT_ROOT/target/release/varpulis" simulate \
    --program "$SCRIPT_DIR/varpulis.vpl" \
    --events "$SCRIPT_DIR/data/sequence_events.evt" \
    --immediate 2>&1)

# Count matches
MATCH_COUNT=$(echo "$VARPULIS_OUTPUT" | grep -c "Match" || true)

echo ""
echo "=== Results ==="
echo "Input: $NUM_PAIRS event pairs"
echo "Matches found: $MATCH_COUNT"
echo ""

if [ "$MATCH_COUNT" -eq "$NUM_PAIRS" ]; then
    echo "SUCCESS: All $NUM_PAIRS sequences detected correctly!"
else
    echo "FAILURE: Expected $NUM_PAIRS matches, got $MATCH_COUNT"
    echo ""
    echo "Output sample:"
    echo "$VARPULIS_OUTPUT" | head -30
fi
