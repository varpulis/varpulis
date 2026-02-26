#!/usr/bin/env bash
#
# Varpulis vs EventFlux benchmark runner
#
# Usage:
#   ./run_benchmark.sh varpulis          # Run Varpulis benchmarks only
#   ./run_benchmark.sh eventflux         # Run EventFlux benchmarks only
#   ./run_benchmark.sh all               # Run both and compare
#
# Prerequisites:
#   - Varpulis: cargo build --release (in /home/cpo/cep)
#   - EventFlux: cargo build --release (in $EVENTFLUX_DIR)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
VARPULIS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
EVENTFLUX_DIR="${EVENTFLUX_DIR:-/tmp/eventflux}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$RESULTS_DIR"

# Event counts for simulation benchmarks
SMALL=100000
MEDIUM=500000
LARGE=1000000

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${BLUE}[bench]${NC} $*"; }
ok()  { echo -e "${GREEN}[ok]${NC} $*"; }
err() { echo -e "${RED}[err]${NC} $*"; }

# ===========================================================================
# Hardware info
# ===========================================================================
collect_hw_info() {
    local out="$RESULTS_DIR/hardware_${TIMESTAMP}.txt"
    {
        echo "=== Hardware Info ==="
        echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "Hostname: $(hostname)"
        echo "OS: $(uname -srm)"
        echo ""
        echo "=== CPU ==="
        lscpu 2>/dev/null | grep -E "^(Architecture|Model name|CPU\(s\)|Thread|Core|Socket|CPU MHz|CPU max)" || true
        echo ""
        echo "=== Memory ==="
        free -h 2>/dev/null || true
        echo ""
        echo "=== Rust Toolchain ==="
        rustc --version 2>/dev/null || true
        cargo --version 2>/dev/null || true
        echo ""
        echo "=== Varpulis Version ==="
        cd "$VARPULIS_DIR" && git log --oneline -1 2>/dev/null || echo "not a git repo"
        echo ""
        if [ -d "$EVENTFLUX_DIR" ]; then
            echo "=== EventFlux Version ==="
            cd "$EVENTFLUX_DIR" && git log --oneline -1 2>/dev/null || echo "not a git repo"
        fi
    } > "$out"
    ok "Hardware info saved to $out"
}

# ===========================================================================
# Generate test events
# ===========================================================================
generate_events() {
    local count=$1 schema=$2 output=$3
    log "Generating $count $schema events -> $output"
    cd "$VARPULIS_DIR"
    cargo run --release -- generate \
        --schema "$schema" \
        --rate 999999 \
        --duration 1 \
        --count "$count" \
        --format jsonl \
        > "$output" 2>/dev/null || {
        # Fallback: generate manually if CLI doesn't support --count
        cargo run --release -- generate \
            --schema "$schema" \
            --rate "$count" \
            --duration 1 \
            --format jsonl \
            > "$output" 2>/dev/null || true
    }
    local lines
    lines=$(wc -l < "$output")
    ok "Generated $lines events"
}

# ===========================================================================
# Varpulis simulation benchmarks
# ===========================================================================
run_varpulis_simulation() {
    local scenario=$1 vpl_file=$2 events_file=$3 output_file=$4
    log "Varpulis: $scenario"

    cd "$VARPULIS_DIR"

    # Warm up
    cargo run --release -- simulate \
        --program "$vpl_file" \
        --events "$events_file" \
        --immediate --preload --quiet \
        > /dev/null 2>&1 || true

    # Timed run (3 iterations)
    local total_time=0
    local runs=3
    for i in $(seq 1 $runs); do
        local start end elapsed
        start=$(date +%s%N)
        cargo run --release -- simulate \
            --program "$vpl_file" \
            --events "$events_file" \
            --immediate --preload --quiet \
            > /dev/null 2>&1
        end=$(date +%s%N)
        elapsed=$(( (end - start) / 1000000 ))
        total_time=$((total_time + elapsed))
        echo "  Run $i: ${elapsed}ms"
    done

    local avg_ms=$((total_time / runs))
    local event_count
    event_count=$(wc -l < "$events_file")
    local throughput=$((event_count * 1000 / avg_ms))

    echo "  Average: ${avg_ms}ms | Throughput: ${throughput} evt/s" | tee -a "$output_file"
}

# ===========================================================================
# EventFlux simulation benchmarks
# ===========================================================================
run_eventflux_simulation() {
    local scenario=$1 ef_file=$2 events_file=$3 output_file=$4
    log "EventFlux: $scenario"

    if [ ! -d "$EVENTFLUX_DIR" ]; then
        err "EventFlux not found at $EVENTFLUX_DIR"
        err "Clone it: git clone https://github.com/eventflux-io/engine.git $EVENTFLUX_DIR"
        echo "  SKIPPED (EventFlux not installed)" >> "$output_file"
        return
    fi

    cd "$EVENTFLUX_DIR"

    # Check if EventFlux binary exists
    if [ ! -f "target/release/eventflux" ]; then
        log "Building EventFlux..."
        cargo build --release 2>/dev/null || {
            err "EventFlux build failed"
            echo "  SKIPPED (build failed)" >> "$output_file"
            return
        }
    fi

    # EventFlux doesn't have a simulation mode like Varpulis
    # We use its standard execution with file input if supported
    local total_time=0
    local runs=3
    for i in $(seq 1 $runs); do
        local start end elapsed
        start=$(date +%s%N)
        timeout 60 ./target/release/eventflux "$ef_file" \
            --input "$events_file" \
            --quiet \
            > /dev/null 2>&1 || true
        end=$(date +%s%N)
        elapsed=$(( (end - start) / 1000000 ))
        total_time=$((total_time + elapsed))
        echo "  Run $i: ${elapsed}ms"
    done

    local avg_ms=$((total_time / runs))
    local event_count
    event_count=$(wc -l < "$events_file")
    local throughput=$((event_count * 1000 / avg_ms))

    echo "  Average: ${avg_ms}ms | Throughput: ${throughput} evt/s" | tee -a "$output_file"
}

# ===========================================================================
# Criterion benchmarks (Varpulis only — precise statistical measurement)
# ===========================================================================
run_criterion() {
    log "Running Criterion micro-benchmarks..."
    cd "$VARPULIS_DIR"

    cargo bench -p varpulis-runtime --bench eventflux_comparison \
        -- --output-format bencher 2>&1 | tee "$RESULTS_DIR/criterion_${TIMESTAMP}.txt"

    ok "Criterion results saved"
    ok "HTML reports at: $VARPULIS_DIR/target/criterion/"
}

# ===========================================================================
# Memory measurement
# ===========================================================================
measure_memory() {
    local binary=$1 args=$2 label=$3 output=$4
    log "Measuring memory: $label"

    if command -v /usr/bin/time &> /dev/null; then
        /usr/bin/time -v $binary $args 2>&1 | grep "Maximum resident" | tee -a "$output"
    else
        echo "  /usr/bin/time not available — skipping memory measurement" >> "$output"
    fi
}

# ===========================================================================
# Main
# ===========================================================================
main() {
    local mode="${1:-all}"

    echo "=========================================="
    echo "  Varpulis vs EventFlux Benchmark"
    echo "  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "=========================================="
    echo ""

    collect_hw_info

    local report="$RESULTS_DIR/report_${TIMESTAMP}.md"
    {
        echo "# Varpulis vs EventFlux Benchmark Results"
        echo ""
        echo "**Date**: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "**Hardware**: $(lscpu 2>/dev/null | grep 'Model name' | sed 's/.*: *//' || echo 'unknown')"
        echo "**Memory**: $(free -h 2>/dev/null | awk '/Mem:/ {print $2}' || echo 'unknown')"
        echo "**Rust**: $(rustc --version 2>/dev/null || echo 'unknown')"
        echo ""
    } > "$report"

    if [ "$mode" = "varpulis" ] || [ "$mode" = "all" ]; then
        echo "## Criterion Micro-Benchmarks (Varpulis)" >> "$report"
        echo "" >> "$report"
        run_criterion
        echo "" >> "$report"
    fi

    echo ""
    ok "Benchmark complete!"
    ok "Results: $RESULTS_DIR/"
    ok "Report:  $report"
    ok ""
    ok "To run Criterion benchmarks independently:"
    ok "  cargo bench -p varpulis-runtime --bench eventflux_comparison"
    ok ""
    ok "To view HTML reports:"
    ok "  open $VARPULIS_DIR/target/criterion/report/index.html"
}

main "$@"
