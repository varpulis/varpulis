#!/usr/bin/env bash
# scripts/pgo-evaluate.sh — Compare baseline vs PGO+BOLT performance
#
# Usage:
#   ./scripts/pgo-evaluate.sh              # Full evaluation (build + bench + simulate)
#   PGO_EVAL_RUNS=3 ./scripts/pgo-evaluate.sh  # Fewer simulation runs (faster)
#   PGO_SKIP_CRITERION=1 ./scripts/pgo-evaluate.sh  # Skip Criterion, simulation only
#
# Output:
#   target/pgo-evaluation/report.md   — Markdown comparison report
#   target/pgo-evaluation/results/    — Raw result files

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# --- Configuration ---
RUNS="${PGO_EVAL_RUNS:-5}"
RESULTS_DIR="target/pgo-evaluation"
RESULTS_RAW="$RESULTS_DIR/results"
REPORT_FILE="$RESULTS_DIR/report.md"
SCENARIOS_DIR="benchmarks/apama-comparison/scenarios"
DATA_DIR="benchmarks/apama-comparison/data"
PACKAGE="varpulis-cli"
BINARY_NAME="varpulis"
BENCH_PACKAGE="varpulis-runtime"

CRITERION_BENCHES=(
    pattern_benchmark
    kleene_benchmark
    hamlet_zdd_benchmark
    pst_forecast_benchmark
    context_benchmark
)

SIMULATION_SCENARIOS=(
    01_filter
    02_aggregation
    03_temporal
    04_kleene
    05_ema_crossover
    06_multi_sensor
    07_sequence
)

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${BLUE}[EVAL]${NC} $*"; }
ok()    { echo -e "${GREEN}[EVAL]${NC} $*"; }
warn()  { echo -e "${YELLOW}[EVAL]${NC} $*"; }
err()   { echo -e "${RED}[EVAL]${NC} $*" >&2; }

# --- Helpers ---

# Parse "Event rate: N events/sec" from varpulis simulate output (strip ANSI codes)
parse_throughput() {
    sed 's/\x1b\[[0-9;]*m//g' | grep -oP 'Event rate:\s+\K[\d.]+' || echo "0"
}

# Compute median from a file of numbers (one per line)
median() {
    sort -n "$1" | awk '{a[NR]=$1} END {
        if (NR%2==1) print a[(NR+1)/2]
        else print (a[NR/2]+a[NR/2+1])/2
    }'
}

# Format number with commas
fmt_num() {
    printf "%'.0f" "$1" 2>/dev/null || echo "$1"
}

# --- Setup ---
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_RAW"

RUST_VERSION="$(rustc --version)"
PLATFORM="$(rustc -vV | grep host | cut -d' ' -f2)"
DATE="$(date -u +%Y-%m-%d)"

info "PGO Performance Evaluation"
info "  Date:     $DATE"
info "  Platform: $PLATFORM"
info "  Rust:     $RUST_VERSION"
info "  Sim runs: $RUNS per scenario per binary"
echo ""

# =========================================================================
# Phase 1: Build baseline
# =========================================================================
info "Phase 1: Building baseline binary..."
cargo build --release -p "$PACKAGE" 2>&1 | tail -1

BASELINE_BIN="target/release/$BINARY_NAME"
if [[ ! -x "$BASELINE_BIN" ]]; then
    err "Baseline binary not found"
    exit 1
fi

# Copy baseline binary so PGO build doesn't overwrite it
BASELINE_COPY="$RESULTS_DIR/varpulis-baseline"
cp "$BASELINE_BIN" "$BASELINE_COPY"
BASELINE_SIZE=$(stat -c%s "$BASELINE_COPY")
ok "Baseline: $BASELINE_COPY ($(du -h "$BASELINE_COPY" | cut -f1))"

# =========================================================================
# Phase 2: Build PGO+BOLT
# =========================================================================
info "Phase 2: Building PGO+BOLT binary..."
"$SCRIPT_DIR/pgo-build.sh" 2>&1

PGO_BIN="target/release/$BINARY_NAME"
PGO_COPY="$RESULTS_DIR/varpulis-pgo"
cp "$PGO_BIN" "$PGO_COPY"
PGO_SIZE=$(stat -c%s "$PGO_COPY")

BOLT_BIN="target/release/${BINARY_NAME}-pgo-bolt"
BOLT_COPY=""
BOLT_SIZE=0
if [[ -x "$BOLT_BIN" ]]; then
    BOLT_COPY="$RESULTS_DIR/varpulis-pgo-bolt"
    cp "$BOLT_BIN" "$BOLT_COPY"
    BOLT_SIZE=$(stat -c%s "$BOLT_COPY")
    ok "PGO+BOLT: $BOLT_COPY ($(du -h "$BOLT_COPY" | cut -f1))"
fi
ok "PGO: $PGO_COPY ($(du -h "$PGO_COPY" | cut -f1))"

# =========================================================================
# Phase 3: Criterion microbenchmarks
# =========================================================================
if [[ -z "${PGO_SKIP_CRITERION:-}" ]]; then
    info "Phase 3: Running Criterion benchmarks..."

    # 3a: Baseline benchmarks
    info "  Criterion: baseline run..."
    for bench in "${CRITERION_BENCHES[@]}"; do
        info "    $bench"
        cargo bench -p "$BENCH_PACKAGE" --bench "$bench" \
            -- --save-baseline baseline 2>&1 | tail -1 || true
    done

    # 3b: PGO benchmarks (rebuild runtime with PGO flags)
    info "  Criterion: PGO run..."
    MERGED_PROFILE="$(pwd)/target/pgo-merged.profdata"
    if [[ -f "$MERGED_PROFILE" ]]; then
        for bench in "${CRITERION_BENCHES[@]}"; do
            info "    $bench"
            RUSTFLAGS="-Cprofile-use=$MERGED_PROFILE" \
                cargo bench -p "$BENCH_PACKAGE" --bench "$bench" \
                    -- --save-baseline pgo 2>&1 | tail -1 || true
        done
    else
        warn "  Skipping PGO criterion run (no merged profile found)"
    fi

    # 3c: Compare with critcmp (if available)
    if command -v critcmp &>/dev/null; then
        info "  Generating Criterion comparison..."
        critcmp baseline pgo > "$RESULTS_DIR/criterion-comparison.txt" 2>/dev/null || true
    else
        warn "  critcmp not installed. Install with: cargo install critcmp"
        warn "  You can compare manually: critcmp baseline pgo"
    fi
else
    info "Phase 3: Skipping Criterion benchmarks (PGO_SKIP_CRITERION set)"
fi

# =========================================================================
# Phase 4: End-to-end CLI simulation
# =========================================================================
info "Phase 4: Running simulation benchmarks ($RUNS runs each)..."

# Build list of binaries to test
declare -A BINARIES
BINARIES[baseline]="$BASELINE_COPY"
BINARIES[pgo]="$PGO_COPY"
if [[ -n "$BOLT_COPY" ]]; then
    BINARIES[pgo-bolt]="$BOLT_COPY"
fi

for label in baseline pgo ${BOLT_COPY:+pgo-bolt}; do
    bin="${BINARIES[$label]}"
    info "  Binary: $label"

    for scenario in "${SIMULATION_SCENARIOS[@]}"; do
        vpl_file="$SCENARIOS_DIR/$scenario/varpulis.vpl"
        evt_file="$DATA_DIR/${scenario}_bench.evt"

        if [[ ! -f "$vpl_file" || ! -f "$evt_file" ]]; then
            warn "    Skipping $scenario (missing files)"
            continue
        fi

        result_file="$RESULTS_RAW/${label}_${scenario}.txt"
        > "$result_file"

        for run in $(seq 1 "$RUNS"); do
            throughput=$("$bin" simulate \
                -p "$vpl_file" -e "$evt_file" -q -w 1 2>&1 | parse_throughput)
            echo "$throughput" >> "$result_file"
        done

        med=$(median "$result_file")
        info "    $scenario: $(fmt_num "$med") evt/s (median of $RUNS)"
    done
done

# =========================================================================
# Phase 5: Generate report
# =========================================================================
info "Phase 5: Generating report..."

{
    echo "# PGO+BOLT Performance Evaluation"
    echo ""
    echo "- **Date:** $DATE"
    echo "- **Platform:** $PLATFORM"
    echo "- **Rust:** $RUST_VERSION"
    echo "- **Simulation runs:** $RUNS (median reported)"
    echo ""

    # Binary sizes
    echo "## Binary Size"
    echo ""
    echo "| Binary | Size |"
    echo "|--------|------|"
    echo "| Baseline | $(du -h "$BASELINE_COPY" | cut -f1) |"
    echo "| PGO | $(du -h "$PGO_COPY" | cut -f1) |"
    if [[ -n "$BOLT_COPY" ]]; then
        echo "| PGO+BOLT | $(du -h "$BOLT_COPY" | cut -f1) |"
    fi
    echo ""

    # Criterion comparison
    if [[ -f "$RESULTS_DIR/criterion-comparison.txt" ]]; then
        echo "## Criterion Microbenchmarks"
        echo ""
        echo '```'
        cat "$RESULTS_DIR/criterion-comparison.txt"
        echo '```'
        echo ""
    fi

    # Simulation results
    echo "## End-to-End Simulation"
    echo ""

    # Header
    header="| Scenario | Baseline (evt/s) | PGO (evt/s) | PGO vs Base"
    separator="|----------|-----------------|-------------|------------"
    if [[ -n "$BOLT_COPY" ]]; then
        header="$header | PGO+BOLT (evt/s) | BOLT vs Base |"
        separator="$separator|------------------|--------------|"
    else
        header="$header |"
        separator="$separator|"
    fi
    echo "$header"
    echo "$separator"

    for scenario in "${SIMULATION_SCENARIOS[@]}"; do
        baseline_file="$RESULTS_RAW/baseline_${scenario}.txt"
        pgo_file="$RESULTS_RAW/pgo_${scenario}.txt"

        if [[ ! -f "$baseline_file" || ! -f "$pgo_file" ]]; then
            continue
        fi

        base_med=$(median "$baseline_file")
        pgo_med=$(median "$pgo_file")

        # Calculate PGO speedup
        if (( $(echo "$base_med > 0" | bc -l) )); then
            pgo_pct=$(echo "scale=1; ($pgo_med - $base_med) / $base_med * 100" | bc -l)
            if (( $(echo "$pgo_pct >= 0" | bc -l) )); then
                pgo_delta="+${pgo_pct}%"
            else
                pgo_delta="${pgo_pct}%"
            fi
        else
            pgo_delta="N/A"
        fi

        row="| $scenario | $(fmt_num "$base_med") | $(fmt_num "$pgo_med") | **$pgo_delta**"

        if [[ -n "$BOLT_COPY" ]]; then
            bolt_file="$RESULTS_RAW/pgo-bolt_${scenario}.txt"
            if [[ -f "$bolt_file" ]]; then
                bolt_med=$(median "$bolt_file")
                if (( $(echo "$base_med > 0" | bc -l) )); then
                    bolt_pct=$(echo "scale=1; ($bolt_med - $base_med) / $base_med * 100" | bc -l)
                    if (( $(echo "$bolt_pct >= 0" | bc -l) )); then
                        bolt_delta="+${bolt_pct}%"
                    else
                        bolt_delta="${bolt_pct}%"
                    fi
                else
                    bolt_delta="N/A"
                fi
                row="$row | $(fmt_num "$bolt_med") | **$bolt_delta** |"
            else
                row="$row | — | — |"
            fi
        else
            row="$row |"
        fi

        echo "$row"
    done

    echo ""

    # Geometric mean of speedups
    echo "### Summary"
    echo ""
    pgo_speedups=()
    for scenario in "${SIMULATION_SCENARIOS[@]}"; do
        baseline_file="$RESULTS_RAW/baseline_${scenario}.txt"
        pgo_file="$RESULTS_RAW/pgo_${scenario}.txt"
        if [[ -f "$baseline_file" && -f "$pgo_file" ]]; then
            base_med=$(median "$baseline_file")
            pgo_med=$(median "$pgo_file")
            if (( $(echo "$base_med > 0" | bc -l) )); then
                ratio=$(echo "scale=4; $pgo_med / $base_med" | bc -l)
                pgo_speedups+=("$ratio")
            fi
        fi
    done

    if [[ ${#pgo_speedups[@]} -gt 0 ]]; then
        # Geometric mean
        product="1"
        for r in "${pgo_speedups[@]}"; do
            product=$(echo "scale=6; $product * $r" | bc -l)
        done
        n=${#pgo_speedups[@]}
        geo_mean=$(echo "scale=4; e(l($product)/$n)" | bc -l)
        geo_pct=$(echo "scale=1; ($geo_mean - 1) * 100" | bc -l)
        echo "- **PGO geometric mean speedup:** ${geo_pct}% across $n scenarios"
    fi

    if [[ -n "$BOLT_COPY" ]]; then
        bolt_speedups=()
        for scenario in "${SIMULATION_SCENARIOS[@]}"; do
            baseline_file="$RESULTS_RAW/baseline_${scenario}.txt"
            bolt_file="$RESULTS_RAW/pgo-bolt_${scenario}.txt"
            if [[ -f "$baseline_file" && -f "$bolt_file" ]]; then
                base_med=$(median "$baseline_file")
                bolt_med=$(median "$bolt_file")
                if (( $(echo "$base_med > 0" | bc -l) )); then
                    ratio=$(echo "scale=4; $bolt_med / $base_med" | bc -l)
                    bolt_speedups+=("$ratio")
                fi
            fi
        done

        if [[ ${#bolt_speedups[@]} -gt 0 ]]; then
            product="1"
            for r in "${bolt_speedups[@]}"; do
                product=$(echo "scale=6; $product * $r" | bc -l)
            done
            n=${#bolt_speedups[@]}
            geo_mean=$(echo "scale=4; e(l($product)/$n)" | bc -l)
            geo_pct=$(echo "scale=1; ($geo_mean - 1) * 100" | bc -l)
            echo "- **PGO+BOLT geometric mean speedup:** ${geo_pct}% across $n scenarios"
        fi
    fi

    echo ""
} > "$REPORT_FILE"

ok "Report written to: $REPORT_FILE"
echo ""
cat "$REPORT_FILE"
