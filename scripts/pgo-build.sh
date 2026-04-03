#!/usr/bin/env bash
# scripts/pgo-build.sh — Build PGO+BOLT optimized varpulis binary
#
# Usage:
#   ./scripts/pgo-build.sh              # Full PGO + optional BOLT
#   PGO_SKIP_BOLT=1 ./scripts/pgo-build.sh  # PGO only, skip BOLT
#
# Prerequisites:
#   rustup component add llvm-tools      # for llvm-profdata
#   sudo apt install llvm-19             # optional, for BOLT (llvm-bolt-19)
#
# Output:
#   target/release/varpulis              — PGO-optimized binary
#   target/release/varpulis-pgo-bolt     — PGO+BOLT binary (if BOLT available)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# --- Configuration ---
PROFILE_DIR="${PGO_PROFILE_DIR:-target/pgo-profiles}"
MERGED_PROFILE="${PGO_MERGED_PROFILE:-target/pgo-merged.profdata}"
PACKAGE="varpulis-cli"
BINARY_NAME="varpulis"
BENCH_PACKAGE="varpulis-runtime"
SCENARIOS_DIR="benchmarks/apama-comparison/scenarios"
DATA_DIR="benchmarks/apama-comparison/data"

# Criterion: reduced iterations for profile collection
CRITERION_SAMPLE_SIZE="${PGO_CRITERION_SAMPLES:-10}"

# BOLT training scenarios (subset for faster profiling)
BOLT_SCENARIOS="01_filter 04_kleene 07_sequence"

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[PGO]${NC} $*"; }
ok()    { echo -e "${GREEN}[PGO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[PGO]${NC} $*"; }
err()   { echo -e "${RED}[PGO]${NC} $*" >&2; }

# --- Step 0: Locate tools ---
info "Locating LLVM tools..."

SYSROOT="$(rustc --print sysroot)"
HOST="$(rustc -vV | grep host | cut -d' ' -f2)"
LLVM_PROFDATA="$SYSROOT/lib/rustlib/$HOST/bin/llvm-profdata"

if [[ ! -x "$LLVM_PROFDATA" ]]; then
    err "llvm-profdata not found. Install with: rustup component add llvm-tools"
    exit 1
fi
ok "llvm-profdata: $LLVM_PROFDATA"

# Detect BOLT (optional)
LLVM_BOLT=""
MERGE_FDATA=""
if [[ -z "${PGO_SKIP_BOLT:-}" ]]; then
    for cmd in llvm-bolt llvm-bolt-19 llvm-bolt-18 llvm-bolt-17; do
        if command -v "$cmd" &>/dev/null; then
            LLVM_BOLT="$(command -v "$cmd")"
            break
        fi
    done
    for cmd in merge-fdata merge-fdata-19 merge-fdata-18 merge-fdata-17; do
        if command -v "$cmd" &>/dev/null; then
            MERGE_FDATA="$(command -v "$cmd")"
            break
        fi
    done
fi

if [[ -n "$LLVM_BOLT" && -n "$MERGE_FDATA" ]]; then
    ok "llvm-bolt: $LLVM_BOLT"
    ok "merge-fdata: $MERGE_FDATA"
else
    warn "BOLT not available (install llvm-19 for BOLT support). PGO-only build."
    LLVM_BOLT=""
    MERGE_FDATA=""
fi

# --- Step 1: Build instrumented binary ---
# IMPORTANT: Must use the same profile (release) for both instrumented and final
# builds. Using a different profile (e.g. thin LTO vs full LTO) causes function
# layout mismatches that degrade PGO effectiveness.
info "Step 1/5: Building instrumented binary (full release profile)..."
rm -rf "$PROFILE_DIR"
mkdir -p "$PROFILE_DIR"

RUSTFLAGS="-Cprofile-generate=$(pwd)/$PROFILE_DIR" \
    cargo build --release -p "$PACKAGE" 2>&1

INSTRUMENTED_BIN="target/release/$BINARY_NAME"
if [[ ! -x "$INSTRUMENTED_BIN" ]]; then
    err "Instrumented binary not found at $INSTRUMENTED_BIN"
    exit 1
fi
ok "Instrumented binary: $INSTRUMENTED_BIN"

# --- Step 2: Run training workloads ---
info "Step 2/5: Running training workloads..."

# 2a: Criterion benchmarks (library-level hot paths)
BENCHMARKS=(
    pattern_benchmark
    kleene_benchmark
    hamlet_zdd_benchmark
    pst_forecast_benchmark
    context_benchmark
)

for bench in "${BENCHMARKS[@]}"; do
    info "  Criterion: $bench (sample-size=$CRITERION_SAMPLE_SIZE)"
    LLVM_PROFILE_FILE="$(pwd)/$PROFILE_DIR/bench_%p_%m.profraw" \
    RUSTFLAGS="-Cprofile-generate=$(pwd)/$PROFILE_DIR" \
        cargo bench -p "$BENCH_PACKAGE" --bench "$bench" \
            -- --sample-size "$CRITERION_SAMPLE_SIZE" --warm-up-time 1 \
            2>&1 | tail -1 || warn "  Benchmark $bench had warnings (profiles still collected)"
done

# 2b: CLI simulation scenarios (end-to-end hot paths)
for scenario_dir in "$SCENARIOS_DIR"/0*; do
    scenario="$(basename "$scenario_dir")"
    vpl_file="$scenario_dir/varpulis.vpl"
    evt_file="$DATA_DIR/${scenario}_bench.evt"

    if [[ -f "$vpl_file" && -f "$evt_file" ]]; then
        info "  Simulate: $scenario"
        LLVM_PROFILE_FILE="$(pwd)/$PROFILE_DIR/sim_%p_%m.profraw" \
            "$INSTRUMENTED_BIN" simulate \
                -p "$vpl_file" -e "$evt_file" -q -w 1 \
            2>&1 | tail -1 || warn "  Scenario $scenario had warnings"
    else
        warn "  Skipping $scenario (missing vpl or evt file)"
    fi
done

# 2c: Large volume test (sustained throughput coverage)
BULK_EVT="$DATA_DIR/events_100k.evt"
FILTER_VPL="$SCENARIOS_DIR/01_filter/varpulis.vpl"
if [[ -f "$BULK_EVT" && -f "$FILTER_VPL" ]]; then
    info "  Simulate: bulk 100K events (filter)"
    LLVM_PROFILE_FILE="$(pwd)/$PROFILE_DIR/bulk_%p_%m.profraw" \
        "$INSTRUMENTED_BIN" simulate \
            -p "$FILTER_VPL" -e "$BULK_EVT" -q -w 1 \
        2>&1 | tail -1 || true
fi

# Count collected profiles
PROFILE_COUNT=$(find "$PROFILE_DIR" -name '*.profraw' | wc -l)
if [[ "$PROFILE_COUNT" -eq 0 ]]; then
    err "No profile data collected! Check that workloads ran correctly."
    exit 1
fi
ok "Collected $PROFILE_COUNT profile files"

# --- Step 3: Merge profiles ---
info "Step 3/5: Merging profiles..."
"$LLVM_PROFDATA" merge -o "$MERGED_PROFILE" "$PROFILE_DIR"/*.profraw
PROFILE_SIZE=$(du -h "$MERGED_PROFILE" | cut -f1)
ok "Merged profile: $MERGED_PROFILE ($PROFILE_SIZE)"

# --- Step 4: Rebuild with PGO ---
# If BOLT is available, add --emit-relocs so BOLT can instrument the binary.
info "Step 4/5: Building PGO-optimized binary (this takes a while with full LTO)..."
PGO_RUSTFLAGS="-Cprofile-use=$(pwd)/$MERGED_PROFILE -Cllvm-args=-pgo-warn-missing-function"
if [[ -n "$LLVM_BOLT" ]]; then
    PGO_RUSTFLAGS="$PGO_RUSTFLAGS -Clink-arg=-Wl,--emit-relocs"
fi
RUSTFLAGS="$PGO_RUSTFLAGS" \
    cargo build --release -p "$PACKAGE" 2>&1

PGO_BIN="target/release/$BINARY_NAME"
if [[ ! -x "$PGO_BIN" ]]; then
    err "PGO binary not found at $PGO_BIN"
    exit 1
fi

PGO_SIZE=$(du -h "$PGO_BIN" | cut -f1)
ok "PGO binary: $PGO_BIN ($PGO_SIZE)"

# --- Step 5: BOLT (optional) ---
if [[ -n "$LLVM_BOLT" && -n "$MERGE_FDATA" ]]; then
    info "Step 5/5: Applying BOLT optimization..."

    BOLT_PROFILE_DIR="target/bolt-profiles"
    BOLT_MERGED="target/bolt-merged.fdata"
    BOLT_INST="${PGO_BIN}.bolt-inst"
    BOLT_OUTPUT="target/release/${BINARY_NAME}-pgo-bolt"

    rm -rf "$BOLT_PROFILE_DIR"
    mkdir -p "$BOLT_PROFILE_DIR"

    # 5a: Instrument with BOLT
    info "  BOLT: instrumenting binary..."
    "$LLVM_BOLT" "$PGO_BIN" \
        -instrument \
        -instrumentation-file="$(pwd)/$BOLT_PROFILE_DIR/bolt.fdata" \
        -o "$BOLT_INST" 2>&1 | tail -3

    if [[ ! -x "$BOLT_INST" ]]; then
        warn "  BOLT instrumentation failed. Skipping BOLT."
    else
        # 5b: Run BOLT training workloads (subset)
        for scenario in $BOLT_SCENARIOS; do
            vpl_file="$SCENARIOS_DIR/$scenario/varpulis.vpl"
            evt_file="$DATA_DIR/${scenario}_bench.evt"
            if [[ -f "$vpl_file" && -f "$evt_file" ]]; then
                info "  BOLT train: $scenario"
                "$BOLT_INST" simulate \
                    -p "$vpl_file" -e "$evt_file" -q -w 1 \
                    2>&1 | tail -1 || true
            fi
        done

        # 5c: Merge BOLT profiles
        BOLT_FILES=$(find "$BOLT_PROFILE_DIR" -name '*.fdata' -size +0c 2>/dev/null)
        if [[ -z "$BOLT_FILES" ]]; then
            warn "  No BOLT profile data collected. Skipping BOLT."
        else
            info "  BOLT: merging profiles..."
            "$MERGE_FDATA" $BOLT_FILES > "$BOLT_MERGED" 2>/dev/null

            # 5d: Optimize with BOLT
            info "  BOLT: optimizing binary layout..."
            "$LLVM_BOLT" "$PGO_BIN" \
                -o "$BOLT_OUTPUT" \
                -data="$BOLT_MERGED" \
                -reorder-blocks=ext-tsp \
                -reorder-functions=cdsort \
                -split-functions \
                -split-all-cold \
                -icf=1 \
                -use-gnu-stack \
                -dyno-stats 2>&1

            if [[ -x "$BOLT_OUTPUT" ]]; then
                BOLT_SIZE=$(du -h "$BOLT_OUTPUT" | cut -f1)
                ok "PGO+BOLT binary: $BOLT_OUTPUT ($BOLT_SIZE)"
            else
                warn "  BOLT optimization failed. PGO-only binary available."
            fi
        fi
    fi

    # Clean up BOLT instrumented binary
    rm -f "$BOLT_INST"
else
    info "Step 5/5: BOLT skipped (not available)"
fi

# --- Summary ---
echo ""
echo "========================================"
echo "  PGO Build Complete"
echo "========================================"
echo ""
echo "  PGO binary:      $PGO_BIN ($PGO_SIZE)"
if [[ -x "target/release/${BINARY_NAME}-pgo-bolt" ]]; then
    BOLT_SIZE=$(du -h "target/release/${BINARY_NAME}-pgo-bolt" | cut -f1)
    echo "  PGO+BOLT binary:  target/release/${BINARY_NAME}-pgo-bolt ($BOLT_SIZE)"
fi
echo "  Merged profile:   $MERGED_PROFILE ($PROFILE_SIZE)"
echo "  Profile files:    $PROFILE_COUNT"
echo ""
echo "  To evaluate: ./scripts/pgo-evaluate.sh"
echo ""
