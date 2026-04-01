#!/bin/bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
VARPULIS="${VARPULIS:-cargo run --bin varpulis --}"
PASS=0
FAIL=0

run_test() {
    local name="$1"
    local vpl="$2"
    local data="$3"
    local expected_min="$4"
    local expected_max="${5:-999}"

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    output=$($VARPULIS simulate -p "$vpl" -e "$data" -v -w 1 2>&1)
    count=$(echo "$output" | grep -c "OUTPUT EVENT" || true)

    if [ "$count" -ge "$expected_min" ] && [ "$count" -le "$expected_max" ]; then
        echo "  PASS ($count alerts, expected $expected_min+)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL ($count alerts, expected $expected_min-$expected_max)"
        echo "$output" | grep -E "OUTPUT EVENT|Error" || true
        FAIL=$((FAIL + 1))
    fi
}

echo "========================================"
echo "  Varpulis Security Demo"
echo "  APT Kill Chain Detection"
echo "========================================"

# Phase 1: Sysmon ingestion smoke test
run_test "Sysmon Ingestion Smoke Test" \
    "$DEMO_DIR/sysmon_smoke.vpl" \
    "$DEMO_DIR/data/sysmon_sample.jsonl" 5

# Phase 2: Scripting -> Discovery
run_test "Scripting -> Discovery (T1059.001, T1057)" \
    "$DEMO_DIR/detect_scripting_to_discovery.vpl" \
    "$DEMO_DIR/data/apt29_scripting_chain.jsonl" 2

# Phase 3: Kill chain suite
run_test "Credential Dumping - LSASS Access (T1003.001)" \
    "$DEMO_DIR/detect_credential_dumping.vpl" \
    "$DEMO_DIR/data/apt29_credential_dump.jsonl" 3

run_test "Lateral Movement - SMB (T1021.002)" \
    "$DEMO_DIR/detect_lateral_movement.vpl" \
    "$DEMO_DIR/data/apt29_lateral_movement.jsonl" 1

run_test "Persistence - Registry Run Key (T1547.001)" \
    "$DEMO_DIR/detect_persistence_registry.vpl" \
    "$DEMO_DIR/data/apt29_persistence.jsonl" 2

run_test "Data Staging -> Exfiltration (T1560, T1041)" \
    "$DEMO_DIR/detect_data_staging_exfil.vpl" \
    "$DEMO_DIR/data/apt29_full_chain.jsonl" 1

run_test "Full Kill Chain: 4-step APT (T1059, T1003, T1021, T1041)" \
    "$DEMO_DIR/detect_full_killchain.vpl" \
    "$DEMO_DIR/data/apt29_full_chain.jsonl" 1

# Phase 4: Sigma comparison — the money shot
run_test "Sigma: Renamed PsExec (should MISS)" \
    "$DEMO_DIR/sigma_comparison/sigma_only.vpl" \
    "$DEMO_DIR/sigma_comparison/evasion_dataset.jsonl" 0 0

run_test "VPL Behavioral: Renamed PsExec (should CATCH)" \
    "$DEMO_DIR/sigma_comparison/vpl_behavioral.vpl" \
    "$DEMO_DIR/sigma_comparison/evasion_dataset.jsonl" 1

run_test "Sigma: Normal PsExec (sanity check)" \
    "$DEMO_DIR/sigma_comparison/sigma_only.vpl" \
    "$DEMO_DIR/sigma_comparison/normal_dataset.jsonl" 1

run_test "VPL Behavioral: Normal PsExec (sanity check)" \
    "$DEMO_DIR/sigma_comparison/vpl_behavioral.vpl" \
    "$DEMO_DIR/sigma_comparison/normal_dataset.jsonl" 1

echo ""
echo "========================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
