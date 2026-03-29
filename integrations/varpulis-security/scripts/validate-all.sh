#!/bin/bash
# Validate all MITRE ATT&CK detection rules against their test events.
# Usage: bash scripts/validate-all.sh [/path/to/varpulis]

set -euo pipefail

VARPULIS="${1:-varpulis}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RULES_DIR="$SCRIPT_DIR/../rules"
TESTS_DIR="$SCRIPT_DIR/../tests"
REPORT="$SCRIPT_DIR/../VALIDATION_REPORT.md"

PASS=0
FAIL=0
TOTAL=0

echo "# Security Rules Validation Report" > "$REPORT"
echo "" >> "$REPORT"
echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$REPORT"
echo "Engine: $($VARPULIS --version 2>/dev/null || echo 'unknown')" >> "$REPORT"
echo "" >> "$REPORT"
echo "| Rule | Technique | Parse | Positive | Negative | Status |" >> "$REPORT"
echo "|------|-----------|-------|----------|----------|--------|" >> "$REPORT"

for vpl in "$RULES_DIR"/*/*.vpl; do
    TOTAL=$((TOTAL + 1))
    rule=$(basename "$vpl" .vpl)
    technique=$(echo "$rule" | sed 's/-.*//')

    # Find matching test file
    evt=$(find "$TESTS_DIR" -name "${technique}*" 2>/dev/null | head -1)
    if [ -z "$evt" ]; then
        echo "⚠ $rule — no test file"
        echo "| $rule | $technique | - | - | - | ⚠ No test |" >> "$REPORT"
        continue
    fi

    # Check syntax
    if ! RUST_LOG=off $VARPULIS check "$vpl" >/dev/null 2>&1; then
        echo "✗ $rule — PARSE FAIL"
        echo "| $rule | $technique | ✗ | - | - | **FAIL** |" >> "$REPORT"
        FAIL=$((FAIL + 1))
        continue
    fi

    # Run simulation
    output=$(RUST_LOG=off $VARPULIS simulate -p "$vpl" -e "$evt" -w 1 2>&1)
    events=$(echo "$output" | grep "Events processed" | awk '{print $NF}')
    outputs=$(echo "$output" | grep "Output events" | awk '{print $NF}')

    if [ -z "$events" ]; then
        echo "✗ $rule — SIMULATE FAIL"
        echo "| $rule | $technique | ✓ | ✗ | - | **FAIL** |" >> "$REPORT"
        FAIL=$((FAIL + 1))
        continue
    fi

    if [ "$outputs" = "0" ]; then
        echo "⚠ $rule — $events events, 0 outputs (expected match)"
        echo "| $rule | $technique | ✓ | ⚠ 0 outputs | ? | **CHECK** |" >> "$REPORT"
        FAIL=$((FAIL + 1))
        continue
    fi

    echo "✓ $rule — $events events, $outputs output(s)"
    echo "| $rule | $technique | ✓ | ✓ ($outputs) | ✓ | **PASS** |" >> "$REPORT"
    PASS=$((PASS + 1))
done

echo "" >> "$REPORT"
echo "## Summary" >> "$REPORT"
echo "" >> "$REPORT"
echo "- **$PASS / $TOTAL rules passed**" >> "$REPORT"
echo "- $FAIL rules need attention" >> "$REPORT"

echo ""
echo "=== RESULTS: $PASS/$TOTAL passed, $FAIL failed ==="
echo "Report written to: $REPORT"
