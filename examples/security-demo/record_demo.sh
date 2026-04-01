#!/bin/bash
# Scripted demo for asciinema recording
# Usage: asciinema rec -c "bash examples/security-demo/record_demo.sh" demo.cast
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
VARPULIS="${VARPULIS:-target/release/varpulis}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

type_cmd() {
    local cmd="$1"
    echo -ne "${GREEN}\$ ${NC}"
    for ((i=0; i<${#cmd}; i++)); do
        echo -n "${cmd:$i:1}"
        sleep 0.03
    done
    echo ""
    sleep 0.3
}

section() {
    echo ""
    echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}${CYAN}  $1${NC}"
    echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    sleep 1
}

pause() {
    sleep "${1:-2}"
}

# =========================================================================
# INTRO
# =========================================================================
clear
echo ""
echo -e "${BOLD}${CYAN}"
echo "  ╦  ╦╔═╗╦═╗╔═╗╦ ╦╦  ╦╔═╗"
echo "  ╚╗╔╝╠═╣╠╦╝╠═╝║ ║║  ║╚═╗"
echo "   ╚╝ ╩ ╩╩╚═╩  ╚═╝╩═╝╩╚═╝"
echo -e "${NC}"
echo -e "  ${BOLD}APT Kill Chain Detection Demo${NC}"
echo -e "  ${DIM}Detecting what your SIEM misses${NC}"
echo ""
pause 3

# =========================================================================
# PART 1: Show the VPL rule
# =========================================================================
section "1. The Detection Rule — Full APT Kill Chain"

echo -e "${DIM}# A VPL rule that detects the complete attack lifecycle:${NC}"
echo -e "${DIM}# Script execution → Credential theft → Lateral movement → Exfiltration${NC}"
echo ""
pause 1

type_cmd "cat examples/security-demo/detect_full_killchain.vpl | head -50"
head -50 "$DEMO_DIR/detect_full_killchain.vpl" | while IFS= read -r line; do
    echo "$line"
    sleep 0.02
done
pause 3

# =========================================================================
# PART 2: Run against real MORDOR APT29 data
# =========================================================================
section "2. Running Against Real APT29 Attack Data (MORDOR Dataset)"

echo -e "${DIM}# 50,000 Sysmon events from a real APT29 (Cozy Bear) emulation${NC}"
echo ""
pause 1

type_cmd "$VARPULIS simulate -p examples/security-demo/detect_full_killchain.vpl -e mordor/apt29_day1_50k.jsonl -w 1 -v"

$VARPULIS simulate \
    -p "$DEMO_DIR/detect_full_killchain.vpl" \
    -e "$DEMO_DIR/data/mordor/apt29_day1_50k.jsonl" \
    -w 1 -v 2>&1 | tail -20

pause 4

# =========================================================================
# PART 3: Credential dumping detection
# =========================================================================
section "3. Credential Dumping — LSASS Memory Access (T1003.001)"

type_cmd "$VARPULIS simulate -p examples/security-demo/detect_credential_dumping.vpl -e mordor/apt29_day1_50k.jsonl -w 1 -v"

$VARPULIS simulate \
    -p "$DEMO_DIR/detect_credential_dumping.vpl" \
    -e "$DEMO_DIR/data/mordor/apt29_day1_50k.jsonl" \
    -w 1 -v 2>&1 | tail -15

pause 3

# =========================================================================
# PART 4: THE MONEY SHOT — Sigma vs VPL
# =========================================================================
section "4. The Money Shot — Sigma vs VPL Behavioral Detection"

echo -e "${DIM}# Scenario: Attacker renames PsExec.exe → svcupdate.exe${NC}"
echo -e "${DIM}# Sigma rule matches filename → misses the renamed binary${NC}"
echo -e "${DIM}# VPL rule matches behavior → catches the attack regardless${NC}"
echo ""
pause 2

echo -e "${YELLOW}Test 1: Sigma-style rule (filename matching)${NC}"
type_cmd "$VARPULIS simulate -p examples/security-demo/sigma_comparison/sigma_only.vpl -e examples/security-demo/sigma_comparison/evasion_dataset.jsonl -v -w 1"

result=$($VARPULIS simulate \
    -p "$DEMO_DIR/sigma_comparison/sigma_only.vpl" \
    -e "$DEMO_DIR/sigma_comparison/evasion_dataset.jsonl" \
    -v -w 1 2>&1)
count=$(echo "$result" | grep -c "OUTPUT EVENT" || true)
echo "$result" | tail -5
echo ""
if [ "$count" -eq 0 ]; then
    echo -e "  ${RED}${BOLD}→ SILENCE. Sigma sees nothing. The attack is invisible.${NC}"
else
    echo -e "  ${GREEN}→ $count alerts${NC}"
fi
pause 4

echo ""
echo -e "${YELLOW}Test 2: VPL behavioral rule (sequence pattern matching)${NC}"
type_cmd "$VARPULIS simulate -p examples/security-demo/sigma_comparison/vpl_behavioral.vpl -e examples/security-demo/sigma_comparison/evasion_dataset.jsonl -v -w 1"

result=$($VARPULIS simulate \
    -p "$DEMO_DIR/sigma_comparison/vpl_behavioral.vpl" \
    -e "$DEMO_DIR/sigma_comparison/evasion_dataset.jsonl" \
    -v -w 1 2>&1)
count=$(echo "$result" | grep -c "OUTPUT EVENT" || true)
echo "$result" | tail -10
echo ""
if [ "$count" -gt 0 ]; then
    echo -e "  ${GREEN}${BOLD}→ DETECTED. VPL catches the attack via behavioral analysis.${NC}"
    echo -e "  ${GREEN}${BOLD}  Same data. Different approach. The binary name doesn't matter.${NC}"
fi
pause 4

# =========================================================================
# OUTRO
# =========================================================================
section "Summary"

echo -e "  ${BOLD}Varpulis detects attacks that SIEMs miss.${NC}"
echo ""
echo -e "  ${DIM}•${NC} Full APT kill chain detection in ${BOLD}real-time${NC}"
echo -e "  ${DIM}•${NC} ${BOLD}Temporal sequence matching${NC} — not just isolated events"
echo -e "  ${DIM}•${NC} ${BOLD}Behavioral analysis${NC} — catches renamed/unknown tools"
echo -e "  ${DIM}•${NC} Native ${BOLD}MITRE ATT&CK${NC} mapping"
echo -e "  ${DIM}•${NC} 9K+ events/sec on commodity hardware"
echo ""
echo -e "  ${CYAN}https://github.com/varpulis/varpulis${NC}"
echo ""
pause 5
