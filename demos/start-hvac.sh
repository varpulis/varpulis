#!/bin/bash
# =============================================================================
# HVAC Building Demo Startup Script
# =============================================================================
#
# Architecture:
#   Generator (raw events) → MQTT → Varpulis CEP → MQTT → Dashboard
#
# Components:
#   1. Mosquitto MQTT broker
#   2. HVAC generator (TemperatureReading, HumidityReading, HVACStatus events)
#   3. Varpulis CEP engine (processes hvac/main.vpl)
#   4. Dashboard UI (optional)
#
# Usage:
#   ./start-hvac.sh           # Start all components
#   ./start-hvac.sh --no-ui   # Start without dashboard
#   ./start-hvac.sh --stop    # Stop all components
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VARPULIS_BIN="$PROJECT_ROOT/target/release/varpulis"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Parse arguments
NO_UI=false
STOP=false
for arg in "$@"; do
    case $arg in
        --no-ui) NO_UI=true ;;
        --stop) STOP=true ;;
    esac
done

stop_demo() {
    log_info "Stopping HVAC demo..."
    pkill -f "varpulis run.*hvac" 2>/dev/null || true
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" stop generator 2>/dev/null || true
    log_success "Demo stopped"
}

if [ "$STOP" = true ]; then
    stop_demo
    exit 0
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           🏢 VARPULIS HVAC BUILDING DEMO                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check if varpulis binary exists
if [ ! -f "$VARPULIS_BIN" ]; then
    log_warn "Varpulis binary not found. Building in release mode..."
    (cd "$PROJECT_ROOT" && cargo build --release -p varpulis-cli)
fi

# Step 1: Start MQTT broker
log_info "Starting MQTT broker..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d mosquitto
sleep 1
log_success "MQTT broker running on localhost:1883"

# Step 2: Start HVAC generator
log_info "Starting HVAC event generator..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" run -d --rm \
    generator /generators/hvac/generator.py --broker mosquitto --rate 2

sleep 1
log_success "Generator producing TemperatureReading, HumidityReading events"

# Step 3: Start Dashboard (optional)
if [ "$NO_UI" = false ]; then
    log_info "Starting Dashboard..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d dashboard-server dashboard-ui
    log_success "Dashboard available at http://localhost:5173"
fi

# Step 4: Start Varpulis CEP engine
echo ""
log_info "Starting Varpulis CEP engine..."
echo ""
echo "  ┌─────────────────────────────────────────────────────────┐"
echo "  │  Varpulis will process events using:                    │"
echo "  │    demos/hvac/main.vpl                                  │"
echo "  │                                                         │"
echo "  │  Monitoring:                                            │"
echo "  │    - Zone temperatures (office, lobby, server_room)     │"
echo "  │    - Humidity levels                                    │"
echo "  │    - HVAC equipment status                              │"
echo "  │    - Energy consumption                                 │"
echo "  │                                                         │"
echo "  │  Detecting:                                             │"
echo "  │    - Temperature anomalies (> 28°C or < 15°C)           │"
echo "  │    - Server room alerts (> 25°C)                        │"
echo "  │    - Power spikes (> 5kW)                               │"
echo "  │                                                         │"
echo "  │  Press Ctrl+C to stop                                   │"
echo "  └─────────────────────────────────────────────────────────┘"
echo ""

# Run Varpulis (this blocks until Ctrl+C)
"$VARPULIS_BIN" run --file "$SCRIPT_DIR/hvac/main.vpl"

# Cleanup on exit
stop_demo
