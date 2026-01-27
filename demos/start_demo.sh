#!/bin/bash
# Varpulis Demo Launcher
# This script starts all components needed for the interactive demo

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}\n"
}

print_step() {
    echo -e "${GREEN}▶${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Default values
DEMO="hvac"
DURATION=0  # 0 = infinite
RATE=2      # events per second

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--demo)
            DEMO="$2"
            shift 2
            ;;
        -t|--duration)
            DURATION="$2"
            shift 2
            ;;
        -r|--rate)
            RATE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Varpulis Demo Launcher"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -d, --demo DEMO       Demo to run: hvac, financial, sase (default: hvac)"
            echo "  -t, --duration SECS   Duration in seconds (0 = infinite, default: 0)"
            echo "  -r, --rate RATE       Events per second (default: 2)"
            echo "  -h, --help            Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                    # Run HVAC demo infinitely"
            echo "  $0 -d sase -t 60      # Run SASE demo for 60 seconds"
            echo "  $0 -d financial -r 5  # Run Financial demo at 5 events/sec"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

print_header "🚀 Varpulis Demo: ${DEMO^^}"

# Check Docker is running
if ! docker info &>/dev/null; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Start infrastructure
print_step "Starting infrastructure (Mosquitto, Dashboard Server, UI)..."
docker compose up -d --build 2>&1 | grep -E "(Created|Started|Running)" || true

# Wait for services
print_step "Waiting for services to be ready..."
sleep 3

# Check services
MOSQUITTO_OK=$(docker exec varpulis-mosquitto mosquitto_pub -t "test" -m "ping" 2>&1 && echo "OK" || echo "FAIL")
if [[ "$MOSQUITTO_OK" != *"OK"* ]]; then
    print_error "Mosquitto is not responding"
    exit 1
fi
print_step "✓ Mosquitto ready on port 1883"

SERVER_OK=$(curl -s http://localhost:3002/api/health | grep -q "ok" && echo "OK" || echo "FAIL")
if [[ "$SERVER_OK" != "OK" ]]; then
    print_error "Dashboard server is not responding"
    exit 1
fi
print_step "✓ Dashboard server ready on port 3002"

UI_OK=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5173)
if [[ "$UI_OK" != "200" ]]; then
    print_error "Dashboard UI is not responding"
    exit 1
fi
print_step "✓ Dashboard UI ready on port 5173"

echo ""
print_header "📊 Dashboard: http://localhost:5173"

# Start event generator
print_step "Starting event generator for ${DEMO} demo..."
echo ""

# Build duration argument
DURATION_ARG=""
if [[ "$DURATION" -gt 0 ]]; then
    DURATION_ARG="--duration $DURATION"
fi

# Build the generator image if needed
print_step "Building generator image..."
docker compose build generator 2>&1 | grep -E "(Building|Built)" || true

# Run the appropriate generator via Docker
print_step "Starting ${DEMO} event generator via Docker..."
docker compose run --rm generator /generators/${DEMO}/generator.py --broker mosquitto --rate "$RATE" $DURATION_ARG
