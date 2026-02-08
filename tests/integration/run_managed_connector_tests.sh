#!/bin/bash
# Managed Connector Integration Tests for Varpulis
#
# Usage:
#   ./run_managed_connector_tests.sh              # Start Mosquitto in Docker + run tests
#   ./run_managed_connector_tests.sh --local      # Skip Docker, assume broker already running
#   ./run_managed_connector_tests.sh --keep       # Keep containers running after tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.managed-connectors.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

LOCAL_MODE=false
KEEP_RUNNING=false

for arg in "$@"; do
    case $arg in
        --local)
            LOCAL_MODE=true
            ;;
        --keep|--keep-running)
            KEEP_RUNNING=true
            ;;
    esac
done

echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Varpulis Managed Connector Integration Tests          ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Project root: $PROJECT_ROOT"
echo "Mode: $([ "$LOCAL_MODE" = true ] && echo "Local" || echo "Docker")"
echo ""

# Cleanup function
cleanup() {
    local exit_code=$?
    if [[ "$LOCAL_MODE" == false && "$KEEP_RUNNING" == false ]]; then
        echo -e "\n${YELLOW}Stopping containers...${NC}"
        docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    elif [[ "$KEEP_RUNNING" == true ]]; then
        echo -e "\n${YELLOW}Keeping containers running.${NC}"
        echo "  Stop with: docker compose -f $COMPOSE_FILE down -v"
    fi
    exit $exit_code
}

trap cleanup EXIT

cd "$PROJECT_ROOT"

if [[ "$LOCAL_MODE" == false ]]; then
    echo -e "${YELLOW}[1/4] Starting Mosquitto...${NC}"
    docker compose -f "$COMPOSE_FILE" up -d mosquitto
fi

echo -e "${YELLOW}[2/4] Waiting for Mosquitto to be ready...${NC}"
max_attempts=20
attempt=0
while ! docker exec varpulis-managed-mqtt mosquitto_sub -h localhost -p 1883 -t '$SYS/#' -C 1 -W 1 &>/dev/null; do
    attempt=$((attempt + 1))
    if [[ $attempt -ge $max_attempts ]]; then
        echo -e "${RED}Mosquitto failed to start after $max_attempts attempts${NC}"
        exit 1
    fi
    echo "  Waiting... ($attempt/$max_attempts)"
    sleep 1
done
echo -e "${GREEN}Mosquitto is ready on port 11883!${NC}"

echo -e "\n${YELLOW}[3/4] Building with mqtt feature...${NC}"
cargo build -p varpulis-runtime --features mqtt

echo -e "\n${YELLOW}[4/4] Running managed connector integration tests...${NC}"
cargo test -p varpulis-runtime --features mqtt managed_connector -- --ignored --nocapture

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   All managed connector tests passed!                   ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"

if [[ "$KEEP_RUNNING" == true ]]; then
    echo ""
    echo -e "${YELLOW}Mosquitto is still running. Connection details:${NC}"
    echo "  Host: localhost:11883"
    echo ""
    echo "Publish a test message:"
    echo "  docker exec varpulis-managed-mqtt mosquitto_pub -h localhost -p 1883 -t test -m '{\"event_type\":\"Test\",\"value\":1}'"
    echo ""
    echo "Subscribe to messages:"
    echo "  docker exec varpulis-managed-mqtt mosquitto_sub -h localhost -p 1883 -t '#' -v"
fi
