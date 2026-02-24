#!/bin/bash
# Pulsar Integration Tests for Varpulis
#
# Usage:
#   ./run_pulsar_tests.sh              # Run tests in Docker (recommended)
#   ./run_pulsar_tests.sh --local      # Run tests locally (requires Pulsar)
#   ./run_pulsar_tests.sh --keep       # Keep containers running after tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.pulsar.yml"

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
echo -e "${CYAN}║       Varpulis Pulsar Integration Tests                  ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Project root: $PROJECT_ROOT"
echo "Mode: $([ "$LOCAL_MODE" = true ] && echo "Local" || echo "Docker")"
echo ""

# Cleanup function
cleanup() {
    local exit_code=$?
    if [[ "$KEEP_RUNNING" == false ]]; then
        echo -e "\n${YELLOW}Stopping containers...${NC}"
        docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    else
        echo -e "\n${YELLOW}Keeping containers running.${NC}"
        echo "  Stop with: docker compose -f $COMPOSE_FILE down -v"
    fi
    exit $exit_code
}

trap cleanup EXIT

cd "$PROJECT_ROOT"

if [[ "$LOCAL_MODE" == true ]]; then
    # ========================
    # LOCAL MODE
    # ========================
    echo -e "${YELLOW}[1/4] Starting Pulsar infrastructure...${NC}"
    docker compose -f "$COMPOSE_FILE" up -d pulsar

    echo -e "${YELLOW}[2/4] Waiting for Pulsar to be ready...${NC}"
    max_attempts=30
    attempt=0
    while ! docker exec varpulis-pulsar bin/pulsar-admin brokers healthcheck &>/dev/null; do
        attempt=$((attempt + 1))
        if [[ $attempt -ge $max_attempts ]]; then
            echo -e "${RED}Pulsar failed to start after $max_attempts attempts${NC}"
            exit 1
        fi
        echo "  Waiting... ($attempt/$max_attempts)"
        sleep 5
    done
    echo -e "${GREEN}Pulsar is ready!${NC}"

    echo -e "\n${YELLOW}[3/4] Building with pulsar feature...${NC}"
    cargo build -p varpulis-runtime --features pulsar

    echo -e "\n${YELLOW}[4/4] Running integration tests...${NC}"
    export PULSAR_URL="pulsar://localhost:6650"
    cargo test -p varpulis-runtime --features pulsar pulsar_integration -- --ignored --nocapture

else
    # ========================
    # DOCKER MODE (recommended)
    # ========================
    echo -e "${YELLOW}[1/2] Building test container and starting infrastructure...${NC}"
    echo "  This may take a while on first run..."
    echo ""

    docker compose -f "$COMPOSE_FILE" build test-runner

    echo -e "\n${YELLOW}[2/2] Running tests in container...${NC}"
    docker compose -f "$COMPOSE_FILE" up --abort-on-container-exit --exit-code-from test-runner
fi

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║       All Pulsar integration tests passed!               ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"

if [[ "$KEEP_RUNNING" == true ]]; then
    echo ""
    echo -e "${YELLOW}Pulsar is still running. Connection details:${NC}"
    echo "  Service URL:  pulsar://localhost:6650"
    echo "  Admin URL:    http://localhost:8080"
    echo "  Topics:       persistent://public/default/varpulis-test-input"
    echo "                persistent://public/default/varpulis-test-output"
fi
