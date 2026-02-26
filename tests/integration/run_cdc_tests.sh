#!/bin/bash
# CDC (PostgreSQL Change Data Capture) Integration Tests for Varpulis
#
# Usage:
#   ./run_cdc_tests.sh              # Run tests in Docker (recommended)
#   ./run_cdc_tests.sh --local      # Run tests locally (requires PostgreSQL with wal_level=logical)
#   ./run_cdc_tests.sh --keep       # Keep containers running after tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.cdc.yml"

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
echo -e "${CYAN}║       Varpulis CDC Integration Tests                     ║${NC}"
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
    echo -e "${YELLOW}[1/4] Starting PostgreSQL with logical replication...${NC}"
    docker compose -f "$COMPOSE_FILE" up -d postgres

    echo -e "${YELLOW}[2/4] Waiting for PostgreSQL to be ready...${NC}"
    max_attempts=30
    attempt=0
    while ! docker exec varpulis-cdc-postgres pg_isready -U varpulis -d cdc_test &>/dev/null; do
        attempt=$((attempt + 1))
        if [[ $attempt -ge $max_attempts ]]; then
            echo -e "${RED}PostgreSQL failed to start after $max_attempts attempts${NC}"
            exit 1
        fi
        echo "  Waiting... ($attempt/$max_attempts)"
        sleep 2
    done
    echo -e "${GREEN}PostgreSQL is ready!${NC}"

    echo -e "\n${YELLOW}[3/4] Setting up tables and publication...${NC}"
    docker compose -f "$COMPOSE_FILE" up postgres-setup

    echo -e "\n${YELLOW}[4/4] Running CDC integration tests...${NC}"
    cargo test -p varpulis-runtime --features cdc --test cdc_e2e -- --nocapture

else
    # ========================
    # DOCKER MODE (recommended)
    # ========================
    echo -e "${YELLOW}[1/2] Starting PostgreSQL and running setup...${NC}"
    docker compose -f "$COMPOSE_FILE" up -d postgres
    docker compose -f "$COMPOSE_FILE" up postgres-setup

    echo -e "\n${YELLOW}[2/2] Running CDC tests locally against Docker PostgreSQL...${NC}"
    CDC_TEST_HOST=localhost CDC_TEST_PORT=5432 \
        cargo test -p varpulis-runtime --features cdc --test cdc_e2e -- --nocapture
fi

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║       All CDC integration tests passed!                  ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"

if [[ "$KEEP_RUNNING" == true ]]; then
    echo ""
    echo -e "${YELLOW}PostgreSQL is still running. Connection details:${NC}"
    echo "  Host: localhost:5432"
    echo "  Database: cdc_test"
    echo "  User: varpulis / Password: testpass"
    echo ""
    echo "Connect:"
    echo "  docker exec -it varpulis-cdc-postgres psql -U varpulis -d cdc_test"
fi
