#!/bin/bash
# Kafka Integration Tests for Varpulis
#
# Usage:
#   ./run_kafka_tests.sh              # Run tests in Docker (recommended)
#   ./run_kafka_tests.sh --local      # Run tests locally (requires deps)
#   ./run_kafka_tests.sh --keep       # Keep containers running after tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.kafka.yml"

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
echo -e "${CYAN}║       Varpulis Kafka Integration Tests                   ║${NC}"
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
    echo -e "${YELLOW}[1/4] Starting Kafka infrastructure...${NC}"
    docker compose -f "$COMPOSE_FILE" up -d zookeeper kafka kafka-setup

    echo -e "${YELLOW}[2/4] Waiting for Kafka to be ready...${NC}"
    max_attempts=30
    attempt=0
    while ! docker exec varpulis-kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do
        attempt=$((attempt + 1))
        if [[ $attempt -ge $max_attempts ]]; then
            echo -e "${RED}Kafka failed to start after $max_attempts attempts${NC}"
            exit 1
        fi
        echo "  Waiting... ($attempt/$max_attempts)"
        sleep 2
    done
    echo -e "${GREEN}Kafka is ready!${NC}"

    echo -e "\n${YELLOW}[3/4] Building with kafka feature...${NC}"
    cargo build -p varpulis-runtime --features kafka

    echo -e "\n${YELLOW}[4/4] Running integration tests...${NC}"
    cargo test -p varpulis-runtime --features kafka kafka -- --ignored --nocapture

else
    # ========================
    # DOCKER MODE (recommended)
    # ========================
    echo -e "${YELLOW}[1/2] Building test container and starting infrastructure...${NC}"
    echo "  This may take a while on first run (building rdkafka)..."
    echo ""

    # Build the test runner image first
    docker compose -f "$COMPOSE_FILE" build test-runner

    echo -e "\n${YELLOW}[2/2] Running tests in container...${NC}"

    # Run all services, test-runner will exit when done
    docker compose -f "$COMPOSE_FILE" up --abort-on-container-exit --exit-code-from test-runner
fi

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║       All Kafka integration tests passed!                ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"

if [[ "$KEEP_RUNNING" == true ]]; then
    echo ""
    echo -e "${YELLOW}Kafka is still running. Connection details:${NC}"
    echo "  Bootstrap servers: localhost:9092"
    echo "  Topics: varpulis-test-input, varpulis-test-output"
    echo ""
    echo "Produce messages:"
    echo "  docker exec -it varpulis-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic varpulis-test-input"
    echo ""
    echo "Consume messages:"
    echo "  docker exec -it varpulis-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic varpulis-test-output --from-beginning"
fi
