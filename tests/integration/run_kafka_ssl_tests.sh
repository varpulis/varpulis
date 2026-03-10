#!/bin/bash
# Kafka SASL_SSL (SCRAM-SHA-512) Integration Tests for Varpulis
#
# Usage:
#   ./run_kafka_ssl_tests.sh              # Run tests locally (recommended)
#   ./run_kafka_ssl_tests.sh --keep       # Keep containers running after tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.kafka-ssl.yml"
SSL_DIR="$SCRIPT_DIR/kafka-ssl"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

KEEP_RUNNING=false

for arg in "$@"; do
    case $arg in
        --keep|--keep-running)
            KEEP_RUNNING=true
            ;;
    esac
done

echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Varpulis Kafka SASL_SSL (SCRAM-SHA-512) Tests         ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
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

# Step 1: Generate certificates
echo -e "${YELLOW}[1/5] Generating SSL certificates...${NC}"
if [[ ! -f "$SSL_DIR/certs/ca-cert.pem" ]]; then
    bash "$SSL_DIR/generate-certs.sh"
else
    echo "  Certificates already exist, skipping generation"
fi

# Step 2: Start Kafka SASL_SSL infrastructure
echo -e "\n${YELLOW}[2/5] Starting Kafka SASL_SSL infrastructure...${NC}"
docker compose -f "$COMPOSE_FILE" up -d zookeeper-ssl

echo -e "\n${YELLOW}[3/5] Bootstrapping SCRAM credentials and starting broker...${NC}"
docker compose -f "$COMPOSE_FILE" up -d kafka-ssl
# Wait for kafka-ssl-setup to complete (creates varpulis user + topics)
docker compose -f "$COMPOSE_FILE" up kafka-ssl-setup

echo -e "\n${YELLOW}[4/5] Building with kafka feature...${NC}"
# Ensure openssl-sys can find OpenSSL (needed for rdkafka ssl feature)
if [[ -z "${PKG_CONFIG_PATH:-}" ]] && [[ -d /usr/lib/x86_64-linux-gnu/pkgconfig ]]; then
    export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig
fi
cargo build -p varpulis-runtime --features kafka 2>&1 | tail -3

echo -e "\n${YELLOW}[5/5] Running SASL_SSL integration tests...${NC}"
export KAFKA_SSL_BOOTSTRAP=localhost:9093
export KAFKA_SSL_CERTS_DIR="$SSL_DIR/certs"

cargo test -p varpulis-runtime --features kafka kafka_ssl -- --ignored --nocapture

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   All Kafka SASL_SSL tests passed!                       ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"

if [[ "$KEEP_RUNNING" == true ]]; then
    echo ""
    echo -e "${YELLOW}Kafka SASL_SSL is still running. Connection details:${NC}"
    echo "  Bootstrap: localhost:9093"
    echo "  Protocol:  SASL_SSL"
    echo "  SASL:      SCRAM-SHA-512"
    echo "  User:      varpulis / varpulis-secret"
    echo "  CA cert:   $SSL_DIR/certs/ca-cert.pem"
    echo "  Topics:    varpulis-ssl-input, varpulis-ssl-output"
fi
