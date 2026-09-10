#!/usr/bin/env bash
# CDC TLS gate tests — run the connector's tls_integration tests against a
# PostgreSQL that REQUIRES SSL.
#
# These are the fail-before/pass-after gates for the rustls TLS work in
# varpulis-connector-cdc. They must never pass by abstaining, so this script
# exports VARPULIS_REQUIRE_BROKERS=1: if the server is not reachable the tests
# fail instead of printing a skip.
#
# Usage:
#   ./run_pg_tls_tests.sh            # bring the server up, run, tear down
#   ./run_pg_tls_tests.sh --keep     # leave the server running afterwards
#
# Port: 5433 by default; override with VARPULIS_TLS_PG_PORT (the compose file
# and the tests both read it).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.pg-tls.yml"

export VARPULIS_TLS_PG_PORT="${VARPULIS_TLS_PG_PORT:-5433}"
export VARPULIS_TLS_PG_HOST="${VARPULIS_TLS_PG_HOST:-localhost}"
export VARPULIS_TLS_PG_CA="$SCRIPT_DIR/pg-tls/ca.crt"
export VARPULIS_REQUIRE_BROKERS=1

KEEP_RUNNING=false
for arg in "$@"; do
    [[ "$arg" == "--keep" || "$arg" == "--keep-running" ]] && KEEP_RUNNING=true
done

cleanup() {
    local exit_code=$?
    if [[ "$KEEP_RUNNING" == false ]]; then
        docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
    else
        echo "Keeping varpulis-pg-tls running on port $VARPULIS_TLS_PG_PORT"
    fi
    exit $exit_code
}
trap cleanup EXIT

echo "[1/3] Generating the self-signed server certificate..."
bash "$SCRIPT_DIR/pg-tls/generate-certs.sh"

echo "[2/3] Starting TLS-only PostgreSQL on port $VARPULIS_TLS_PG_PORT..."
docker compose -f "$COMPOSE_FILE" up -d
for _ in $(seq 1 40); do
    if docker exec varpulis-pg-tls pg_isready -h /var/run/postgresql -U postgres >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
docker exec varpulis-pg-tls pg_isready -h /var/run/postgresql -U postgres

echo "[3/3] Running the CDC TLS gates..."
cd "$PROJECT_ROOT"
cargo test -p varpulis-connector-cdc --lib tls_integration -- --nocapture --test-threads=1

echo "All CDC TLS gates passed."
