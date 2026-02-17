#!/usr/bin/env bash
# Chaos Test Runner with Retry and Flaky Test Tracking
#
# Runs chaos tests with automatic retry for transient failures.
# Reports flaky (passed on retry) vs genuine (failed all attempts) separately.
#
# Usage: ./scripts/run-chaos-tests.sh [max_retries]
#   max_retries: Number of retry attempts (default: 2)
#
# Exit codes:
#   0 — All tests passed (possibly after retries)
#   1 — At least one test had a genuine failure

set -euo pipefail

MAX_RETRIES="${1:-2}"
VARPULIS_BIN="${VARPULIS_BIN:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors (disabled in CI if NO_COLOR is set)
if [[ -z "${NO_COLOR:-}" && -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' NC=''
fi

# Collect test names from the chaos test modules
CHAOS_TESTS=(
    "test_basic_failover"
    "test_failover_preserves_state"
    "test_drain_worker"
    "test_rebalance_on_join"
    "test_replica_deployment"
    "test_replica_hash_partitioning"
    "test_migration_during_injection"
    "test_all_workers_die"
    "test_coordinator_survives_empty_cluster"
    "test_deploy_undeploy_idempotent"
    "test_inject_to_deleted_group"
    "test_max_workers_scaling"
    "test_failover_latency"
    "test_batch_injection_throughput"
    "test_multi_group_migration_throughput"
    "test_chaos_monkey"
    "test_sustained_event_processing"
    "test_rolling_restart"
)

# Track results
declare -A RESULTS  # test -> "passed" | "flaky" | "failed"
FLAKY_TESTS=()
FAILED_TESTS=()
PASSED_TESTS=()

echo "=========================================="
echo " Chaos Test Runner (max retries: $MAX_RETRIES)"
echo "=========================================="
echo ""

for test_name in "${CHAOS_TESTS[@]}"; do
    attempt=0
    passed=false

    while [[ $attempt -le $MAX_RETRIES ]]; do
        if [[ $attempt -eq 0 ]]; then
            printf "  %-45s " "$test_name"
        else
            printf "  %-45s " "  (retry $attempt/$MAX_RETRIES)"
        fi

        # Run the individual test
        if cargo test -p varpulis-cluster --test chaos -- --ignored --test-threads=1 --exact "$test_name" 2>&1 | tail -5 > /tmp/chaos_test_output_$$ 2>&1; then
            if [[ $attempt -eq 0 ]]; then
                printf "${GREEN}PASS${NC}\n"
                PASSED_TESTS+=("$test_name")
            else
                printf "${YELLOW}PASS (flaky)${NC}\n"
                FLAKY_TESTS+=("$test_name")
            fi
            RESULTS[$test_name]="passed"
            passed=true
            break
        else
            if [[ $attempt -lt $MAX_RETRIES ]]; then
                printf "${YELLOW}FAIL (will retry)${NC}\n"
            else
                printf "${RED}FAIL${NC}\n"
            fi
        fi

        attempt=$((attempt + 1))
    done

    if [[ "$passed" != "true" ]]; then
        FAILED_TESTS+=("$test_name")
        RESULTS[$test_name]="failed"
    fi
done

rm -f /tmp/chaos_test_output_$$

echo ""
echo "=========================================="
echo " Results"
echo "=========================================="
echo ""
echo -e "  ${GREEN}Passed:${NC}  ${#PASSED_TESTS[@]}"
echo -e "  ${YELLOW}Flaky:${NC}   ${#FLAKY_TESTS[@]}"
echo -e "  ${RED}Failed:${NC}  ${#FAILED_TESTS[@]}"
echo "  Total:   ${#CHAOS_TESTS[@]}"
echo ""

if [[ ${#FLAKY_TESTS[@]} -gt 0 ]]; then
    echo -e "${YELLOW}Flaky tests (passed on retry):${NC}"
    for t in "${FLAKY_TESTS[@]}"; do
        echo "  - $t"
    done
    echo ""
    echo "Consider adding these to tests/flaky.txt for tracking."
fi

if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
    echo -e "${RED}Genuine failures:${NC}"
    for t in "${FAILED_TESTS[@]}"; do
        echo "  - $t"
    done
    echo ""
    exit 1
fi

echo -e "${GREEN}All chaos tests passed.${NC}"
exit 0
