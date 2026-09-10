#!/usr/bin/env bash
# Chaos Test Runner with Retry and Flaky Test Tracking
#
# Runs chaos tests with automatic retry for transient failures.
# Reports flaky (passed on retry) vs genuine (failed all attempts) separately.
#
# Usage: ./scripts/run-chaos-tests.sh [max_retries]
#   max_retries: Number of retry attempts (default: 2)
#
# Environment:
#   CHAOS_FEATURES  cargo features for the chaos test binary
#                   (default: distributed-checkpoint,raft). Without these the
#                   coordinator-failover / distributed-checkpoint /
#                   multi-replica / network-partition modules are #[cfg]'d out
#                   of the binary entirely and cannot run at all.
#   CHAOS_NATS=1    declare that a NATS broker is reachable on 4222 AND that
#                   VARPULIS_BIN was built with `--features raft,nats-transport`.
#                   Runs tier 2; under the flag a "[skip]" is a hard failure.
#   CHAOS_DC=1      declare a VARPULIS_BIN that speaks the distributed
#                   checkpoint protocol (plus Kafka on 9092 in a container
#                   named varpulis-kafka, NATS, and docker). Runs tier 3, same
#                   no-abstaining rule. Not satisfiable today — see tier 3.
#   VARPULIS_BIN    path to the varpulis binary the harness spawns.
#
# Exit codes:
#   0 — All selected tests passed (possibly after retries)
#   1 — At least one test had a genuine failure, or the manifest below is out
#       of sync with the tests that actually exist.
#
# ---------------------------------------------------------------------------
# WHY THE NAMES BELOW ARE FULLY QUALIFIED
#
# The runner invokes `cargo test ... -- --exact "$name"`. libtest's `--exact`
# compares against the test's FULL path (`functional::test_basic_failover`),
# not its bare function name. A bare name matches zero tests, and a run that
# selects zero tests exits 0 — which is how this script reported PASS for 18
# tests while executing none of them. `verify_manifest` below now makes any
# such mismatch fatal, in both directions.
# ---------------------------------------------------------------------------

set -euo pipefail

MAX_RETRIES="${1:-2}"
VARPULIS_BIN="${VARPULIS_BIN:-}"
# `-` not `:-`: an explicitly empty CHAOS_FEATURES means "no features".
CHAOS_FEATURES="${CHAOS_FEATURES-distributed-checkpoint,raft}"
CHAOS_NATS="${CHAOS_NATS:-0}"
CHAOS_DC="${CHAOS_DC:-0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Colors (disabled in CI if NO_COLOR is set)
if [[ -z "${NO_COLOR:-}" && -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' NC=''
fi

CARGO_TEST_ARGS=(-p varpulis-cluster --test chaos)
if [[ -n "$CHAOS_FEATURES" ]]; then
    CARGO_TEST_ARGS+=(--features "$CHAOS_FEATURES")
fi

# ---------------------------------------------------------------------------
# Tier 1 — self-contained. Spawns real coordinator/worker processes from the
# varpulis binary and talks to them over loopback HTTP. Needs no broker, no
# docker, no network egress. These must genuinely run in every chaos CI run.
# ---------------------------------------------------------------------------
CHAOS_TESTS=(
    "functional::test_basic_failover"
    "functional::test_failover_preserves_state"
    "functional::test_drain_worker"
    "functional::test_rebalance_on_join"
    "functional::test_replica_deployment"
    "functional::test_replica_hash_partitioning"
    "functional::test_migration_during_injection"
    "edge_cases::test_all_workers_die"
    "edge_cases::test_failover_target_also_dies"
    "edge_cases::test_double_drain"
    "edge_cases::test_rapid_worker_join_leave"
    "perf::test_failover_latency"
    "perf::test_migration_throughput"
    "perf::test_replica_throughput_scaling"
    "sustained::test_chaos_monkey"
)

# ---------------------------------------------------------------------------
# Tier 2 — needs a NATS broker on 4222 and a VARPULIS_BIN built with
# `--features raft,nats-transport`. Both are cheap for CI to provide (a
# `nats:latest` service container and two cargo feature flags), so CI sets
# CHAOS_NATS=1 and these run for real.
#
#   coordinator_failover::…leader_election  raft in the binary (no broker)
#   network_partition::…real_nats           nats-transport + NATS
# ---------------------------------------------------------------------------
CHAOS_NATS_TESTS=(
    "coordinator_failover::test_coordinator_failover_leader_election"
    "network_partition::test_network_partition_real_nats"
)

# ---------------------------------------------------------------------------
# Tier 3 — everything that needs the spawned processes to actually SPEAK the
# distributed checkpoint protocol. Compiled (so they cannot rot) but NOT
# runnable, and not because CI lacks a container.
#
# THE BLOCKER, precisely: `distributed-checkpoint` is a feature of
# `varpulis-cluster` only. `crates/varpulis-cli/Cargo.toml` [features] exposes
# kafka / nats / nats-transport / onnx / k8s / raft / persistent / saas / oidc /
# … and NO `distributed-checkpoint` passthrough. So the coordinator and worker
# processes these tests spawn are built without it, and
# `nats_worker.rs::handle_checkpoint_barrier` — which is
# #[cfg(all(feature = "nats-transport", feature = "distributed-checkpoint"))] —
# simply does not exist in the binary. A worker therefore never acks a barrier.
# Verified empirically: with NATS up and a binary built
# `--features raft,nats-transport`, test_coordinator_failover_workers_self_abort
# fails on "worker must publish an ack within 3s".
#
# What is missing is a one-line CLI feature passthrough (a CLI/engine change,
# owned elsewhere), not infrastructure. The Kafka half is already solved:
# tests/integration/docker-compose.kafka.yml brings up a broker on 9092 in a
# container named `varpulis-kafka`, which is exactly what these tests
# `docker exec` into to create and seed topics.
#
# Until the passthrough lands these are reported as NOT RUN rather than being
# allowed to report a green "[skip]".
# ---------------------------------------------------------------------------
CHAOS_DC_TESTS=(
    "coordinator_failover::test_coordinator_failover_workers_self_abort"
    "distributed_checkpoint::test_distributed_exactly_once"
    "distributed_checkpoint::test_distributed_exactly_once_smoke"
    "multi_replica_checkpoint::test_multi_replica_checkpoint"
    "multi_replica_checkpoint::test_multi_replica_checkpoint_smoke"
)

# Track results
FLAKY_TESTS=()
FAILED_TESTS=()
PASSED_TESTS=()
SKIPPED_TESTS=()
NOT_RUN_TESTS=()

echo "=========================================="
echo " Chaos Test Runner (max retries: $MAX_RETRIES)"
echo "=========================================="
echo "  features : ${CHAOS_FEATURES:-<none>}"
echo "  NATS     : $([[ "$CHAOS_NATS" == "1" ]] && echo "declared available" || echo "NOT available")"
echo "  dist-ckpt: $([[ "$CHAOS_DC" == "1" ]] && echo "declared available" || echo "NOT available")"
echo ""

# ---------------------------------------------------------------------------
# Manifest verification — the guard that makes a green run mean something.
#
# Asks the compiled test binary what #[ignore]d tests it actually contains and
# compares against the manifest above. A name in the manifest that matches no
# test is fatal (it would silently select zero tests and "pass"); a test in the
# binary that is missing from the manifest is fatal too (it would silently
# never run, which is how five real chaos tests sat unexecuted).
# ---------------------------------------------------------------------------
verify_manifest() {
    local listing status
    echo "Building the chaos test binary and listing its tests..."
    if ! listing="$(cargo test "${CARGO_TEST_ARGS[@]}" -- --ignored --list 2>&1)"; then
        echo -e "${RED}FATAL: could not build/list the chaos test binary${NC}"
        echo "$listing" | tail -40
        exit 1
    fi

    # `--list` prints "module::name: test" lines.
    local actual
    actual="$(echo "$listing" | sed -n 's/^\(.*\): test$/\1/p' | sort -u)"

    if [[ -z "$actual" ]]; then
        echo -e "${RED}FATAL: the chaos test binary reports zero #[ignore]d tests${NC}"
        exit 1
    fi

    local declared
    declared="$(printf '%s\n' "${CHAOS_TESTS[@]}" "${CHAOS_NATS_TESTS[@]}" \
        "${CHAOS_DC_TESTS[@]}" | sort -u)"

    local phantom missing
    phantom="$(comm -23 <(echo "$declared") <(echo "$actual"))"
    missing="$(comm -13 <(echo "$declared") <(echo "$actual"))"

    status=0
    if [[ -n "$phantom" ]]; then
        echo -e "${RED}FATAL: these names match no test in the chaos binary${NC}"
        echo "  (a --exact filter matching nothing exits 0, i.e. a vacuous PASS)"
        while IFS= read -r t; do echo "  - $t"; done <<<"$phantom"
        status=1
    fi
    if [[ -n "$missing" ]]; then
        echo -e "${RED}FATAL: these chaos tests exist but are not in the manifest${NC}"
        echo "  (they would never run — add them to CHAOS_TESTS, CHAOS_NATS_TESTS"
        echo "   or CHAOS_DC_TESTS)"
        while IFS= read -r t; do echo "  - $t"; done <<<"$missing"
        status=1
    fi
    if [[ $status -ne 0 ]]; then
        echo ""
        echo "Tests present in the binary (features: ${CHAOS_FEATURES:-<none>}):"
        while IFS= read -r t; do echo "  * $t"; done <<<"$actual"
        exit 1
    fi

    local count
    count="$(echo "$actual" | wc -l | tr -d ' ')"
    echo -e "${GREEN}Manifest verified:${NC} $count chaos tests compiled and accounted for."
    echo ""
}

# Run one test with retries. $1 = fully-qualified name, $2 = "1" when the
# infrastructure this tier needs has been declared present (in which case an
# abstention is a failure, not a skip).
run_one() {
    local test_name="$1" infra_declared="$2"
    local attempt=0 passed=false out
    local outfile
    outfile="$(mktemp)"

    while [[ $attempt -le $MAX_RETRIES ]]; do
        if [[ $attempt -eq 0 ]]; then
            printf "  %-62s " "$test_name"
        else
            printf "  %-62s " "  (retry $attempt/$MAX_RETRIES)"
        fi

        if cargo test "${CARGO_TEST_ARGS[@]}" -- --ignored --test-threads=1 --nocapture \
            --exact "$test_name" >"$outfile" 2>&1; then
            out="$(cat "$outfile")"

            # A test that ran but printed "[skip]" abstained. It did not verify
            # anything, so it is never a PASS.
            if echo "$out" | grep -q '\[skip\]'; then
                if [[ "$infra_declared" == "1" ]]; then
                    printf "${RED}FAIL (abstained although its infra was declared)${NC}\n"
                    echo "$out" | grep '\[skip\]' | sed 's/^/      /'
                    FAILED_TESTS+=("$test_name (skipped although infra was declared)")
                    rm -f "$outfile"
                    return 1
                fi
                printf "${YELLOW}SKIPPED${NC}\n"
                echo "$out" | grep '\[skip\]' | head -1 | sed 's/^/      /'
                SKIPPED_TESTS+=("$test_name")
                rm -f "$outfile"
                return 0
            fi

            # Guard against a filter that selected nothing (belt-and-braces —
            # verify_manifest should already have caught it).
            if echo "$out" | grep -qE 'running 0 tests'; then
                printf "${RED}FAIL (selected 0 tests)${NC}\n"
                FAILED_TESTS+=("$test_name (filter matched no test)")
                rm -f "$outfile"
                return 1
            fi

            if [[ $attempt -eq 0 ]]; then
                printf "${GREEN}PASS${NC}\n"
                PASSED_TESTS+=("$test_name")
            else
                printf "${YELLOW}PASS (flaky)${NC}\n"
                FLAKY_TESTS+=("$test_name")
            fi
            passed=true
            break
        else
            if [[ $attempt -lt $MAX_RETRIES ]]; then
                printf "${YELLOW}FAIL (will retry)${NC}\n"
            else
                printf "${RED}FAIL${NC}\n"
                tail -25 "$outfile" | sed 's/^/      /'
            fi
        fi

        attempt=$((attempt + 1))
    done

    rm -f "$outfile"
    if [[ "$passed" != "true" ]]; then
        FAILED_TESTS+=("$test_name")
        return 1
    fi
    return 0
}

verify_manifest

echo "--- Tier 1: self-contained (${#CHAOS_TESTS[@]} tests) ---"
for test_name in "${CHAOS_TESTS[@]}"; do
    run_one "$test_name" "0" || true
done

echo ""
if [[ "$CHAOS_NATS" == "1" ]]; then
    echo "--- Tier 2: NATS + raft (${#CHAOS_NATS_TESTS[@]} tests) ---"
    for test_name in "${CHAOS_NATS_TESTS[@]}"; do
        run_one "$test_name" "1" || true
    done
else
    echo "--- Tier 2: NATS + raft (${#CHAOS_NATS_TESTS[@]} tests) — NOT RUN ---"
    echo "  Needs NATS on 4222 and VARPULIS_BIN built with --features raft,nats-transport."
    echo "  Set CHAOS_NATS=1 once both hold; they then fail instead of skipping."
    for test_name in "${CHAOS_NATS_TESTS[@]}"; do
        echo "  - $test_name"
        NOT_RUN_TESTS+=("$test_name")
    done
fi

echo ""
if [[ "$CHAOS_DC" == "1" ]]; then
    echo "--- Tier 3: distributed checkpoint (${#CHAOS_DC_TESTS[@]} tests) ---"
    for test_name in "${CHAOS_DC_TESTS[@]}"; do
        run_one "$test_name" "1" || true
    done
else
    echo "--- Tier 3: distributed checkpoint (${#CHAOS_DC_TESTS[@]} tests) — NOT RUN ---"
    echo "  BLOCKED: varpulis-cli exposes no 'distributed-checkpoint' feature"
    echo "  (crates/varpulis-cli/Cargo.toml [features]), so the coordinator/worker"
    echo "  processes these tests spawn are built without nats_worker.rs's"
    echo "  handle_checkpoint_barrier and can never ack a barrier."
    echo "  Kafka + NATS containers alone are NOT sufficient; a CLI feature"
    echo "  passthrough is. See the tier-3 comment in this script."
    for test_name in "${CHAOS_DC_TESTS[@]}"; do
        echo "  - $test_name"
        NOT_RUN_TESTS+=("$test_name")
    done
fi

echo ""
echo "=========================================="
echo " Results"
echo "=========================================="
echo ""
echo -e "  ${GREEN}Passed:${NC}   ${#PASSED_TESTS[@]}"
echo -e "  ${YELLOW}Flaky:${NC}    ${#FLAKY_TESTS[@]}"
echo -e "  ${YELLOW}Skipped:${NC}  ${#SKIPPED_TESTS[@]}"
echo -e "  ${YELLOW}Not run:${NC}  ${#NOT_RUN_TESTS[@]}"
echo -e "  ${RED}Failed:${NC}   ${#FAILED_TESTS[@]}"
echo "  Declared: $(( ${#CHAOS_TESTS[@]} + ${#CHAOS_NATS_TESTS[@]} + ${#CHAOS_DC_TESTS[@]} ))"
echo ""

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Chaos tests"
        echo ""
        echo "| outcome | count |"
        echo "|---|---|"
        echo "| passed | ${#PASSED_TESTS[@]} |"
        echo "| flaky | ${#FLAKY_TESTS[@]} |"
        echo "| skipped (no infra) | ${#SKIPPED_TESTS[@]} |"
        echo "| not run (no infra) | ${#NOT_RUN_TESTS[@]} |"
        echo "| failed | ${#FAILED_TESTS[@]} |"
        for t in "${SKIPPED_TESTS[@]}" "${NOT_RUN_TESTS[@]}"; do
            echo ""
            echo "- :warning: not verified: \`$t\`"
        done
    } >>"$GITHUB_STEP_SUMMARY"
fi

if [[ ${#FLAKY_TESTS[@]} -gt 0 ]]; then
    echo -e "${YELLOW}Flaky tests (passed on retry):${NC}"
    for t in "${FLAKY_TESTS[@]}"; do
        echo "  - $t"
    done
    echo ""
    echo "Consider adding these to tests/flaky.txt for tracking."
fi

if [[ ${#SKIPPED_TESTS[@]} -gt 0 || ${#NOT_RUN_TESTS[@]} -gt 0 ]]; then
    echo -e "${YELLOW}NOT VERIFIED by this run:${NC}"
    for t in "${SKIPPED_TESTS[@]}" "${NOT_RUN_TESTS[@]}"; do
        echo "  - $t"
    done
    echo ""
fi

if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
    echo -e "${RED}Genuine failures:${NC}"
    for t in "${FAILED_TESTS[@]}"; do
        echo "  - $t"
    done
    echo ""
    exit 1
fi

echo -e "${GREEN}${#PASSED_TESTS[@]} chaos tests executed and passed.${NC}"
exit 0
