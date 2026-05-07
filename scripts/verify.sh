#!/usr/bin/env bash
#
# scripts/verify.sh — local verification suite (fmt + clippy + audit + deny)
#
# Mirrors the gates that CI runs so a clean local pass should keep CI green.
# Idempotent: safe to re-run; no state outside cargo's target dir is touched.
# Exits non-zero on the first failing step and prints which step failed.
#
# Usage:
#     ./scripts/verify.sh            # run all gates
#     SKIP_AUDIT=1 ./scripts/verify.sh
#     SKIP_DENY=1  ./scripts/verify.sh
#     SKIP_FMT=1   ./scripts/verify.sh
#     SKIP_CLIPPY=1 ./scripts/verify.sh
#

set -Eeuo pipefail  # -E so the ERR trap fires inside run_step functions

# ----- presentation ---------------------------------------------------------
if [[ -t 1 ]]; then
    BOLD=$'\e[1m'; DIM=$'\e[2m'
    RED=$'\e[31m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; CYAN=$'\e[36m'
    RESET=$'\e[0m'
else
    BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; CYAN=""; RESET=""
fi

CURRENT_STEP=""
on_err() {
    local exit_code=$?
    echo
    echo "${RED}${BOLD}✗ verify suite FAILED at step:${RESET} ${RED}${CURRENT_STEP}${RESET}" >&2
    echo "${RED}  exit code: ${exit_code}${RESET}" >&2
    exit "$exit_code"
}
trap on_err ERR

# Per-step timing helper. Captures wall time in ms via GNU date %s%N.
run_step() {
    local label=$1; shift
    CURRENT_STEP="$label"
    echo
    echo "${CYAN}${BOLD}→ ${label}${RESET}"
    local start end duration_ms
    start=$(date +%s%N)
    "$@"
    end=$(date +%s%N)
    duration_ms=$(( (end - start) / 1000000 ))
    printf '  %b✓ %s%b %b(%dms)%b\n' \
        "$GREEN" "$label" "$RESET" "$DIM" "$duration_ms" "$RESET"
}

# ----- clippy allowlist -----------------------------------------------------
#
# Why an explicit allowlist rather than `--workspace`?
#
# Workspace-wide clippy unifies optional features across all members,
# which forces optional connectors that pull in `openssl-sys` (Pulsar
# via tokio-runtime, Kafka via rdkafka-sys) to compile even when a
# developer only changed an unrelated crate. On systems without
# `libssl-dev` headers that build fails noisily and is the single
# most common verify-suite paper cut on this project.
#
# Per-crate clippy avoids the unification trap: each crate compiles
# only its own (default) features. The allowlist below is every
# workspace member that builds clean on a Linux dev box with stable
# rustc and no extra system packages beyond what `cargo build`
# already needs. CI installs libssl-dev/protobuf-compiler/zlib1g-dev
# and runs the same set workspace-wide as a belt-and-braces check.
#
# `varpulis-wasm` is included: the host-target build (default features,
# no `smartmodule`) is clippy-clean and is what `cargo clippy -p` picks
# unless an explicit `--target wasm32-unknown-unknown` is passed. The
# wasm32 build is covered by a dedicated CI job.
#
# Keep this list sorted; add any new workspace member here.
CLIPPY_CRATES=(
    varpulis
    varpulis-actors
    varpulis-cli
    varpulis-cluster
    varpulis-connector-api
    varpulis-connector-cdc
    varpulis-connector-database
    varpulis-connector-elasticsearch
    varpulis-connector-http
    varpulis-connector-kafka
    varpulis-connector-kinesis
    varpulis-connector-mqtt
    varpulis-connector-nats
    varpulis-connector-pulsar
    varpulis-connector-redis
    varpulis-connector-s3
    varpulis-connector-slack
    varpulis-connector-splunk
    varpulis-connector-syslog
    varpulis-connectors
    varpulis-core
    varpulis-datagen
    varpulis-db
    varpulis-dead-letter
    varpulis-engine-wasm
    varpulis-enrichment
    varpulis-hamlet
    varpulis-lsp
    varpulis-mcp
    varpulis-parser
    varpulis-pst
    varpulis-runtime
    varpulis-sase
    varpulis-simd
    varpulis-wasm
    varpulis-zdd
)

# Anchor to repo root so relative paths inside cargo behave the same
# regardless of the caller's CWD.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

# Make sure openssl-sys can find the system OpenSSL via pkg-config.
# Connectors that route through reqwest's native-tls (elasticsearch,
# slack, splunk, etc.) compile openssl-sys without the rdkafka-sys
# vendored fallback, and the build script aborts if pkg-config cannot
# resolve `openssl.pc`. Some shells ship pkg-config without the
# default search paths, so we prepend the multi-arch dir if it
# exists and the user hasn't already set PKG_CONFIG_PATH.
for _candidate in /usr/lib/x86_64-linux-gnu/pkgconfig /usr/lib64/pkgconfig /usr/lib/pkgconfig; do
    if [[ -f "${_candidate}/openssl.pc" ]]; then
        export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:+${PKG_CONFIG_PATH}:}${_candidate}"
        break
    fi
done
unset _candidate

OVERALL_START=$(date +%s%N)
echo "${BOLD}verify suite${RESET}  ${DIM}(${REPO_ROOT})${RESET}"

# ----- 1. nightly rustfmt ---------------------------------------------------
fmt_step() {
    cargo +nightly fmt --all -- --check
}
if [[ "${SKIP_FMT:-0}" != "1" ]]; then
    run_step "cargo +nightly fmt --check" fmt_step
else
    echo "${YELLOW}  · skipping fmt (SKIP_FMT=1)${RESET}"
fi

# ----- 2. per-crate clippy --------------------------------------------------
clippy_step() {
    for crate in "${CLIPPY_CRATES[@]}"; do
        printf '  %s· clippy %s%s\n' "$DIM" "$crate" "$RESET"
        cargo clippy -p "$crate" --all-targets -- -D warnings
    done
}
if [[ "${SKIP_CLIPPY:-0}" != "1" ]]; then
    run_step "cargo clippy (per-crate, --all-targets, -D warnings)" clippy_step
else
    echo "${YELLOW}  · skipping clippy (SKIP_CLIPPY=1)${RESET}"
fi

# ----- 3. cargo audit -------------------------------------------------------
audit_step() {
    cargo audit
}
if [[ "${SKIP_AUDIT:-0}" != "1" ]]; then
    run_step "cargo audit" audit_step
else
    echo "${YELLOW}  · skipping audit (SKIP_AUDIT=1)${RESET}"
fi

# ----- 4. cargo deny --------------------------------------------------------
deny_step() {
    cargo deny check
}
if [[ "${SKIP_DENY:-0}" != "1" ]]; then
    run_step "cargo deny check" deny_step
else
    echo "${YELLOW}  · skipping deny (SKIP_DENY=1)${RESET}"
fi

# ----- summary --------------------------------------------------------------
OVERALL_END=$(date +%s%N)
TOTAL_MS=$(( (OVERALL_END - OVERALL_START) / 1000000 ))
echo
echo "${GREEN}${BOLD}✓ verify suite green${RESET} ${DIM}(${TOTAL_MS}ms total)${RESET}"
