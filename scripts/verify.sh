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

# ----- clippy strategy ------------------------------------------------------
#
# We run clippy in two passes, mirroring what CI does for `cargo check` and
# `cargo test` (`.github/workflows/ci.yml`):
#
#   1. **Workspace pass with excludes.** One `cargo clippy --workspace
#      --exclude X --exclude Y --exclude Z --all-targets -- -D warnings`
#      analyzes the dep graph ONCE for every crate except the three
#      problematic connectors, instead of 30+ separate clippy
#      invocations each rebuilding shared deps. ~10× faster.
#
#   2. **Per-crate pass for the excluded connectors.** Workspace clippy
#      would unify optional features across all members, forcing
#      `openssl-sys` (Pulsar via tokio-runtime, Kafka via rdkafka-sys)
#      and elasticsearch's TLS chain to compile with the wrong feature
#      set. Per-crate clippy each gets its own clean default features.
#
# CONNECTOR_CRATES is intentionally short — only the three that hit the
# openssl-sys / feature-unification trap. New "well-behaved" connectors
# (no openssl-sys-via-features, no rdkafka-sys) belong in the workspace
# pass, not here.
CONNECTOR_CRATES=(
    varpulis-connector-elasticsearch
    varpulis-connector-kafka
    varpulis-connector-pulsar
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

# Steps are ordered cheapest-first so a failing commit fails fast: a
# license violation should cost ~10s to detect, not 90s of clippy
# compilation. fmt → audit → deny → clippy.

# ----- 1. nightly rustfmt (~1s) ---------------------------------------------
fmt_step() {
    cargo +nightly fmt --all -- --check
}
if [[ "${SKIP_FMT:-0}" != "1" ]]; then
    run_step "cargo +nightly fmt --check" fmt_step
else
    echo "${YELLOW}  · skipping fmt (SKIP_FMT=1)${RESET}"
fi

# ----- 2. cargo audit (~5s) -------------------------------------------------
audit_step() {
    cargo audit
}
if [[ "${SKIP_AUDIT:-0}" != "1" ]]; then
    run_step "cargo audit" audit_step
else
    echo "${YELLOW}  · skipping audit (SKIP_AUDIT=1)${RESET}"
fi

# ----- 3. cargo deny (~10s) -------------------------------------------------
deny_step() {
    cargo deny check
}
if [[ "${SKIP_DENY:-0}" != "1" ]]; then
    run_step "cargo deny check" deny_step
else
    echo "${YELLOW}  · skipping deny (SKIP_DENY=1)${RESET}"
fi

# ----- 4. clippy (workspace pass + per-crate excluded connectors) ----------
clippy_step() {
    # Pass 1: single workspace clippy excluding the openssl-sys connectors.
    # Same exclude list CI uses for `cargo check --workspace`.
    printf '  %s· workspace (excluding openssl-sys connectors)%s\n' "$DIM" "$RESET"
    local exclude_args=()
    for c in "${CONNECTOR_CRATES[@]}"; do
        exclude_args+=("--exclude" "$c")
    done
    cargo clippy --workspace "${exclude_args[@]}" --all-targets -- -D warnings

    # Pass 2: per-crate for the excluded connectors (avoids feature
    # unification triggering openssl-sys with the wrong feature set).
    for crate in "${CONNECTOR_CRATES[@]}"; do
        printf '  %s· clippy %s%s\n' "$DIM" "$crate" "$RESET"
        cargo clippy -p "$crate" --all-targets -- -D warnings
    done
}
if [[ "${SKIP_CLIPPY:-0}" != "1" ]]; then
    run_step "cargo clippy (workspace + 3 connectors, -D warnings)" clippy_step
else
    echo "${YELLOW}  · skipping clippy (SKIP_CLIPPY=1)${RESET}"
fi

# ----- summary --------------------------------------------------------------
OVERALL_END=$(date +%s%N)
TOTAL_MS=$(( (OVERALL_END - OVERALL_START) / 1000000 ))
echo
echo "${GREEN}${BOLD}✓ verify suite green${RESET} ${DIM}(${TOTAL_MS}ms total)${RESET}"
