#!/usr/bin/env bash
#
# scripts/git-push-gate.sh — PreToolUse hook helper
#
# Wired into /home/cpo/cep/.claude/settings.local.json on the Bash tool's
# PreToolUse event. For every Bash invocation in this project the hook
# script is spawned with the tool input on stdin (JSON). If the command
# contains `git push`, we run the verify suite (`bash scripts/verify.sh`)
# and emit a `permissionDecision: "deny"` JSON if it fails — blocking the
# push at the harness level. Otherwise we emit `allow` immediately.
#
# Why this exists: the verify suite already exists, CI runs it, CLAUDE.md
# documents it, the rust-core skill explains it, and memory says "always
# run before pushing" — but every layer is tribal knowledge that any
# agent (including Claude itself) can skip by going straight to
# `git push`. Convention without enforcement always loses; this script
# is the enforcement.
#
# Always exits 0. The decision is communicated via JSON on stdout —
# that's the modern PreToolUse contract per Claude Code's hook schema.
# Exiting non-zero would leave behavior undefined; exiting 0 with the
# correct JSON is well-specified.

set -uo pipefail

REPO_ROOT="/home/cpo/cep"

# Read the hook input from stdin. Claude Code sends a JSON object with
# `tool_name` and `tool_input.command` for Bash tool calls. Tolerate
# missing/empty stdin (manual invocation) by allowing through.
input_json=""
if ! [ -t 0 ]; then
    input_json=$(cat)
fi

command=""
if [ -n "$input_json" ]; then
    command=$(printf '%s' "$input_json" | jq -r '.tool_input.command // empty' 2>/dev/null || true)
fi

emit_allow() {
    printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow"}}\n'
}

emit_deny() {
    local reason=$1
    jq -nc --arg r "$reason" \
        '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":$r}}'
}

# Fast-path: only gate `git push`. Everything else is allowed without
# even spawning verify.sh. Substring match catches chained commands
# like `git commit -m '...' && git push origin main`.
case "$command" in
    *"git push"*) ;;  # fall through to gating below
    *)
        emit_allow
        exit 0
        ;;
esac

# This IS a git-push. Determine WHAT is being pushed.
cd "$REPO_ROOT" || {
    emit_deny "git-push-gate: cannot cd $REPO_ROOT — aborting push"
    exit 0
}

# Multi-language awareness: this repo contains Rust crates, TypeScript
# (stream-builder), Vue, Node demos, etc. The Rust verify suite
# (fmt + audit + deny + clippy) only needs to run when Rust files are
# being pushed. For other technologies, no verify suite is wired yet —
# they pass through. Add their gates here when they exist.
#
# Decide between three outcomes:
#   (a) Diff is empty (already up-to-date with upstream): allow.
#       git itself will report "Everything up-to-date" — nothing to gate.
#   (b) Diff is non-empty AND contains only non-Rust files: allow.
#       (No verify suite configured for those techs yet.)
#   (c) Diff is non-empty AND contains Rust files, OR we couldn't
#       compute the diff at all (new branch / detached HEAD / no
#       remote configured): run the Rust verify suite. The "couldn't
#       compute" path is the conservative fallback — over-verifying
#       is friction; under-verifying is the bug this gate exists to fix.

# Returns 0 on success and prints (possibly empty) file list.
# Returns non-zero if no upstream could be determined.
detect_changed_files() {
    local branch upstream
    branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
    if [ -n "$branch" ] && git rev-parse --verify --quiet "origin/$branch" >/dev/null 2>&1; then
        upstream="origin/$branch"
    elif git rev-parse --verify --quiet "origin/main" >/dev/null 2>&1; then
        upstream="origin/main"
    else
        return 1
    fi
    git diff --name-only "$upstream"..HEAD 2>/dev/null
}

if changed=$(detect_changed_files); then
    if [ -z "$changed" ]; then
        # (a) Nothing to push.
        emit_allow
        exit 0
    fi
    if ! printf '%s\n' "$changed" \
        | grep -qE '\.rs$|^Cargo\.(toml|lock)$|/Cargo\.toml$|^crates/|^scripts/verify\.sh$|^Makefile$|^deny\.toml$|^rust-toolchain'; then
        # (b) Non-Rust-only push. No verify suite configured for
        # other techs yet — pass through.
        emit_allow
        exit 0
    fi
    # Fall through to (c) — diff has Rust files.
fi
# else (c) — couldn't determine upstream, run verify defensively.

# Capture verify output (last 120 lines is enough context for the agent
# to understand which gate failed without flooding the response).
verify_output=$(bash scripts/verify.sh 2>&1)
verify_status=$?

if [ $verify_status -eq 0 ]; then
    emit_allow
    exit 0
fi

truncated=$(printf '%s' "$verify_output" | tail -120)
reason="verify suite FAILED (exit $verify_status) — git push BLOCKED. Run \`make verify\` locally, fix the gate that failed, and re-push. Last 120 lines:

$truncated"
emit_deny "$reason"
exit 0
