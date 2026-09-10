#!/usr/bin/env bash
# Branch protection for `main`, as code.
#
# The protection rule lives in GitHub's settings UI, which means it is
# invisible to review and drifts silently. This script is the record of what it
# is supposed to be, and can both verify and re-apply it.
#
#   ./scripts/branch-protection.sh            # verify (exit 1 on drift)
#   ./scripts/branch-protection.sh --apply    # write the settings below
#
# `enforce_admins` is the load-bearing one: without it every required check is
# advisory for anyone with admin rights, i.e. for the only person who pushes.
set -euo pipefail

REPO="${REPO:-varpulis/varpulis}"
BRANCH="${BRANCH:-main}"
API="repos/$REPO/branches/$BRANCH/protection"

# The fast-gate jobs from .github/workflows/ci.yml that must pass before merge.
REQUIRED_CHECKS=(
    "Check"
    "Test"
    "Clippy"
    "Format"
    "Deny (licenses & advisories)"
    "Audit"
)

payload() {
    python3 - "$@" <<'PY'
import json, sys
checks = sys.argv[1:]
print(json.dumps({
    "required_status_checks": {
        "strict": False,
        "checks": [{"context": c} for c in checks],
    },
    "enforce_admins": True,
    "required_pull_request_reviews": {
        "dismiss_stale_reviews": False,
        "require_code_owner_reviews": False,
        "required_approving_review_count": 0,
    },
    "restrictions": None,
    "allow_force_pushes": False,
    "allow_deletions": False,
}))
PY
}

if [[ "${1:-}" == "--apply" ]]; then
    payload "${REQUIRED_CHECKS[@]}" | gh api -X PUT "$API" --input - >/dev/null
    echo "applied branch protection to $REPO@$BRANCH"
fi

current="$(gh api "$API")"
status=0

enforce_admins="$(echo "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["enforce_admins"]["enabled"])')"
if [[ "$enforce_admins" != "True" ]]; then
    echo "DRIFT: enforce_admins is $enforce_admins — required checks do not apply to admins"
    status=1
fi

actual_checks="$(echo "$current" | python3 -c 'import json,sys; print("\n".join(sorted(json.load(sys.stdin)["required_status_checks"]["contexts"])))')"
expected_checks="$(printf '%s\n' "${REQUIRED_CHECKS[@]}" | sort)"
if [[ "$actual_checks" != "$expected_checks" ]]; then
    echo "DRIFT: required status checks differ"
    echo "  expected: $(echo "$expected_checks" | tr '\n' ',')"
    echo "  actual:   $(echo "$actual_checks" | tr '\n' ',')"
    status=1
fi

if [[ $status -eq 0 ]]; then
    echo "branch protection on $REPO@$BRANCH matches this script"
fi
exit $status
