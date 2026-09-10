#!/usr/bin/env python3
"""Every `VARPULIS_*` variable the documentation describes must be read by code.

`VARPULIS_ENCRYPTION_PASSPHRASE` had a tutorial section of its own — Argon2id,
memory cost, iteration count, a "verify the files are encrypted" step — and no
Rust code read it. Checkpoints were written in plaintext for anyone who
followed the tutorial exactly, and nothing anywhere said otherwise.

Documentation that promises a control the binary does not implement is the
worst version of that failure, because the reader has no way to tell. This
walks the documentation for `VARPULIS_*` names and fails when one is neither
read by the Rust sources nor listed below as consumed elsewhere.

Usage: scripts/check-documented-env.py
"""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
NAME = re.compile(r"\bVARPULIS_[A-Z0-9_]+\b")

# Names consumed outside the Rust sources. Each needs a reason: this list is
# the only way a documented variable escapes the check, so it must stay short
# and every entry must say who reads it.
CONSUMED_ELSEWHERE = {
    "VARPULIS_WORKER_KEY": "docker-compose.cluster.yml, passed to the worker as --api-key",
}


def documented() -> dict[str, set[str]]:
    """`VARPULIS_*` names appearing in the documentation sources."""
    found: dict[str, set[str]] = {}
    sources = list((ROOT / "docs").rglob("*.md")) + [ROOT / "README.md"]
    for md in sources:
        # `.vitepress/dist` is generated output; checking it would double-report.
        if not md.is_file() or ".vitepress" in md.parts:
            continue
        for name in NAME.findall(md.read_text(errors="replace")):
            found.setdefault(name, set()).add(str(md.relative_to(ROOT)))
    return found


def read_by_rust() -> set[str]:
    names: set[str] = set()
    for rs in (ROOT / "crates").rglob("*.rs"):
        text = rs.read_text(errors="replace")
        # `env::var("NAME")`, and `const X: &str = "NAME"` for the constant form.
        names.update(re.findall(r'var(?:_os)?\s*\(\s*"(VARPULIS_[A-Z0-9_]+)"', text))
        names.update(re.findall(r'=\s*"(VARPULIS_[A-Z0-9_]+)"', text))
        # clap's `env = "NAME"` attribute.
        names.update(re.findall(r'env\s*=\s*"(VARPULIS_[A-Z0-9_]+)"', text))
    return names


def main() -> int:
    docs = documented()
    read = read_by_rust()

    orphans = {k: v for k, v in docs.items() if k not in read and k not in CONSUMED_ELSEWHERE}
    stale = sorted(k for k in CONSUMED_ELSEWHERE if k not in docs and k not in read)

    print(f"{len(docs)} documented VARPULIS_* names, {len(read)} read by Rust, "
          f"{len(CONSUMED_ELSEWHERE)} consumed elsewhere")

    status = 0
    if orphans:
        status = 1
        print("\nDocumented but read by nothing:\n", file=sys.stderr)
        for k, where in sorted(orphans.items()):
            print(f"  {k}\n      documented in {', '.join(sorted(where))}", file=sys.stderr)
        print(
            "\nEither the code reads it or the documentation stops promising it.\n"
            "If something outside crates/ consumes it, add it to CONSUMED_ELSEWHERE\n"
            "with the name of what reads it.",
            file=sys.stderr,
        )
    if stale:
        status = 1
        print(f"\nCONSUMED_ELSEWHERE lists names nothing mentions any more: {stale}",
              file=sys.stderr)
        print("Remove them; a stale exemption hides nothing but itself.", file=sys.stderr)
    if not status:
        print("every documented variable is read by something")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
