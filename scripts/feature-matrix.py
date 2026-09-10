#!/usr/bin/env python3
"""Emit the CI feature matrix, derived from the workspace's own Cargo.toml files.

The matrix used to be a hand-written list in ci.yml. It drifted: `saas` was
never added, so `varpulis-cli`'s test targets stopped compiling under it and
nothing noticed for months. A hand-maintained list of what to verify is a list
that eventually stops matching what exists, so this generates it instead.

Every declared feature is covered unless it appears in SKIP below with a
reason. Adding a feature to a Cargo.toml adds it to CI on the next run; the
only way to leave one out is to say so here, in public, with a justification.

Output (stdout): a JSON array of {"feature", "packages", "name"} objects for
`fromJSON()` in a GitHub Actions matrix.
"""

import json
import pathlib
import sys
import tomllib

# Features deliberately not in the matrix. Key -> why. Anything here is NOT
# verified by the feature-flags job, so keep the list short and the reasons
# real. "It fails" is not a reason to add an entry; fix it or track it.
SKIP = {
    # Bench-only harness switches. `--all-targets` builds benches anyway, so
    # the code behind them is compiled by the jobs that own those crates.
    "bench": "criterion harness toggle, no code of its own",
    "iai-bench": "iai-callgrind harness toggle, needs valgrind on the runner",
    # Covered more directly by a dedicated job than by a host-target build.
    "wasm": "built for real by the WASM Build job on wasm32-unknown-unknown",
    # On by default, so every ordinary check/test/clippy job already has it on.
    # Listed rather than auto-detected: a feature leaving `default` should
    # force a visible edit here, not silently drop out of CI.
    "async-runtime": "in varpulis-runtime's default set",
    "arrow": "in varpulis-runtime's default set",
}


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    declared: dict[str, list[str]] = {}
    for manifest in sorted(root.glob("crates/*/Cargo.toml")):
        data = tomllib.loads(manifest.read_text())
        name = data.get("package", {}).get("name", manifest.parent.name)
        for feature in data.get("features", {}):
            if feature != "default":
                declared.setdefault(feature, []).append(name)

    unknown = sorted(set(SKIP) - set(declared))
    if unknown:
        print(
            f"error: SKIP names features no crate declares: {unknown}. "
            "Remove them; a stale skip entry hides nothing but itself.",
            file=sys.stderr,
        )
        return 1

    # Verify the default sets really do carry the features excused as default,
    # so that entry cannot rot into a silent exclusion.
    defaulted: set[str] = set()
    for manifest in sorted(root.glob("crates/*/Cargo.toml")):
        data = tomllib.loads(manifest.read_text())
        features = data.get("features", {})
        pending = list(features.get("default", []))
        seen: set[str] = set()
        while pending:
            item = pending.pop()
            if item in seen or item not in features:
                continue
            seen.add(item)
            pending.extend(features[item])
        defaulted |= seen
    lying = sorted(
        f
        for f, why in SKIP.items()
        if "default set" in why and f not in defaulted
    )
    if lying:
        print(
            f"error: {lying} are excused as default-on but are not in any "
            "crate's default set, so nothing in CI enables them. Add them to "
            "the matrix by deleting their SKIP entry.",
            file=sys.stderr,
        )
        return 1

    matrix = [
        {
            "feature": feature,
            "packages": " ".join(f"-p {c}" for c in crates),
            "name": f"{feature} ({', '.join(c.removeprefix('varpulis-') for c in crates)})",
        }
        for feature, crates in sorted(declared.items())
        if feature not in SKIP
    ]
    json.dump(matrix, sys.stdout)
    print(file=sys.stderr)
    print(f"{len(matrix)} features in the matrix, {len(SKIP)} skipped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
