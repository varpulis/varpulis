#!/usr/bin/env python3
"""Emit the CI feature matrix, derived from the workspace's own Cargo.toml files.

The matrix used to be a hand-written list in ci.yml. It drifted: `saas` was
never added, so `varpulis-cli`'s test targets stopped compiling under it and
nothing noticed for months. A hand-maintained list of what to verify is a list
that eventually stops matching what exists, so this generates it instead.

Every declared feature is covered unless it appears in SKIP below with a
reason. Adding a feature to a Cargo.toml adds it to CI on the next run; the
only way to leave one out is to say so here, in public, with a justification.

It also covers *combinations*. A matrix that enables one feature at a time
cannot reach code behind `#[cfg(all(feature = "a", feature = "b"))]`: with
only `a` on, the item is compiled out, so `cargo clippy --features a` lints
nothing there and reports success. Four such gates existed and none of them
was ever linted — including the chaos coordinator-failover harness, which is
gated on two features at once. So the combinations are read out of the sources
the same way the features are read out of the manifests: a new `cfg(all(...))`
adds its own CI entry on the next run, and nobody has to remember.

Negated features (`not(feature = "x")`) are ignored when forming a
combination: they describe a configuration in which the feature is *off*,
which every entry that does not enable it already covers.

Output (stdout): a JSON array of {"feature", "packages", "name"} objects for
`fromJSON()` in a GitHub Actions matrix. For a combination, "feature" is the
comma-separated list `cargo --features` expects.
"""

import json
import pathlib
import re
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
    "wasm": "built on wasm32 by the WASM Build job, via varpulis-engine-wasm",
    # On by default, so every ordinary check/test/clippy job already has it on.
    # Listed rather than auto-detected: a feature leaving `default` should
    # force a visible edit here, not silently drop out of CI.
    "async-runtime": "in varpulis-runtime's default set",
    "arrow": "in varpulis-runtime's default set",
}


CFG_ALL = re.compile(r"cfg\(\s*all\((.*?)\)\s*\)", re.S)
FEATURE = re.compile(r'feature\s*=\s*"([a-z0-9-]+)"')
NOT_FEATURE = re.compile(r'not\(\s*feature\s*=\s*"([a-z0-9-]+)"')
# Line comments are stripped first: a doc comment that *describes* a gate
# would otherwise mint a CI entry for a combination no code is behind. A
# comment inside a real multi-line `cfg(all(...))` survives this, because
# removing it leaves the attribute itself intact.
LINE_COMMENT = re.compile(r"//[^\n]*")


def combinations(root: pathlib.Path) -> dict[tuple[str, ...], set[str]]:
    """Feature sets that some `cfg(all(...))` requires together.

    Keyed by the sorted tuple of features, valued by the files that gate on
    them — so an error message can say where the requirement comes from
    instead of just asserting one exists.
    """
    found: dict[tuple[str, ...], set[str]] = {}
    for path in sorted(root.glob("crates/*/**/*.rs")):
        if "/target/" in str(path):
            continue
        text = LINE_COMMENT.sub("", path.read_text(errors="ignore"))
        for match in CFG_ALL.finditer(text):
            body = match.group(1)
            wanted = set(FEATURE.findall(body)) - set(NOT_FEATURE.findall(body))
            if len(wanted) >= 2:
                found.setdefault(tuple(sorted(wanted)), set()).add(
                    str(path.relative_to(root))
                )
    return found


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

    # Combinations, read out of the sources rather than listed here.
    combos = combinations(root)
    for wanted, where in sorted(combos.items()):
        missing = [f for f in wanted if f not in declared]
        if missing:
            print(
                f"error: {sorted(where)} gate on features no crate declares: "
                f"{missing}. A `cfg(all(...))` naming a feature that does not "
                "exist is dead code that can never compile.",
                file=sys.stderr,
            )
            return 1
        skipped = [f for f in wanted if f in SKIP]
        if skipped:
            # A combination is only as verifiable as its parts. Rather than
            # silently dropping it, say so: the SKIP entry now hides a
            # combination too, which its justification has to cover.
            print(
                f"note: not covering {'+'.join(wanted)} (from "
                f"{sorted(where)[0]}) because {skipped} is in SKIP",
                file=sys.stderr,
            )
            continue
        # Every crate that declares *all* of them, so `-p` names something
        # that can actually enable the combination.
        crates = sorted(
            set.intersection(*(set(declared[f]) for f in wanted))
        )
        if not crates:
            print(
                f"error: {sorted(where)} gate on {list(wanted)} together, but "
                "no single crate declares all of them, so no `cargo --features` "
                "invocation can enable the combination. Re-export the features "
                "from one crate, or split the gate.",
                file=sys.stderr,
            )
            return 1
        matrix.append(
            {
                "feature": ",".join(wanted),
                "packages": " ".join(f"-p {c}" for c in crates),
                "name": (
                    f"{' + '.join(wanted)} "
                    f"({', '.join(c.removeprefix('varpulis-') for c in crates)})"
                ),
            }
        )

    json.dump(matrix, sys.stdout)
    print(file=sys.stderr)
    print(
        f"{len(matrix)} entries in the matrix "
        f"({len(matrix) - len(combos)} features, {len(combos)} combinations), "
        f"{len(SKIP)} skipped",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
