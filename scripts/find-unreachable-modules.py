#!/usr/bin/env python3
"""Find .rs files the compiler never sees, by walking `mod` from each root.

Unreachable source is worse than dead code, because dead code at least gets
compiled: `cargo check` type-checks it, clippy lints it, and a rename that
breaks it fails the build. A file no `mod` declaration reaches is edited
without effect and without error.

`crates/varpulis-runtime/src/pst/` was 2 174 lines of exactly that. It was the
pre-extraction copy of what became the `varpulis-pst` crate, left behind when
`lib.rs` started saying `pub use varpulis_pst as pst;`, and by then it had
silently drifted ten lines behind the crate that actually runs. Anyone who
went looking for `crate::pst` and opened the directory of that name would have
edited the wrong file and seen nothing happen.

A crate-wide "is this name declared anywhere" check does not find that, because
`mod tree;` inside the orphaned `mod.rs` makes `tree.rs` look declared. This
walks from lib.rs / main.rs / each bin root and reports what it never arrives
at. Exit 1 when anything is unreachable.

Usage: scripts/find-unreachable-modules.py
"""
import sys
import pathlib
import re

# Derived from this file's own location, not hardcoded: the first version of
# this script carried an absolute path to the machine it was written on and
# failed in CI on its first run with "No such file or directory".
ROOT = pathlib.Path(__file__).resolve().parent.parent / 'crates'
MOD = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;', re.M)


def walk(path: pathlib.Path, reached: set[pathlib.Path]) -> None:
    if path in reached or not path.is_file():
        return
    reached.add(path)
    # A file `foo.rs` owns the directory `foo/`; `foo/mod.rs` owns `foo/`.
    base = path.parent if path.name == 'mod.rs' else path.with_suffix('')
    for name in MOD.findall(path.read_text(errors='replace')):
        for cand in (base / f'{name}.rs', base / name / 'mod.rs',
                     path.parent / f'{name}.rs', path.parent / name / 'mod.rs'):
            if cand.is_file():
                walk(cand, reached)
                break


total = 0
for crate in sorted(ROOT.iterdir()):
    src = crate / 'src'
    if not src.is_dir():
        continue
    reached: set[pathlib.Path] = set()
    for entry in ('lib.rs', 'main.rs'):
        walk(src / entry, reached)
    for extra in src.glob('bin/*.rs'):
        walk(extra, reached)
    orphans = sorted(p for p in src.rglob('*.rs') if p not in reached)
    if orphans:
        loc = sum(len(p.read_text(errors='replace').splitlines()) for p in orphans)
        total += loc
        print(f"{crate.name}: {len(orphans)} unreachable file(s), {loc} LOC")
        for p in orphans:
            print(f"    {p.relative_to(ROOT.parent)}")
if total:
    print(f"\ntotal unreachable: {total} LOC", file=sys.stderr)
    print(
        "\nThese files are not compiled, not linted, and not type-checked.\n"
        "Either wire them in with a `mod` declaration or delete them.",
        file=sys.stderr,
    )
    raise SystemExit(1)
print("every .rs file under crates/*/src is reachable from a crate root")
