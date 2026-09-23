#!/usr/bin/env python3
"""Check the VPL in the documentation against the engine in this commit.

Every ```vpl block in docs/ and README.md is extracted. A block that declares a
`stream`, a `pattern` or an `event` at the start of a line, and contains no
`...`, reads as a program a reader can copy, so it must pass `varpulis check`.
The two exceptions are visible on the page:

- a block with `...` in it (`.emit(...)`, `# ... more sectors`) is a sketch,
  and the reader can see that it is one;
- a block that declares nothing (an operator signature, a clause shown on its
  own) is a fragment, and there is no honest way to check half a program.

This used to check only blocks that declared an event type, but a VPL program
needs no `event` declaration to run, so most of the documented programs were
never checked and 47 of them did not parse: postfix `A+`/`A*`, `AND(...)`,
`OR(...)`, `NOT(...)`, bracket predicates, per-step `within`, joins written
with a bare `on`, and a whole tutorial in a syntax the parser dropped long ago.

Usage: check-doc-vpl.py <path to the varpulis binary>
"""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Programs that do NOT check, each kept here with the reason.
#
# The list can only shrink. A block named here that starts passing fails this
# script, and so does one that is no longer checked at all (it moved, became a
# sketch or was deleted), so a fix cannot leave a stale exemption behind. It
# went from eight entries to one: five webMethods blocks needed
# absence-with-timeout, which now works, and two comparison blocks used
# `left_join` as an operator when it is a stream source. The last one, an
# article outline's sketch, ends in `.emit(...)` and is skipped as a sketch.
KNOWN_BROKEN: dict[str, str] = {}

DECLARATION = re.compile(r"^(stream|pattern|event)\s+\w+", re.M)


def blocks():
    """Yield (source location, body) for every ```vpl block."""
    files = sorted(ROOT.glob("docs/**/*.md")) + [ROOT / "README.md"]
    for md in files:
        if not md.exists() or "node_modules" in md.parts:
            continue
        text = md.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(r"^```vpl\n(.*?)^```", text, re.S | re.M):
            line = text[: m.start()].count("\n") + 1
            yield f"{md.relative_to(ROOT)}:{line}", m.group(1)


def is_program(body: str) -> bool:
    """A block that looks runnable: it declares something and elides nothing."""
    return "..." not in body and DECLARATION.search(body) is not None


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <varpulis binary>", file=sys.stderr)
        return 2
    binary = sys.argv[1]

    tmp = ROOT / "target" / "doc-vpl"
    tmp.mkdir(parents=True, exist_ok=True)

    checked: set[str] = set()
    skipped = 0
    broke: list[tuple[str, str]] = []
    fixed: list[str] = []

    for where, body in blocks():
        if not is_program(body):
            skipped += 1
            continue
        checked.add(where)
        path = tmp / (where.replace("/", "__").replace(":", "_L") + ".vpl")
        path.write_text(body)
        run = subprocess.run(
            [binary, "check", str(path)], capture_output=True, text=True
        )
        if run.returncode == 0:
            if where in KNOWN_BROKEN:
                fixed.append(where)
        elif where not in KNOWN_BROKEN:
            lines = [l.strip() for l in (run.stdout + run.stderr).splitlines()
                     if l.strip()]
            first = next((l for l in lines if "×" in l),
                         lines[0] if lines else "(no diagnostic)")
            broke.append((where, first))

    stale = sorted(w for w in KNOWN_BROKEN if w not in checked)

    print(f"{len(checked)} documented programs checked, {skipped} sketches and "
          f"fragments skipped, {len(KNOWN_BROKEN)} known-broken")

    status = 0
    if broke:
        status = 1
        print("\nDocumented VPL programs that do not check:\n", file=sys.stderr)
        for where, msg in broke:
            print(f"  {where}\n      {msg}", file=sys.stderr)
        print(
            "\nA ```vpl block that declares a stream, pattern or event and has "
            "no `...`\nreads as a program a reader can run. Fix it, mark what it "
            "leaves out with `...`,\nor add it to KNOWN_BROKEN in this script "
            "with the reason it cannot be fixed.",
            file=sys.stderr,
        )
    if fixed or stale:
        status = 1
        if fixed:
            print("\nThese are in KNOWN_BROKEN but now check clean:\n",
                  file=sys.stderr)
            for where in fixed:
                print(f"  {where}", file=sys.stderr)
        if stale:
            print("\nThese are in KNOWN_BROKEN but no checked program is there "
                  "any more\n(moved, deleted, or now a sketch):\n",
                  file=sys.stderr)
            for where in stale:
                print(f"  {where}", file=sys.stderr)
        print(
            "\nRemove them from KNOWN_BROKEN. The list is allowed to shrink "
            "and nothing else,\nso that a fix cannot leave a stale exemption "
            "standing.",
            file=sys.stderr,
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
