#!/usr/bin/env python3
"""Check the VPL in the documentation against the engine in this commit.

Every ```vpl block in docs/ and README.md is extracted. A block that declares
at least one event type is treated as a complete program and must pass
`varpulis check`; a block without one is a fragment — an operator signature on
a reference page, a clause shown in isolation — and is not checked, because
there is no honest way to check half a program.

That split is the whole policy. It is narrow on purpose: it catches the case
that actually costs a reader something, which is a documented example that
looks runnable and is not.

Usage: check-doc-vpl.py <path to the varpulis binary>
"""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Complete programs that do NOT check, each kept here with the reason.
#
# The list can only shrink: a block named here that starts passing fails this
# script, so a fix cannot leave a stale exemption behind. It went from eight
# entries to one — five webMethods blocks needed absence-with-timeout, which
# now works, and two comparison blocks used `left_join` as an operator when it
# is a stream source.
KNOWN_BROKEN = {
    "docs/siem-evasion-lab-05-OUTLINE.md:89":
        "the document marks this block as an unvalidated sketch in its own "
        "prose — 'the example above is illustrative, not yet syntactically "
        "validated' — and asks the drafter to check the Kleene previous-event "
        "reference against varpulis-sase before publishing. It is an outline "
        "for an unwritten article, so the pedagogy is the drafter's call, not "
        "a syntax fix.",
}


def blocks():
    """Yield (source location, body) for every ```vpl block."""
    files = sorted(ROOT.glob("docs/**/*.md")) + [ROOT / "README.md"]
    for md in files:
        if not md.exists():
            continue
        text = md.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(r"^```vpl\n(.*?)^```", text, re.S | re.M):
            line = text[: m.start()].count("\n") + 1
            yield f"{md.relative_to(ROOT)}:{line}", m.group(1)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <varpulis binary>", file=sys.stderr)
        return 2
    binary = sys.argv[1]

    tmp = ROOT / "target" / "doc-vpl"
    tmp.mkdir(parents=True, exist_ok=True)

    checked = skipped = 0
    broke: list[tuple[str, str]] = []
    fixed: list[str] = []

    for where, body in blocks():
        if not re.search(r"^event ", body, re.M):
            skipped += 1
            continue
        checked += 1
        path = tmp / (where.replace("/", "__").replace(":", "_L") + ".vpl")
        path.write_text(body)
        run = subprocess.run(
            [binary, "check", str(path)], capture_output=True, text=True
        )
        if run.returncode == 0:
            if where in KNOWN_BROKEN:
                fixed.append(where)
        elif where not in KNOWN_BROKEN:
            first = next(
                (l.strip() for l in (run.stdout + run.stderr).splitlines()
                 if "×" in l),
                "(no diagnostic)",
            )
            broke.append((where, first))

    print(f"{checked} complete programs checked, {skipped} fragments skipped, "
          f"{len(KNOWN_BROKEN)} known-broken")

    status = 0
    if broke:
        status = 1
        print("\nDocumented VPL programs that do not check:\n", file=sys.stderr)
        for where, msg in broke:
            print(f"  {where}\n      {msg}", file=sys.stderr)
        print(
            "\nA ```vpl block that declares an event type reads as a runnable "
            "program.\nFix it, or add it to KNOWN_BROKEN in this script with "
            "the reason it cannot be.",
            file=sys.stderr,
        )
    if fixed:
        status = 1
        print("\nThese are in KNOWN_BROKEN but now check clean:\n",
              file=sys.stderr)
        for where in fixed:
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
