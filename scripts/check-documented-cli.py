#!/usr/bin/env python3
"""Every `varpulis ...` command line in the documentation must parse.

`docs/examples/hvac-building.md` documents

    varpulis run examples/hvac_demo.vpl --source kafka://... --output kafka://...

and the binary answers `error: unexpected argument 'examples/hvac_demo.vpl'`.
`run` takes `-f/--file`, and neither `--source` nor `--output` exists. Other
pages in the same tree use the correct form, so a reader can pick either and
only one works.

This extracts `varpulis <subcommand> ...` invocations from fenced shell blocks
and checks each subcommand's flags and positionals against what
`varpulis <subcommand> --help` accepts. It does not execute anything.

Usage: scripts/check-documented-cli.py <path to the varpulis binary>
"""

import pathlib
import re
import shlex
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
# Placeholders a reader is meant to substitute; not our problem if they are odd.
PLACEHOLDER = re.compile(r"^[<${]|[>}]$")


def subcommands(binary: str) -> set[str]:
    out = subprocess.run([binary, "--help"], capture_output=True, text=True).stdout
    body = out.split("Commands:", 1)[-1].split("Options:", 1)[0]
    return set(re.findall(r"^\s{2,}([a-z][a-z0-9-]*)\s{2,}", body, re.M))


def accepts(binary: str, path: list[str]) -> tuple[set[str], bool, set[str]]:
    """Long flags, whether a positional is taken, and any nested subcommands.

    `varpulis connector test --url ...` is valid and `varpulis connector --url`
    is not, so the help of the leaf command is the only one that answers the
    question. Reading only the first level reported `connector has no --url`,
    which was wrong.
    """
    out = subprocess.run([binary, *path, "--help"], capture_output=True, text=True).stdout
    flags = set(re.findall(r"(?<![\w-])--([a-z][a-z0-9-]*)", out))
    usage = re.search(r"^Usage:.*$", out, re.M)
    positional = bool(usage and re.search(r"\s[<\[][A-Z_]+[>\]]", usage.group(0)))
    nested = set()
    if "Commands:" in out:
        body = out.split("Commands:", 1)[1].split("Options:", 1)[0]
        nested = {c for c in re.findall(r"^\s{2,}([a-z][a-z0-9-]*)\s{2,}", body, re.M)
                  if c != "help"}
    return flags, positional, nested


def invocations():
    """(file, line, argv) for every `varpulis ...` line in a fenced block."""
    for md in sorted((ROOT / "docs").rglob("*.md")) + [ROOT / "README.md"]:
        if not md.is_file() or ".vitepress" in md.parts or "node_modules" in md.parts:
            continue
        text = md.read_text(errors="replace")
        # Match EVERY fence and filter by language, rather than only the shell
        # ones. Matching `^```(?:bash|sh)?\n` skips a ```vpl opener, so its
        # closing fence becomes the next opener and every block after it pairs
        # up shifted by one — which silently hid the `varpulis run` line this
        # script was written to catch.
        for block in re.finditer(r"^```(\w*)[^\n]*\n(.*?)^```", text, re.S | re.M):
            if block.group(1).lower() not in ("", "bash", "sh", "shell", "console"):
                continue
            start = text[: block.start()].count("\n") + 1
            for offset, line in enumerate(block.group(2).split("\n")):
                stripped = line.strip().lstrip("$ ").strip()
                # One command per line only; a pipeline or `&&` chain is the
                # reader's problem, not a claim about our argument parser.
                if not re.match(r"^(?:\w+=\S+\s+)*(?:\./)?varpulis\s", stripped):
                    continue
                if any(c in stripped for c in "|&;`"):
                    continue
                stripped = re.sub(r"^(?:\w+=\S+\s+)*", "", stripped).rstrip("\\").strip()
                try:
                    argv = shlex.split(stripped)
                except ValueError:
                    continue
                yield md.relative_to(ROOT), start + offset + 1, argv[1:]


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <varpulis binary>", file=sys.stderr)
        return 2
    binary = sys.argv[1]
    subs = subcommands(binary)
    cache: dict[str, tuple[set[str], bool]] = {}

    problems, checked = [], 0
    for where, line, argv in invocations():
        if not argv or argv[0].startswith("-"):
            continue
        sub = argv[0]
        if sub not in subs:
            problems.append((where, line, f"no such subcommand: {sub}"))
            continue
        # Walk into nested subcommands so the leaf's help is what is consulted.
        path = [sub]
        rest = argv[1:]
        while True:
            key = " ".join(path)
            if key not in cache:
                cache[key] = accepts(binary, path)
            flags, positional, nested = cache[key]
            if rest and rest[0] in nested:
                path.append(rest[0])
                rest = rest[1:]
                continue
            break
        checked += 1
        i = 0
        while i < len(rest):
            tok = rest[i]
            if tok == "--":
                break
            if tok.startswith("--"):
                name = tok[2:].split("=", 1)[0]
                if name and name not in flags:
                    problems.append((where, line, f"{' '.join(path)} has no --{name}"))
            elif not tok.startswith("-") and not positional:
                if not PLACEHOLDER.match(tok) and (i == 0 or not rest[i - 1].startswith("-")):
                    problems.append(
                        (
                            where,
                            line,
                            f"{' '.join(path)} takes no positional argument, got {tok!r}",
                        )
                    )
            i += 1

    print(f"{checked} documented `varpulis` invocations checked against the binary")
    if problems:
        print("\nDocumented command lines the binary rejects:\n", file=sys.stderr)
        for where, line, msg in problems:
            print(f"  {where}:{line}\n      {msg}", file=sys.stderr)
        print(
            "\nA reader copying these gets an argument error. Fix the "
            "documentation,\nor add the flag.",
            file=sys.stderr,
        )
        return 1
    print("every documented command line parses")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
