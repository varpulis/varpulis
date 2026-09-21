#!/usr/bin/env python3
"""varpulis-engine must stay free of any async runtime, broker client or server stack.

The crate exists so a host with its own threads and its own bus can embed the
engine (Vejas ADR-0031). A transitive dependency on tokio, a NATS or Kafka
client, or an HTTP stack would compile without a word and make the crate
unusable for that host — so this fails the build the moment one appears in
`cargo tree`.

    python3 scripts/check-engine-deps.py               # gate varpulis-engine
    python3 scripts/check-engine-deps.py --package X   # gate another package
                                                       # (demonstration: X=varpulis-runtime fails)
"""
from __future__ import annotations

import argparse
import subprocess
import sys

FORBIDDEN = {
    # async runtimes and their reactor
    "tokio", "tokio-util", "mio", "async-std", "smol",
    # broker clients
    "async-nats", "nats", "rdkafka", "rdkafka-sys", "rumqttc", "pulsar", "redis",
    # HTTP / server stacks
    "reqwest", "hyper", "axum", "warp", "tower", "h2",
    # columnar engine, only meaningful behind the async dispatch path
    "arrow", "arrow-array", "parquet",
}


def tree(package: str) -> list[str]:
    out = subprocess.run(
        ["cargo", "tree", "-p", package, "-e", "normal", "--prefix", "none"],
        check=True, capture_output=True, text=True,
    ).stdout
    names = set()
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        names.add(line.split(" ", 1)[0])
    return sorted(names)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--package", default="varpulis-engine")
    args = ap.parse_args()

    names = tree(args.package)
    hits = sorted(n for n in names if n in FORBIDDEN)
    print(f"{args.package}: {len(names)} packages in the normal dependency tree")
    if not hits:
        print("no async runtime, broker client or server stack: ok")
        return 0
    print(f"\nFORBIDDEN in {args.package}'s dependency tree: {', '.join(hits)}", file=sys.stderr)
    for hit in hits[:3]:
        chain = subprocess.run(
            ["cargo", "tree", "-p", args.package, "-e", "normal", "-i", hit],
            capture_output=True, text=True,
        ).stdout
        print(f"\nwho pulls {hit} in:\n{chain.strip()}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
