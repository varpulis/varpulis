#!/usr/bin/env python3
"""
SASE+ Sequence Pattern Benchmark: Varpulis vs Apama

Tests A -> B sequence pattern matching:
- Correctness: All A/B pairs should produce a Match
- Throughput: Events per second
"""

import subprocess
import time
import json
import os
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent.parent

def generate_events(count: int) -> tuple[Path, Path]:
    """Generate events in both Varpulis and Apama formats."""
    vpl_file = SCRIPT_DIR / "data" / "sequence_events.evt"
    apama_file = SCRIPT_DIR / "data" / "sequence_events_apama.evt"

    (SCRIPT_DIR / "data").mkdir(exist_ok=True)

    with open(vpl_file, "w") as f_vpl, open(apama_file, "w") as f_apama:
        for i in range(1, count + 1):
            # Varpulis format
            f_vpl.write(f'A {{ id: {i} }}\n')
            f_vpl.write(f'B {{ id: {i} }}\n')
            # Apama format
            f_apama.write(f'A({i})\n')
            f_apama.write(f'B({i})\n')

    return vpl_file, apama_file

def run_varpulis(event_file: Path, event_count: int) -> dict:
    """Run Varpulis sequence benchmark."""
    vpl_file = SCRIPT_DIR / "varpulis.vpl"
    varpulis_bin = PROJECT_ROOT / "target" / "release" / "varpulis"

    start = time.perf_counter()
    result = subprocess.run(
        [str(varpulis_bin), "simulate",
         "--program", str(vpl_file),
         "--events", str(event_file),
         "--immediate"],
        capture_output=True,
        text=True,
        timeout=120
    )
    elapsed = time.perf_counter() - start

    # Count matches from output
    output_count = result.stdout.count('"Match"')

    return {
        "engine": "Varpulis",
        "events": event_count * 2,  # A + B events
        "elapsed_ms": elapsed * 1000,
        "throughput": (event_count * 2) / elapsed if elapsed > 0 else 0,
        "matches": output_count,
        "expected_matches": event_count,
        "correct": output_count == event_count
    }

def run_apama(event_file: Path, event_count: int) -> dict:
    """Run Apama sequence benchmark."""
    # Check if Apama is running
    result = subprocess.run(
        ["docker", "ps", "-q", "-f", "name=apama-bench"],
        capture_output=True,
        text=True
    )
    if not result.stdout.strip():
        return {
            "engine": "Apama",
            "events": 0,
            "elapsed_ms": 0,
            "throughput": 0,
            "matches": 0,
            "error": "Apama container not running"
        }

    # Inject the monitor
    mon_file = SCRIPT_DIR / "apama.mon"
    subprocess.run(
        ["docker", "exec", "apama-bench", "engine_inject", "-p", "15903", f"/app/{mon_file.name}"],
        capture_output=True
    )
    time.sleep(0.5)

    # Send events and measure time
    # Using engine_send is slow, so we'll use a socket-based approach
    import socket

    start = time.perf_counter()

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 15903))
        sock.settimeout(30)

        # Read events and send them
        with open(event_file) as f:
            batch = []
            for line in f:
                batch.append(line.strip())
                if len(batch) >= 100:
                    msg = "&FLUSHING(true)\n" + "\n".join(batch) + "\n"
                    sock.send(msg.encode())
                    batch = []
            if batch:
                msg = "&FLUSHING(true)\n" + "\n".join(batch) + "\n"
                sock.send(msg.encode())

        sock.close()
        elapsed = time.perf_counter() - start

        # Get stats from correlator
        time.sleep(0.5)
        result = subprocess.run(
            ["docker", "exec", "apama-bench", "engine_inspect", "-p", "15903", "-r"],
            capture_output=True,
            text=True
        )

        return {
            "engine": "Apama",
            "events": event_count * 2,
            "elapsed_ms": elapsed * 1000,
            "throughput": (event_count * 2) / elapsed if elapsed > 0 else 0,
            "matches": "N/A (check correlator)",
            "expected_matches": event_count,
            "correct": None
        }
    except Exception as e:
        return {
            "engine": "Apama",
            "events": 0,
            "elapsed_ms": 0,
            "throughput": 0,
            "matches": 0,
            "error": str(e)
        }

def main():
    import argparse
    parser = argparse.ArgumentParser(description="SASE+ Sequence Benchmark")
    parser.add_argument("--pairs", "-n", type=int, default=10000,
                       help="Number of A/B event pairs")
    parser.add_argument("--engine", "-e", choices=["varpulis", "apama", "both"],
                       default="both", help="Engine to benchmark")
    args = parser.parse_args()

    print("=" * 60)
    print("SASE+ Sequence Pattern Benchmark")
    print(f"Pattern: A as a -> B as b .where(a.id == b.id)")
    print(f"Event pairs: {args.pairs}")
    print("=" * 60)

    # Generate events
    print("\nGenerating events...")
    vpl_file, apama_file = generate_events(args.pairs)
    print(f"  Generated {args.pairs * 2} events ({args.pairs} pairs)")

    results = []

    # Run Varpulis
    if args.engine in ["varpulis", "both"]:
        print("\n>>> Running Varpulis SASE+...")
        result = run_varpulis(vpl_file, args.pairs)
        results.append(result)
        print(f"    Throughput: {result['throughput']:.0f} events/sec")
        print(f"    Matches: {result['matches']} (expected: {result['expected_matches']})")
        print(f"    Correct: {'YES' if result['correct'] else 'NO'}")

    # Run Apama
    if args.engine in ["apama", "both"]:
        print("\n>>> Running Apama...")
        result = run_apama(apama_file, args.pairs)
        results.append(result)
        if "error" in result:
            print(f"    Error: {result['error']}")
        else:
            print(f"    Throughput: {result['throughput']:.0f} events/sec")

    # Summary
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print(f"\n{'Engine':<15} {'Events':<12} {'Time (ms)':<12} {'Throughput':<15} {'Matches':<10}")
    print("-" * 64)
    for r in results:
        if "error" in r:
            print(f"{r['engine']:<15} ERROR: {r['error']}")
        else:
            print(f"{r['engine']:<15} {r['events']:<12} {r['elapsed_ms']:<12.1f} {r['throughput']:<15.0f} {r['matches']:<10}")

    # Compare
    if len(results) == 2 and "error" not in results[0] and "error" not in results[1]:
        v_thru = results[0]['throughput']
        a_thru = results[1]['throughput']
        if a_thru > 0:
            ratio = v_thru / a_thru
            winner = "Varpulis" if ratio > 1 else "Apama"
            print(f"\n{winner} is {max(ratio, 1/ratio):.1f}x faster")

if __name__ == "__main__":
    main()
