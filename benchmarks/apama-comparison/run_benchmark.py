#!/usr/bin/env python3
"""
Benchmark comparison: Apama vs Varpulis

Runs the same workload on both engines and compares performance.
"""

import subprocess
import time
import socket
import json
import os
import sys
import signal
from pathlib import Path

BENCHMARK_DIR = Path(__file__).parent
APAMA_DIR = BENCHMARK_DIR / "apama"
VARPULIS_DIR = BENCHMARK_DIR / "varpulis"
PROJECT_ROOT = BENCHMARK_DIR.parent.parent

NUM_EVENTS = 10000
APAMA_PORT = 15903

def generate_events(count: int) -> list:
    """Generate test events"""
    events = []
    for i in range(count):
        # Alternate between A and B for sequence matching
        symbol = "A" if i % 2 == 0 else "B"
        price = 50.0 + (i % 100)  # Prices 50-149
        volume = 1000 + (i % 500)
        events.append({
            "symbol": symbol,
            "price": price,
            "volume": volume
        })
    return events


def run_apama_benchmark(events: list) -> dict:
    """Run benchmark on Apama correlator"""
    print("\n=== Running Apama Benchmark ===")

    # Start correlator in container
    container_name = "apama-benchmark"

    # Stop any existing container
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True
    )

    # Copy monitor file to temp location
    monitor_file = APAMA_DIR / "benchmark.mon"

    # Start correlator
    print("Starting Apama correlator...")
    proc = subprocess.Popen(
        [
            "docker", "run", "--rm", "--name", container_name,
            "-p", f"{APAMA_PORT}:{APAMA_PORT}",
            "-v", f"{monitor_file}:/app/benchmark.mon:ro",
            "public.ecr.aws/apama/apama-correlator:latest",
            "correlator", "-p", str(APAMA_PORT), "--loglevel", "INFO"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # Wait for correlator to start
    time.sleep(3)

    try:
        # Inject the monitor
        print("Injecting monitor...")
        inject_result = subprocess.run(
            [
                "docker", "exec", container_name,
                "engine_inject", "-p", str(APAMA_PORT), "/app/benchmark.mon"
            ],
            capture_output=True,
            text=True
        )
        if inject_result.returncode != 0:
            print(f"Inject failed: {inject_result.stderr}")
            return {"error": inject_result.stderr}

        time.sleep(1)

        # Send benchmark start event
        print(f"Processing {len(events)} events...")
        start_time = time.perf_counter()

        # Send start event
        subprocess.run(
            [
                "docker", "exec", container_name,
                "engine_send", "-p", str(APAMA_PORT), "-c", "default",
                "BenchmarkStart()"
            ],
            capture_output=True
        )

        # Send events in batches using engine_send
        batch_size = 100
        for i in range(0, len(events), batch_size):
            batch = events[i:i+batch_size]
            event_strings = [
                f'Tick("{e["symbol"]}", {e["price"]}, {e["volume"]})'
                for e in batch
            ]
            # Send batch
            for evt_str in event_strings:
                subprocess.run(
                    [
                        "docker", "exec", container_name,
                        "engine_send", "-p", str(APAMA_PORT), "-c", "default",
                        evt_str
                    ],
                    capture_output=True
                )

        # Send end event
        subprocess.run(
            [
                "docker", "exec", container_name,
                "engine_send", "-p", str(APAMA_PORT), "-c", "default",
                f"BenchmarkEnd({len(events)}, 0.0)"
            ],
            capture_output=True
        )

        elapsed = time.perf_counter() - start_time

        # Get logs
        time.sleep(1)

        return {
            "events": len(events),
            "elapsed_ms": elapsed * 1000,
            "throughput": len(events) / elapsed,
            "latency_us": (elapsed * 1_000_000) / len(events)
        }

    finally:
        # Stop container
        subprocess.run(["docker", "stop", container_name], capture_output=True)


def run_varpulis_benchmark(events: list) -> dict:
    """Run benchmark on Varpulis"""
    print("\n=== Running Varpulis Benchmark ===")

    # Use inline Rust code via cargo test or direct binary
    varpulis_bin = PROJECT_ROOT / "target" / "release" / "varpulis"

    if not varpulis_bin.exists():
        print("Building Varpulis...")
        subprocess.run(
            ["cargo", "build", "--release", "-p", "varpulis-cli"],
            cwd=PROJECT_ROOT,
            capture_output=True
        )

    # Create a test VPL file
    vpl_content = '''
event Tick:
    symbol: str
    price: float
    volume: int

stream FilterBench = Tick
    .where(price > 50.0)
    .emit(event_type: "Filtered", symbol: symbol, price: price)

stream SequenceBench = Tick as a
    -> Tick as b
    .where(a.symbol == "A" and b.symbol == "B")
    .within(1s)
    .emit(event_type: "Sequence", a_price: a.price, b_price: b.price)
'''

    vpl_file = VARPULIS_DIR / "benchmark.vpl"
    vpl_file.write_text(vpl_content)

    # For accurate benchmarking, we'd need to run events through the engine
    # Using a simple timing approach with the check command as proxy
    print(f"Processing {len(events)} events...")

    # Create event file
    event_file = BENCHMARK_DIR / "data" / "benchmark_events.jsonl"
    event_file.parent.mkdir(exist_ok=True)

    with open(event_file, "w") as f:
        for e in events:
            evt = {
                "event_type": "Tick",
                "data": e
            }
            f.write(json.dumps(evt) + "\n")

    # Run varpulis with event file
    start_time = time.perf_counter()

    result = subprocess.run(
        [str(varpulis_bin), "run", str(vpl_file), "--events", str(event_file)],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT
    )

    elapsed = time.perf_counter() - start_time

    return {
        "events": len(events),
        "elapsed_ms": elapsed * 1000,
        "throughput": len(events) / elapsed,
        "latency_us": (elapsed * 1_000_000) / len(events)
    }


def main():
    print("=" * 60)
    print("Apama vs Varpulis Benchmark")
    print("=" * 60)

    # Generate events
    print(f"\nGenerating {NUM_EVENTS} test events...")
    events = generate_events(NUM_EVENTS)

    # Run Apama benchmark
    try:
        apama_results = run_apama_benchmark(events)
    except Exception as e:
        apama_results = {"error": str(e)}

    # Run Varpulis benchmark
    try:
        varpulis_results = run_varpulis_benchmark(events)
    except Exception as e:
        varpulis_results = {"error": str(e)}

    # Print comparison
    print("\n" + "=" * 60)
    print("RESULTS COMPARISON")
    print("=" * 60)

    print(f"\nEvents: {NUM_EVENTS}")
    print()

    print(f"{'Metric':<25} {'Apama':<20} {'Varpulis':<20}")
    print("-" * 65)

    if "error" not in apama_results and "error" not in varpulis_results:
        print(f"{'Elapsed (ms)':<25} {apama_results['elapsed_ms']:<20.2f} {varpulis_results['elapsed_ms']:<20.2f}")
        print(f"{'Throughput (evt/s)':<25} {apama_results['throughput']:<20.0f} {varpulis_results['throughput']:<20.0f}")
        print(f"{'Latency (µs/evt)':<25} {apama_results['latency_us']:<20.2f} {varpulis_results['latency_us']:<20.2f}")

        # Speedup
        if apama_results['elapsed_ms'] > 0:
            speedup = apama_results['elapsed_ms'] / varpulis_results['elapsed_ms']
            print()
            if speedup > 1:
                print(f"Varpulis is {speedup:.2f}x faster than Apama")
            else:
                print(f"Apama is {1/speedup:.2f}x faster than Varpulis")
    else:
        if "error" in apama_results:
            print(f"Apama error: {apama_results['error']}")
        if "error" in varpulis_results:
            print(f"Varpulis error: {varpulis_results['error']}")


if __name__ == "__main__":
    main()
