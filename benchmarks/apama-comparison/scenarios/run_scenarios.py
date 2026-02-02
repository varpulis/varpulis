#!/usr/bin/env python3
"""
Benchmark Runner for Apama vs Varpulis Scenarios

Runs each scenario with the same event workload and compares:
- Parse/compile time
- Throughput (events/second)
- Latency (microseconds/event)
- Output count (correctness check)
"""

import subprocess
import time
import json
import os
import sys
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, List

SCENARIOS_DIR = Path(__file__).parent
PROJECT_ROOT = SCENARIOS_DIR.parent.parent.parent
DATA_DIR = SCENARIOS_DIR.parent / "data"


@dataclass
class BenchmarkResult:
    scenario: str
    engine: str
    events: int
    elapsed_ms: float
    throughput: float
    output_count: int
    error: Optional[str] = None


def generate_events(scenario: str, count: int) -> List[Dict]:
    """Generate events appropriate for each scenario."""
    events = []

    if scenario == "01_filter":
        for i in range(count):
            events.append({
                "event_type": "StockTick",
                "data": {
                    "symbol": ["AAPL", "GOOG", "MSFT"][i % 3],
                    "price": 40.0 + (i % 100),  # 40-139, some > 50
                    "volume": 1000 + (i % 5000)
                }
            })

    elif scenario == "02_aggregation":
        for i in range(count):
            events.append({
                "event_type": "Trade",
                "data": {
                    "symbol": "ACME",
                    "price": 100.0 + (i % 50) * 0.1,
                    "volume": 100.0 + (i % 900)
                }
            })

    elif scenario == "03_temporal":
        for i in range(count):
            if i % 3 == 0:
                events.append({
                    "event_type": "Login",
                    "data": {
                        "user_id": f"user_{i % 100}",
                        "ip": f"192.168.1.{i % 255}",
                        "device": "mobile"
                    }
                })
            else:
                events.append({
                    "event_type": "Transaction",
                    "data": {
                        "user_id": f"user_{(i-1) % 100}",
                        "amount": 1000.0 + (i % 10000),
                        "ip": f"192.168.1.{(i+50) % 255}",  # Different IP
                        "merchant": "Store"
                    }
                })

    elif scenario == "04_kleene":
        for i in range(count):
            # Create rising sequences
            base_price = 100.0 + (i // 10) * 10
            events.append({
                "event_type": "StockTick",
                "data": {
                    "symbol": ["AAPL", "GOOG"][i % 2],
                    "price": base_price + (i % 10),  # Rising within groups
                    "volume": 1000
                }
            })

    elif scenario == "05_ema_crossover":
        import math
        for i in range(count):
            # Simulate price with trends
            trend = math.sin(i / 100) * 10
            events.append({
                "event_type": "StockTick",
                "data": {
                    "symbol": "AAPL",
                    "price": 100.0 + trend + (i % 5) * 0.1,
                    "volume": 1000
                }
            })

    elif scenario == "06_multi_sensor":
        for i in range(count):
            if i % 2 == 0:
                events.append({
                    "event_type": "TemperatureReading",
                    "data": {
                        "sensor_id": f"temp_{i % 10}",
                        "location": f"zone_{i % 5}",
                        "value": 20.0 + (i % 30) + (5.0 if i % 50 == 0 else 0)
                    }
                })
            else:
                events.append({
                    "event_type": "PressureReading",
                    "data": {
                        "sensor_id": f"press_{i % 10}",
                        "location": f"zone_{i % 5}",
                        "value": 1000.0 + (i % 100) + (20.0 if i % 50 == 0 else 0)
                    }
                })
    else:
        # Generic events
        for i in range(count):
            events.append({
                "event_type": "Event",
                "data": {"id": i, "value": i * 10}
            })

    return events


def run_varpulis(scenario: str, events: List[Dict]) -> BenchmarkResult:
    """Run a scenario with Varpulis."""
    vpl_file = SCENARIOS_DIR / scenario / "varpulis.vpl"
    if not vpl_file.exists():
        return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0,
                              f"VPL file not found: {vpl_file}")

    # Write events to temp file
    event_file = DATA_DIR / f"{scenario}_events.jsonl"
    event_file.parent.mkdir(exist_ok=True)
    with open(event_file, "w") as f:
        for evt in events:
            f.write(json.dumps(evt) + "\n")

    # Build varpulis if needed
    varpulis_bin = PROJECT_ROOT / "target" / "release" / "varpulis"
    if not varpulis_bin.exists():
        print("  Building Varpulis...")
        subprocess.run(
            ["cargo", "build", "--release", "-p", "varpulis-cli"],
            cwd=PROJECT_ROOT,
            capture_output=True
        )

    # Run benchmark
    start = time.perf_counter()
    try:
        result = subprocess.run(
            [str(varpulis_bin), "run", str(vpl_file), "--events", str(event_file)],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=PROJECT_ROOT
        )
        elapsed = time.perf_counter() - start

        # Count output (rough estimate from stdout)
        output_count = result.stdout.count("event_type")

        return BenchmarkResult(
            scenario, "varpulis", len(events),
            elapsed * 1000,
            len(events) / elapsed if elapsed > 0 else 0,
            output_count
        )
    except subprocess.TimeoutExpired:
        return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0, "Timeout")
    except Exception as e:
        return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0, str(e))


def print_results(results: List[BenchmarkResult]):
    """Print benchmark results in a table."""
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS")
    print("=" * 80)

    print(f"\n{'Scenario':<20} {'Engine':<10} {'Events':<10} {'Time (ms)':<12} {'Throughput':<15} {'Outputs':<10}")
    print("-" * 80)

    for r in results:
        if r.error:
            print(f"{r.scenario:<20} {r.engine:<10} ERROR: {r.error}")
        else:
            print(f"{r.scenario:<20} {r.engine:<10} {r.events:<10} {r.elapsed_ms:<12.2f} {r.throughput:<15.0f} {r.output_count:<10}")


def main():
    parser = argparse.ArgumentParser(description="Run Apama vs Varpulis benchmarks")
    parser.add_argument("--scenario", "-s", help="Run specific scenario (e.g., 01_filter)")
    parser.add_argument("--events", "-n", type=int, default=10000, help="Number of events")
    parser.add_argument("--engine", "-e", choices=["varpulis", "apama", "both"], default="varpulis",
                       help="Which engine to benchmark")
    args = parser.parse_args()

    # Find scenarios
    if args.scenario:
        scenarios = [args.scenario]
    else:
        scenarios = sorted([
            d.name for d in SCENARIOS_DIR.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])

    print(f"Running {len(scenarios)} scenarios with {args.events} events each")
    print(f"Engine: {args.engine}")

    results = []

    for scenario in scenarios:
        print(f"\n>>> Scenario: {scenario}")

        # Generate events
        events = generate_events(scenario, args.events)

        if args.engine in ["varpulis", "both"]:
            print("  Running Varpulis...")
            result = run_varpulis(scenario, events)
            results.append(result)

        # TODO: Add Apama runner
        # if args.engine in ["apama", "both"]:
        #     print("  Running Apama...")
        #     result = run_apama(scenario, events)
        #     results.append(result)

    print_results(results)

    # Save results
    results_file = SCENARIOS_DIR.parent / "results" / f"scenario_benchmark_{int(time.time())}.json"
    results_file.parent.mkdir(exist_ok=True)
    with open(results_file, "w") as f:
        json.dump([{
            "scenario": r.scenario,
            "engine": r.engine,
            "events": r.events,
            "elapsed_ms": r.elapsed_ms,
            "throughput": r.throughput,
            "output_count": r.output_count,
            "error": r.error
        } for r in results], f, indent=2)
    print(f"\nResults saved to: {results_file}")


if __name__ == "__main__":
    main()
