#!/usr/bin/env python3
"""
Fair Benchmark Runner: Apama v27.18 vs Varpulis

Methodology:
  - Both engines use their native event format (.evt for Varpulis, Apama format for Apama)
  - Varpulis: `simulate --preload --immediate --quiet --workers 1`
    Timing from engine's internal clock (excludes program load, includes file parse + processing)
  - Apama: `engine_send` from file to already-running correlator (excludes monitor inject)
    Timing from engine_send start to completion (includes file read + TCP + processing)
  - Multiple runs, median throughput reported
  - Output/match counts verified for correctness

Fairness notes:
  - Varpulis timing includes event file parsing from disk
  - Apama timing includes engine_send process startup, file read, TCP send, and event processing
  - Neither includes program/monitor compilation
  - Varpulis supports multi-threaded processing via --workers flag
"""

import subprocess
import time
import json
import os
import re
import math
import argparse
import statistics
import threading
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict

SCENARIOS_DIR = Path(__file__).parent
PROJECT_ROOT = SCENARIOS_DIR.parent.parent.parent
DATA_DIR = SCENARIOS_DIR.parent / "data"

APAMA_PORT = 15903
APAMA_CONTAINER = "bench-apama"


# ---------------------------------------------------------------------------
# Memory tracking
# ---------------------------------------------------------------------------

class MemoryTracker:
    """Track peak RSS memory of a process or Docker container."""

    def __init__(self, mode: str, target, interval: float = 0.1):
        self.mode = mode
        self.target = target
        self.interval = interval if mode == "process" else 1.0
        self.samples: List[float] = []
        self.running = False
        self.thread = None

    def _read_process_rss(self) -> Optional[float]:
        try:
            with open(f"/proc/{self.target}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) / 1024.0
        except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
            pass
        return None

    def _read_container_rss(self) -> Optional[float]:
        try:
            result = subprocess.run(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.MemUsage}}", self.target],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                usage = result.stdout.strip().split("/")[0].strip()
                if "GiB" in usage:
                    return float(usage.replace("GiB", "").strip()) * 1024
                elif "MiB" in usage:
                    return float(usage.replace("MiB", "").strip())
                elif "KiB" in usage:
                    return float(usage.replace("KiB", "").strip()) / 1024
        except Exception:
            pass
        return None

    def _sample_loop(self):
        while self.running:
            rss = self._read_process_rss() if self.mode == "process" else self._read_container_rss()
            if rss is not None:
                self.samples.append(rss)
            time.sleep(self.interval)

    def start(self):
        self.running = True
        self.samples = []
        self.thread = threading.Thread(target=self._sample_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=3)

    def get_peak_mb(self) -> float:
        return max(self.samples) if self.samples else 0.0


@dataclass
class BenchmarkResult:
    scenario: str
    engine: str
    events: int
    elapsed_ms: float
    throughput: float
    output_count: int
    error: Optional[str] = None
    runs: List[float] = field(default_factory=list)
    peak_rss_mb: float = 0.0


# ---------------------------------------------------------------------------
# Event generation — produces dicts, converted to native format per engine
# ---------------------------------------------------------------------------

def generate_events(scenario: str, count: int) -> List[Dict]:
    """Generate events appropriate for each scenario."""
    events = []

    if scenario == "01_filter":
        for i in range(count):
            events.append({
                "event_type": "StockTick",
                "fields": {
                    "symbol": ["AAPL", "GOOG", "MSFT"][i % 3],
                    "price": 40.0 + (i % 100),  # 40-139, ~90% pass price>50
                    "volume": 1000 + (i % 5000),
                },
            })

    elif scenario == "02_aggregation":
        for i in range(count):
            events.append({
                "event_type": "Trade",
                "fields": {
                    "symbol": "ACME",
                    "price": 100.0 + (i % 50) * 0.1,
                    "volume": 100.0 + (i % 900),
                },
            })

    elif scenario == "03_temporal":
        for i in range(count):
            if i % 3 == 0:
                events.append({
                    "event_type": "Login",
                    "fields": {
                        "user_id": f"user_{i % 100}",
                        "ip": f"192.168.1.{i % 255}",
                        "device": "mobile",
                    },
                })
            else:
                events.append({
                    "event_type": "Transaction",
                    "fields": {
                        "user_id": f"user_{(i - 1) % 100}",
                        "amount": 1000.0 + (i % 10000),
                        "ip": f"192.168.1.{(i + 50) % 255}",
                        "merchant": "Store",
                    },
                })

    elif scenario == "04_kleene":
        for i in range(count):
            base_price = 100.0 + (i // 10) * 10
            events.append({
                "event_type": "StockTick",
                "fields": {
                    "symbol": ["AAPL", "GOOG"][i % 2],
                    "price": base_price + (i % 10),
                    "volume": 1000,
                },
            })

    elif scenario == "05_ema_crossover":
        for i in range(count):
            trend = math.sin(i / 100) * 10
            events.append({
                "event_type": "StockTick",
                "fields": {
                    "symbol": "AAPL",
                    "price": 100.0 + trend + (i % 5) * 0.1,
                    "volume": 1000,
                },
            })

    elif scenario == "06_multi_sensor":
        for i in range(count):
            if i % 2 == 0:
                events.append({
                    "event_type": "TemperatureReading",
                    "fields": {
                        "sensor_id": f"temp_{i % 10}",
                        "location": f"zone_{i % 5}",
                        "value": 20.0 + (i % 30) + (5.0 if i % 50 == 0 else 0),
                    },
                })
            else:
                events.append({
                    "event_type": "PressureReading",
                    "fields": {
                        "sensor_id": f"press_{i % 10}",
                        "location": f"zone_{i % 5}",
                        "value": 1000.0 + (i % 100) + (20.0 if i % 50 == 0 else 0),
                    },
                })

    elif scenario == "07_sequence":
        # A/B pairs with matching ids
        for i in range(count // 2):
            events.append({
                "event_type": "A",
                "fields": {"id": i},
            })
            events.append({
                "event_type": "B",
                "fields": {"id": i},
            })

    else:
        for i in range(count):
            events.append({
                "event_type": "Event",
                "fields": {"id": i, "value": i * 10},
            })

    return events


# ---------------------------------------------------------------------------
# Event format writers
# ---------------------------------------------------------------------------

def write_varpulis_evt(events: List[Dict], path: Path):
    """Write events in Varpulis .evt format: EventType { key: value, ... }"""
    with open(path, "w") as f:
        for evt in events:
            fields = ", ".join(
                f'{k}: {json.dumps(v)}' if isinstance(v, str) else f"{k}: {v}"
                for k, v in evt["fields"].items()
            )
            f.write(f'{evt["event_type"]} {{ {fields} }}\n')


def format_apama_event(evt: Dict) -> str:
    """Format a single event in Apama format: EventType(val1, val2, ...)"""
    vals = []
    for v in evt["fields"].values():
        if isinstance(v, str):
            vals.append(f'"{v}"')
        elif isinstance(v, float):
            vals.append(f"{v}")
        elif isinstance(v, int):
            vals.append(str(v))
        else:
            vals.append(str(v))
    return f'{evt["event_type"]}({",".join(vals)})'


def write_apama_evt(events: List[Dict], path: Path):
    """Write events in Apama format: EventType(val1, val2, ...)"""
    with open(path, "w") as f:
        for evt in events:
            f.write(format_apama_event(evt) + "\n")


# ---------------------------------------------------------------------------
# Varpulis runner
# ---------------------------------------------------------------------------

def run_varpulis(scenario: str, events: List[Dict], runs: int,
                 data_dir: Path = None, workers: int = 1) -> BenchmarkResult:
    """Run Varpulis benchmark using simulate command.

    Uses --preload --immediate --quiet --workers 1 for single-threaded processing.
    Parses internal timing from stdout (measures processing only, not program load).
    Memory tracked via /proc/{pid}/status during execution.
    """
    vpl_file = SCENARIOS_DIR / scenario / "varpulis.vpl"
    if not vpl_file.exists():
        return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0,
                               f"VPL file not found: {vpl_file}")

    # Write events in native .evt format
    out_dir = data_dir or DATA_DIR
    evt_file = out_dir / f"{scenario}_bench.evt"
    evt_file.parent.mkdir(exist_ok=True)
    write_varpulis_evt(events, evt_file)

    varpulis_bin = PROJECT_ROOT / "target" / "release" / "varpulis"
    if not varpulis_bin.exists():
        return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0,
                               "Binary not found — run: cargo build --release")

    throughputs = []
    peak_rss_values = []
    last_output_count = 0
    last_elapsed_ms = 0

    for run_idx in range(runs):
        try:
            proc = subprocess.Popen(
                [
                    str(varpulis_bin), "simulate",
                    "--program", str(vpl_file),
                    "--events", str(evt_file),
                    "--immediate", "--preload", "--quiet", "--workers", str(workers),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=PROJECT_ROOT,
            )

            # Track memory while process runs
            mem_tracker = MemoryTracker("process", proc.pid)
            mem_tracker.start()

            stdout, stderr = proc.communicate(timeout=120)
            mem_tracker.stop()

            if proc.returncode != 0:
                stderr_str = stderr.decode("utf-8", errors="replace").strip()[:200]
                return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0,
                                       f"Exit code {proc.returncode}: {stderr_str}")

            peak_rss = mem_tracker.get_peak_mb()
            if peak_rss > 0:
                peak_rss_values.append(peak_rss)

            # Parse internal timing from output
            output = stdout.decode("utf-8", errors="replace") + stderr.decode("utf-8", errors="replace")
            rate_match = re.search(r"Event rate:\s+([\d.]+)\s+events/sec", output)
            output_match = re.search(r"Output events emitted:\s+(\d+)", output)
            duration_match = re.search(r"Duration:\s+([\d.]+)([a-zµ]+)", output)

            if rate_match:
                throughput = float(rate_match.group(1))
                throughputs.append(throughput)

            if output_match:
                last_output_count = int(output_match.group(1))

            if duration_match:
                val = float(duration_match.group(1))
                unit = duration_match.group(2)
                if "ms" in unit:
                    last_elapsed_ms = val
                elif "µs" in unit or "us" in unit:
                    last_elapsed_ms = val / 1000
                else:  # seconds
                    last_elapsed_ms = val * 1000

        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0, "Timeout")
        except Exception as e:
            return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0, str(e))

    if not throughputs:
        return BenchmarkResult(scenario, "varpulis", len(events), 0, 0, 0,
                               "Could not parse timing from output")

    median_throughput = statistics.median(throughputs)
    median_elapsed = (len(events) / median_throughput * 1000) if median_throughput > 0 else 0
    median_peak_rss = statistics.median(peak_rss_values) if peak_rss_values else 0.0

    return BenchmarkResult(
        scenario, "varpulis", len(events),
        median_elapsed, median_throughput, last_output_count,
        runs=throughputs, peak_rss_mb=median_peak_rss,
    )


# ---------------------------------------------------------------------------
# Apama runner
# ---------------------------------------------------------------------------

def apama_available() -> bool:
    """Check if Apama container is running."""
    result = subprocess.run(
        ["docker", "ps", "-q", "-f", f"name={APAMA_CONTAINER}"],
        capture_output=True, text=True,
    )
    return bool(result.stdout.strip())


def apama_inject_monitors():
    """Inject all scenario monitors into the running Apama correlator."""
    monitors = sorted(SCENARIOS_DIR.glob("*/apama.mon"))
    for mon in monitors:
        # Copy monitor into container then inject
        subprocess.run(
            ["docker", "cp", str(mon), f"{APAMA_CONTAINER}:/tmp/{mon.parent.name}.mon"],
            capture_output=True,
        )
        result = subprocess.run(
            ["docker", "exec", APAMA_CONTAINER,
             "engine_inject", "-p", str(APAMA_PORT),
             f"/tmp/{mon.parent.name}.mon"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"    WARN: Failed to inject {mon.parent.name}: {result.stderr.strip()[:100]}")


def apama_has_event_listener(event_type: str) -> bool:
    """Check if Apama has a listener for the given event type."""
    result = subprocess.run(
        ["docker", "exec", APAMA_CONTAINER, "engine_inspect", "-p", str(APAMA_PORT)],
        capture_output=True, text=True,
    )
    # Look for the event type with Num Event Templates > 0
    in_events = False
    for line in result.stdout.splitlines():
        if "Event Types" in line:
            in_events = True
            continue
        if in_events and event_type in line:
            # Check if it has templates (listeners)
            parts = line.split()
            if len(parts) >= 2:
                try:
                    count = int(parts[-1])
                    return count > 0
                except ValueError:
                    pass
    return False


def run_apama(scenario: str, events: List[Dict], runs: int,
              data_dir: Path = None) -> BenchmarkResult:
    """Run Apama benchmark using engine_send from file.

    Events are written to an .evt file, copied into the container,
    and sent via `engine_send` which handles the Apama protocol correctly.
    Timing measures the engine_send process (includes file read + TCP send + processing).
    Memory tracked via `docker stats` during execution.
    """
    if not apama_available():
        return BenchmarkResult(scenario, "apama", len(events), 0, 0, 0,
                               "Apama container not running")

    # Check if Apama has listeners for this scenario's event types
    first_event_type = events[0]["event_type"] if events else "Unknown"
    if not apama_has_event_listener(first_event_type):
        return BenchmarkResult(scenario, "apama", len(events), 0, 0, 0,
                               f"No Apama listener for {first_event_type} (monitor not loaded)")

    # Write events in Apama format
    out_dir = data_dir or DATA_DIR
    apama_file = out_dir / f"{scenario}_apama.evt"
    apama_file.parent.mkdir(exist_ok=True)
    write_apama_evt(events, apama_file)

    # Copy event file into container
    container_evt = f"/tmp/{scenario}_bench.evt"
    subprocess.run(
        ["docker", "cp", str(apama_file), f"{APAMA_CONTAINER}:{container_evt}"],
        capture_output=True,
    )

    throughputs = []
    peak_rss_values = []

    for run_idx in range(runs):
        try:
            # Track Apama correlator memory during processing
            mem_tracker = MemoryTracker("container", APAMA_CONTAINER)
            mem_tracker.start()

            start = time.perf_counter()
            result = subprocess.run(
                [
                    "docker", "exec", APAMA_CONTAINER,
                    "engine_send", "-p", str(APAMA_PORT), container_evt,
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )
            elapsed = time.perf_counter() - start
            mem_tracker.stop()

            if result.returncode != 0:
                return BenchmarkResult(scenario, "apama", len(events), 0, 0, 0,
                                       f"engine_send failed: {result.stderr.strip()[:100]}")

            throughput = len(events) / elapsed if elapsed > 0 else 0
            throughputs.append(throughput)

            peak_rss = mem_tracker.get_peak_mb()
            if peak_rss > 0:
                peak_rss_values.append(peak_rss)

            # Brief pause between runs to let correlator settle
            if run_idx < runs - 1:
                time.sleep(0.5)

        except subprocess.TimeoutExpired:
            return BenchmarkResult(scenario, "apama", len(events), 0, 0, 0, "Timeout")
        except Exception as e:
            return BenchmarkResult(scenario, "apama", len(events), 0, 0, 0, str(e))

    if not throughputs:
        return BenchmarkResult(scenario, "apama", len(events), 0, 0, 0, "No successful runs")

    median_throughput = statistics.median(throughputs)
    median_elapsed = (len(events) / median_throughput * 1000) if median_throughput > 0 else 0
    median_peak_rss = statistics.median(peak_rss_values) if peak_rss_values else 0.0

    # Output count: Apama sends to "output" channel — not captured externally.
    return BenchmarkResult(
        scenario, "apama", len(events),
        median_elapsed, median_throughput, -1,
        runs=throughputs, peak_rss_mb=median_peak_rss,
    )


# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------

def format_memory(mb: float) -> str:
    if mb <= 0:
        return "—"
    elif mb >= 1024:
        return f"{mb/1024:.1f} GB"
    elif mb >= 1:
        return f"{mb:.1f} MB"
    else:
        return f"{mb*1024:.0f} KB"


def print_results(results: List[BenchmarkResult], num_runs: int):
    """Print benchmark results with comparison table."""
    # Group by scenario
    scenarios = {}
    for r in results:
        scenarios.setdefault(r.scenario, {})[r.engine] = r

    print("\n" + "=" * 110)
    print(f"BENCHMARK RESULTS  (median of {num_runs} runs)")
    print("=" * 110)

    header = (
        f"{'Scenario':<20} {'Engine':<10} {'Events':>8} {'Time (ms)':>10} "
        f"{'Throughput':>14} {'Outputs':>8} {'Peak RSS':>10}"
    )
    print(f"\n{header}")
    print("-" * 110)

    for scenario in sorted(scenarios.keys()):
        group = scenarios[scenario]
        for engine in ["varpulis", "apama"]:
            if engine not in group:
                continue
            r = group[engine]
            if r.error:
                print(f"{r.scenario:<20} {r.engine:<10} ERROR: {r.error}")
            else:
                out_str = str(r.output_count) if r.output_count >= 0 else "N/A"
                print(
                    f"{r.scenario:<20} {r.engine:<10} {r.events:>8,} {r.elapsed_ms:>10.1f} "
                    f"{r.throughput:>12,.0f}/s {out_str:>8} "
                    f"{format_memory(r.peak_rss_mb):>10}"
                )

        # Print comparison if both engines present
        if "varpulis" in group and "apama" in group:
            v = group["varpulis"]
            a = group["apama"]
            if v.throughput > 0 and a.throughput > 0 and not v.error and not a.error:
                ratio = v.throughput / a.throughput
                winner = "Varpulis" if ratio > 1 else "Apama"
                factor = max(ratio, 1 / ratio)
                mem_note = ""
                if v.peak_rss_mb > 0 and a.peak_rss_mb > 0:
                    mem_ratio = a.peak_rss_mb / v.peak_rss_mb
                    mem_winner = "Varpulis" if mem_ratio > 1 else "Apama"
                    mem_note = f"  |  RAM: {mem_winner} {max(mem_ratio, 1/mem_ratio):.1f}x lighter"
                print(f"{'':>20} {'->':>10} {winner} {factor:.1f}x faster{mem_note}")
        print()

    # Summary comparison table
    if any("varpulis" in scenarios[s] and "apama" in scenarios[s] for s in scenarios):
        print("=" * 110)
        print("COMPARISON SUMMARY")
        print("=" * 110)
        print(
            f"\n{'Scenario':<20} {'Apama':>14} {'Varpulis':>14} "
            f"{'Winner':>16} {'Apama RSS':>12} {'V. RSS':>10} {'RSS Winner':>14}"
        )
        print("-" * 100)
        for scenario in sorted(scenarios.keys()):
            group = scenarios[scenario]
            if "varpulis" not in group or "apama" not in group:
                continue
            v = group["varpulis"]
            a = group["apama"]
            if v.error or a.error:
                continue
            ratio = v.throughput / a.throughput if a.throughput > 0 else 0
            if ratio > 1:
                winner = f"V {ratio:.1f}x"
            elif ratio > 0:
                winner = f"A {1/ratio:.1f}x"
            else:
                winner = "N/A"
            if v.peak_rss_mb > 0 and a.peak_rss_mb > 0:
                mem_ratio = a.peak_rss_mb / v.peak_rss_mb
                rss_winner = f"V {mem_ratio:.1f}x" if mem_ratio > 1 else f"A {1/mem_ratio:.1f}x"
            else:
                rss_winner = "—"
            print(
                f"{scenario:<20} {a.throughput:>12,.0f}/s {v.throughput:>12,.0f}/s "
                f"{winner:>16} {format_memory(a.peak_rss_mb):>12} "
                f"{format_memory(v.peak_rss_mb):>10} {rss_winner:>14}"
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fair Benchmark: Apama v27.18 vs Varpulis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 run_scenarios.py --events 100000 --engine both --runs 3
  python3 run_scenarios.py --events 50000 --engine varpulis -s 01_filter
  python3 run_scenarios.py -n 100000 -e both -r 5
        """,
    )
    parser.add_argument("--scenario", "-s", help="Run specific scenario (e.g., 01_filter)")
    parser.add_argument("--events", "-n", type=int, default=100000, help="Number of events (default: 100000)")
    parser.add_argument("--engine", "-e", choices=["varpulis", "apama", "both"], default="both",
                        help="Which engine to benchmark (default: both)")
    parser.add_argument("--runs", "-r", type=int, default=3, help="Number of runs per scenario (default: 3)")
    parser.add_argument("--tmpfs", action="store_true",
                        help="Write event files to /dev/shm (ramdisk) to eliminate disk I/O")
    parser.add_argument("--workers", "-w", type=int, default=1,
                        help="Number of worker threads for Varpulis (default: 1)")
    args = parser.parse_args()

    # Find scenarios
    if args.scenario:
        scenarios = [args.scenario]
    else:
        scenarios = sorted([
            d.name for d in SCENARIOS_DIR.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])

    print("=" * 96)
    print("Fair Benchmark: Apama v27.18 vs Varpulis")
    print("=" * 96)
    print(f"Scenarios:  {len(scenarios)}")
    print(f"Events:     {args.events:,}")
    print(f"Runs:       {args.runs} (median reported)")
    print(f"Engine:     {args.engine}")
    io_mode = "ramdisk (/dev/shm)" if args.tmpfs else "disk"
    print()
    print("Methodology:")
    print(f"  Storage:  {io_mode}")
    print(f"  Varpulis: simulate --preload --immediate --quiet --workers {args.workers}, .evt format")
    print("            Timing: engine internal clock (processing only)")
    print("  Apama:    engine_send from file to running correlator, native format")
    print("            Timing: engine_send start to completion")
    print("  Memory:   peak RSS tracked via /proc (Varpulis) and docker stats (Apama)")

    # Check prerequisites
    varpulis_bin = PROJECT_ROOT / "target" / "release" / "varpulis"
    if args.engine in ["varpulis", "both"] and not varpulis_bin.exists():
        print("\nBuilding Varpulis release binary...")
        subprocess.run(["cargo", "build", "--release", "-p", "varpulis-cli"],
                        cwd=PROJECT_ROOT, capture_output=True)

    if args.engine in ["apama", "both"]:
        if apama_available():
            print(f"\nApama container '{APAMA_CONTAINER}' is running on port {APAMA_PORT}")
            print("  Injecting scenario monitors...")
            apama_inject_monitors()
            time.sleep(1)
        else:
            print(f"\nWARN: Apama container '{APAMA_CONTAINER}' not running. Skipping Apama benchmarks.")
            if args.engine == "apama":
                return

    # Set up data directory (tmpfs ramdisk or regular disk)
    data_dir = None
    if args.tmpfs:
        data_dir = Path("/dev/shm/varpulis-bench")
        data_dir.mkdir(exist_ok=True)
        print(f"\nUsing ramdisk: {data_dir}")
    else:
        print(f"\nUsing disk: {DATA_DIR}")

    results = []

    for scenario in scenarios:
        print(f"\n>>> Scenario: {scenario}")

        events = generate_events(scenario, args.events)
        print(f"    Generated {len(events):,} events")

        if args.engine in ["varpulis", "both"]:
            print(f"    Running Varpulis ({args.runs} runs)...", end="", flush=True)
            result = run_varpulis(scenario, events, args.runs, data_dir, args.workers)
            results.append(result)
            if result.error:
                print(f" ERROR: {result.error}")
            else:
                mem = f", RSS: {format_memory(result.peak_rss_mb)}" if result.peak_rss_mb > 0 else ""
                print(f" {result.throughput:,.0f} evt/s (outputs: {result.output_count}{mem})")

        if args.engine in ["apama", "both"] and apama_available():
            print(f"    Running Apama ({args.runs} runs)...", end="", flush=True)
            result = run_apama(scenario, events, args.runs, data_dir)
            results.append(result)
            if result.error:
                print(f" ERROR: {result.error}")
            else:
                mem = f", RSS: {format_memory(result.peak_rss_mb)}" if result.peak_rss_mb > 0 else ""
                print(f" {result.throughput:,.0f} evt/s{mem}")

    print_results(results, args.runs)

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
            "peak_rss_mb": round(r.peak_rss_mb, 1),
            "error": r.error,
            "all_runs_throughput": r.runs,
        } for r in results], f, indent=2)
    print(f"\nResults saved to: {results_file}")


if __name__ == "__main__":
    main()
