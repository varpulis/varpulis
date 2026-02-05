#!/usr/bin/env python3
"""
Distributed Mandelbrot A/B Benchmark

Compares:
  A) Baseline: 1 server, 1 pipeline, 16 contexts (single process)
  B) Distributed: 1 coordinator + 4 workers, 4 pipelines, 4 contexts each

Prerequisites:
  - MQTT broker on localhost:1883
  - Built binary: cargo build --release

Usage:
  python3 examples/mandelbrot/distributed/bench.py
"""

import json
import os
import signal
import subprocess
import sys
import time

import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BINARY = os.path.join(SCRIPT_DIR, "..", "..", "..", "target", "release", "varpulis")
API_KEY = "test"
COORDINATOR_PORT = 9100
BASE_WORKER_PORT = 9000
BASELINE_PORT = 9010


def wait_for_server(url, timeout=10):
    """Wait for a server to become available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=2)
            return True
        except Exception:
            time.sleep(0.3)
    return False


def api_post(url, data, api_key=API_KEY):
    """POST JSON to an API endpoint."""
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
        },
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode()}")
        return None


def api_get(url, api_key=API_KEY):
    """GET from an API endpoint."""
    req = urllib.request.Request(url, headers={"x-api-key": api_key})
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  Error: {e}")
        return None


def read_vpl(filename):
    """Read a VPL file."""
    path = os.path.join(SCRIPT_DIR, filename)
    with open(path) as f:
        return f.read()


def read_baseline_vpl():
    """Read the single-process mandelbrot VPL."""
    path = os.path.join(SCRIPT_DIR, "..", "mandelbrot_server.vpl")
    with open(path) as f:
        return f.read()


def run_baseline():
    """Run baseline: single server, single pipeline with all 16 tiles."""
    print("=" * 60)
    print("BASELINE: 1 server, 1 pipeline (mandelbrot_server.vpl)")
    print("=" * 60)

    # Start server
    proc = subprocess.Popen(
        [BINARY, "server", "--port", str(BASELINE_PORT), "--api-key", API_KEY],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        if not wait_for_server(f"http://127.0.0.1:{BASELINE_PORT}/health"):
            print("ERROR: Baseline server did not start")
            return None

        # Deploy pipeline
        source = read_baseline_vpl()
        resp = api_post(
            f"http://127.0.0.1:{BASELINE_PORT}/api/v1/pipelines",
            {"name": "mandelbrot-baseline", "source": source},
        )
        if not resp:
            print("ERROR: Failed to deploy baseline pipeline")
            return None

        pipeline_id = resp["id"]
        print(f"  Pipeline deployed: {pipeline_id}")

        # Inject events and measure time
        # mandelbrot_server.vpl uses a single ComputeTile event with tile_id
        start = time.time()
        for tile_id in range(16):  # 16 tiles (4x4)
            row = tile_id // 4
            col = tile_id % 4
            api_post(
                f"http://127.0.0.1:{BASELINE_PORT}/api/v1/pipelines/{pipeline_id}/events",
                {
                    "event_type": "ComputeTile",
                    "fields": {"tile_id": row * 32 + col},
                },
            )
        elapsed = time.time() - start

        print(f"  16 events injected in {elapsed:.3f}s")
        return elapsed

    finally:
        proc.terminate()
        proc.wait()


def run_distributed():
    """Run distributed: coordinator + 4 workers."""
    print()
    print("=" * 60)
    print("DISTRIBUTED: 1 coordinator + 4 workers, 4 pipelines")
    print("=" * 60)

    procs = []

    try:
        # Start coordinator
        coord = subprocess.Popen(
            [
                BINARY,
                "coordinator",
                "--port",
                str(COORDINATOR_PORT),
                "--api-key",
                API_KEY,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(coord)

        if not wait_for_server(f"http://127.0.0.1:{COORDINATOR_PORT}/health"):
            print("ERROR: Coordinator did not start")
            return None

        # Start 4 workers
        for i in range(4):
            port = BASE_WORKER_PORT + i
            worker = subprocess.Popen(
                [
                    BINARY,
                    "server",
                    "--port",
                    str(port),
                    "--api-key",
                    API_KEY,
                    "--coordinator",
                    f"http://127.0.0.1:{COORDINATOR_PORT}",
                    "--worker-id",
                    f"worker-{i}",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            procs.append(worker)

        # Wait for workers to register
        time.sleep(3)

        workers = api_get(
            f"http://127.0.0.1:{COORDINATOR_PORT}/api/v1/cluster/workers"
        )
        if workers:
            print(f"  {workers['total']} workers registered")
        else:
            print("  WARNING: Could not verify worker registration")

        # Deploy pipeline group
        spec = {
            "name": "mandelbrot-distributed",
            "pipelines": [
                {
                    "name": f"row{i}",
                    "source": read_vpl(f"mandelbrot_worker_{i}.vpl"),
                    "worker_affinity": f"worker-{i}",
                }
                for i in range(4)
            ],
            "routes": [
                {
                    "from_pipeline": "_external",
                    "to_pipeline": f"row{i}",
                    "event_types": [f"ComputeTile{i}*"],
                }
                for i in range(4)
            ],
        }

        resp = api_post(
            f"http://127.0.0.1:{COORDINATOR_PORT}/api/v1/cluster/pipeline-groups",
            spec,
        )
        if not resp:
            print("ERROR: Failed to deploy pipeline group")
            return None

        group_id = resp["id"]
        print(f"  Pipeline group deployed: {group_id} ({resp['status']})")

        # Inject events via coordinator
        start = time.time()
        for row in range(4):
            for col in range(4):
                api_post(
                    f"http://127.0.0.1:{COORDINATOR_PORT}/api/v1/cluster/pipeline-groups/{group_id}/inject",
                    {"event_type": f"ComputeTile{row}{col}", "fields": {}},
                )
        elapsed = time.time() - start

        print(f"  16 events injected in {elapsed:.3f}s")
        return elapsed

    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()


def main():
    if not os.path.isfile(BINARY):
        print(f"Binary not found: {BINARY}")
        print("Run: cargo build --release")
        sys.exit(1)

    print("Distributed Mandelbrot A/B Benchmark")
    print("-" * 60)
    print()

    baseline_time = run_baseline()
    distributed_time = run_distributed()

    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    if baseline_time is not None:
        print(f"  Baseline (1 process):    {baseline_time:.3f}s")
    if distributed_time is not None:
        print(f"  Distributed (5 processes): {distributed_time:.3f}s")
    if baseline_time and distributed_time:
        speedup = baseline_time / distributed_time
        print(f"  Speedup: {speedup:.2f}x")
    print()


if __name__ == "__main__":
    main()
