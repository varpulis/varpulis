#!/usr/bin/env python3
"""
Kafka exactly-once crash recovery test harness.

Verifies that the Varpulis 2PC checkpoint barrier delivers exactly-once
semantics by killing the process at different points during processing
and verifying no duplicates or gaps on restart.

Architecture:
  - Redpanda (Kafka-compatible) runs via docker compose.
  - A VPL pipeline reads from an input topic and writes transactionally
    to an output topic (exactly_once: true).
  - The harness produces N sequentially-numbered events, starts varpulis,
    kills it at various points, restarts, and verifies the output.

The 2PC barrier in run.rs fires every ~2 seconds (tied to the progress
report timer). On restart, the consumer group picks up from the last
committed offset, and uncommitted transactions are invisible to
read_committed consumers.

Prerequisites:
  - Docker with Redpanda running (see docker-compose.yml)
  - varpulis binary built with --features kafka
  - Python 3.8+

Usage:
  python3 test_harness.py [--events N] [--varpulis-bin PATH]
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_VARPULIS_BIN = PROJECT_ROOT / "target/release/varpulis"
VPL_FILE = SCRIPT_DIR / "scenario_eo.vpl"

REDPANDA_CONTAINER = "eo-test-redpanda"
REDPANDA_BROKER_INTERNAL = "redpanda:9092"
REDPANDA_BROKER_EXTERNAL = "localhost:29092"

INPUT_TOPIC = "test-eo-input"
OUTPUT_TOPIC = "test-eo-output"
CONSUMER_GROUP = "eo-test-consumer"

# ---------------------------------------------------------------------------
# Redpanda helpers (rpk inside the container)
# ---------------------------------------------------------------------------

def rpk(*args, timeout=60):
    """Run rpk inside the Redpanda container."""
    cmd = [
        "docker", "exec", REDPANDA_CONTAINER, "rpk",
    ] + list(args) + [
        "--brokers", REDPANDA_BROKER_INTERNAL,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def topic_create(topic, partitions=1):
    r = rpk("topic", "create", topic,
            "--partitions", str(partitions),
            "--replicas", "1")
    if r.returncode != 0 and "TOPIC_ALREADY_EXISTS" not in r.stderr:
        print(f"  WARN: topic create {topic}: {r.stderr.strip()}")


def topic_delete(topic):
    rpk("topic", "delete", topic)


def topic_high_watermark(topic) -> int:
    """Return the current high-watermark of partition 0."""
    r = rpk("topic", "describe", "-p", topic)
    if r.returncode != 0:
        return 0
    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == "0":
            try:
                return int(parts[5])
            except ValueError:
                continue
    return 0


def group_delete(group):
    rpk("group", "delete", group)


def wait_for_group_empty(group, timeout_s=15):
    """Wait until the consumer group is Empty/Dead/gone."""
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        r = rpk("group", "describe", group)
        if r.returncode != 0 or "GROUP_ID_NOT_FOUND" in r.stdout + r.stderr:
            return
        state_line = next(
            (l for l in r.stdout.splitlines() if l.startswith("STATE")), ""
        )
        if "Empty" in state_line or "Dead" in state_line:
            return
        time.sleep(0.3)


def produce_events(topic, count):
    """Produce `count` sequential Reading events to the topic via rpk."""
    print(f"  Producing {count} events to {topic}...")
    # Build JSONL payload
    lines = []
    for i in range(count):
        event = {
            "event_type": "Reading",
            "id": i,
            "value": float(i) * 1.5,
        }
        lines.append(json.dumps(event))
    payload = "\n".join(lines) + "\n"

    proc = subprocess.Popen(
        [
            "docker", "exec", "-i", REDPANDA_CONTAINER, "rpk",
            "topic", "produce", topic,
            "--brokers", REDPANDA_BROKER_INTERNAL,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate(input=payload.encode(), timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(
            f"rpk produce failed (rc={proc.returncode}):\n"
            f"  stdout: {out.decode()[-500:]}\n"
            f"  stderr: {err.decode()[-500:]}"
        )

    # Verify the watermark
    hw = topic_high_watermark(topic)
    print(f"  Input topic high watermark: {hw}")
    if hw < count:
        raise RuntimeError(
            f"Expected {count} events but high watermark is {hw}"
        )


def consume_output_read_committed(topic, timeout_s=30):
    """Consume all visible (read_committed) messages from the output topic.

    Returns a list of parsed JSON records.
    """
    # Use rpk topic consume with --read-committed flag.
    # rpk consume reads from the beginning by default.
    # We use --offset start and --num to read all, with a timeout.
    #
    # rpk topic consume outputs one JSON object per line for each message.
    # The format is: {"topic":"...","value":"...","timestamp":...,...}
    r = rpk(
        "topic", "consume", topic,
        "--offset", "start",
        "--format", "%v\n",
        timeout=timeout_s,
    )
    # rpk consume will timeout (which is fine -- it means it read all available).
    records = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            records.append(record)
        except json.JSONDecodeError:
            # Skip non-JSON lines (e.g., rpk metadata)
            pass
    return records


def consume_output_via_pipe(topic, expected_count, timeout_s=60):
    """Consume all messages from the output topic.

    Uses rpk --num to read a fixed count and exit cleanly.
    Returns list of parsed JSON records.
    """
    # rpk --num counts DATA records (not transaction markers), so use
    # expected_count directly. If there are fewer records, rpk blocks
    # until timeout — that's the failure case we want to detect.
    num = expected_count
    r = subprocess.run(
        [
            "docker", "exec", REDPANDA_CONTAINER, "rpk",
            "topic", "consume", topic,
            "--offset", "start",
            "--num", str(num),
            "--format", "%v\n",
            "--brokers", REDPANDA_BROKER_INTERNAL,
        ],
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )

    records = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            records.append(record)
        except json.JSONDecodeError:
            pass

    return records


# ---------------------------------------------------------------------------
# Varpulis process management
# ---------------------------------------------------------------------------

CHECKPOINT_DIR = Path("/tmp/varpulis-eo-test-checkpoints")


def start_varpulis(varpulis_bin, vpl_file, quiet=True):
    """Start varpulis run with the given VPL file. Returns Popen handle."""
    cmd = [
        str(varpulis_bin),
        "run",
        "--file", str(vpl_file),
        "--checkpoint-dir", str(CHECKPOINT_DIR),
        "--checkpoint-interval", "2",
    ]
    if quiet:
        cmd.append("--quiet")

    print(f"  Starting varpulis: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Give it a moment to connect to Kafka
    time.sleep(2)
    if proc.poll() is not None:
        out = proc.stdout.read().decode()
        err = proc.stderr.read().decode()
        raise RuntimeError(
            f"Varpulis exited immediately (rc={proc.returncode}):\n"
            f"  stdout: {out[-500:]}\n"
            f"  stderr: {err[-500:]}"
        )
    return proc


def kill_varpulis(proc):
    """Hard-kill (SIGKILL) the varpulis process, simulating a crash."""
    pid = proc.pid
    print(f"  Sending SIGKILL to varpulis (pid={pid})...")
    os.kill(pid, signal.SIGKILL)
    proc.wait(timeout=10)
    print(f"  Varpulis process exited (rc={proc.returncode})")


def stop_varpulis_graceful(proc, timeout=10):
    """Gracefully stop varpulis with SIGINT (Ctrl+C), then SIGKILL if needed.

    Note: varpulis handles SIGINT via tokio::signal::ctrl_c(), NOT SIGTERM.
    The graceful shutdown path runs a final 2PC barrier before exiting.
    """
    pid = proc.pid
    print(f"  Sending SIGINT to varpulis (pid={pid})...")
    os.kill(pid, signal.SIGINT)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"  SIGTERM timed out, sending SIGKILL...")
        os.kill(pid, signal.SIGKILL)
        proc.wait(timeout=5)
    print(f"  Varpulis stopped (rc={proc.returncode})")


def poll_output_watermark(expected, poll_interval=0.5, timeout_s=120):
    """Poll output topic high-watermark until it reaches expected count."""
    deadline = time.perf_counter() + timeout_s
    last_hw = 0
    stall_start = None

    while time.perf_counter() < deadline:
        hw = topic_high_watermark(OUTPUT_TOPIC)
        if hw != last_hw:
            last_hw = hw
            stall_start = time.perf_counter()
        elif stall_start and (time.perf_counter() - stall_start) > 30:
            # Stalled for 30 seconds -- break early
            print(f"  Output stalled at {hw}/{expected} for 30s")
            break

        if hw >= expected:
            return hw
        time.sleep(poll_interval)

    return last_hw


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_exactly_once(expected_count, timeout_s=60):
    """Consume output topic and verify exactly-once delivery.

    Returns (passed: bool, details: dict).
    """
    print(f"  Verifying output (expecting {expected_count} events)...")

    records = consume_output_via_pipe(OUTPUT_TOPIC, expected_count, timeout_s)

    total = len(records)
    print(f"  Consumed {total} records from output topic")

    # Extract IDs -- the output should have 'id' field from the VPL emit
    ids = []
    for r in records:
        event_id = r.get("id")
        if event_id is not None:
            ids.append(int(event_id))

    if len(ids) != total:
        print(f"  WARNING: {total - len(ids)} records missing 'id' field")

    # Check for duplicates
    id_counts = {}
    for eid in ids:
        id_counts[eid] = id_counts.get(eid, 0) + 1

    duplicates = {k: v for k, v in id_counts.items() if v > 1}
    duplicate_count = sum(v - 1 for v in duplicates.values())

    # Check for gaps
    if ids:
        expected_ids = set(range(expected_count))
        actual_ids = set(ids)
        missing = expected_ids - actual_ids
        extra = actual_ids - expected_ids
    else:
        missing = set(range(expected_count))
        extra = set()

    passed = (
        total == expected_count
        and duplicate_count == 0
        and len(missing) == 0
    )

    details = {
        "total_records": total,
        "expected": expected_count,
        "unique_ids": len(set(ids)),
        "duplicate_ids": len(duplicates),
        "duplicate_records": duplicate_count,
        "missing_ids": len(missing),
        "extra_ids": len(extra),
    }

    if duplicates:
        # Show first 10 duplicate IDs
        sample = sorted(duplicates.keys())[:10]
        details["duplicate_sample"] = {k: duplicates[k] for k in sample}
    if missing:
        sample = sorted(missing)[:10]
        details["missing_sample"] = sample

    return passed, details


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

def setup_topics():
    """Create/recreate input and output topics."""
    print("Setting up topics...")
    topic_delete(INPUT_TOPIC)
    topic_delete(OUTPUT_TOPIC)
    time.sleep(1)
    topic_create(INPUT_TOPIC)
    topic_create(OUTPUT_TOPIC)
    time.sleep(1)


def reset_output():
    """Delete and recreate the output topic for a fresh scenario."""
    print("  Resetting output topic...")
    topic_delete(OUTPUT_TOPIC)
    time.sleep(1)
    topic_create(OUTPUT_TOPIC)
    time.sleep(1)


def reset_checkpoint_dir():
    """Clean the checkpoint directory for a fresh scenario."""
    import shutil
    if CHECKPOINT_DIR.exists():
        shutil.rmtree(CHECKPOINT_DIR)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def reset_consumer_group():
    """Delete the consumer group so varpulis restarts from committed offsets
    or auto_offset_reset=earliest."""
    group_delete(CONSUMER_GROUP)
    wait_for_group_empty(CONSUMER_GROUP)


def check_redpanda():
    """Verify Redpanda is running and reachable."""
    r = rpk("cluster", "info")
    if r.returncode != 0:
        print("ERROR: Redpanda not reachable. Start it with:")
        print("  docker compose -f tests/kafka_exactly_once/docker-compose.yml up -d")
        sys.exit(1)
    print("Redpanda is running.")


def check_varpulis_bin(bin_path):
    """Verify the varpulis binary exists."""
    if not bin_path.exists():
        print(f"ERROR: varpulis binary not found at {bin_path}")
        print("Build it with:")
        print("  cargo build --release --features kafka -p varpulis-cli")
        sys.exit(1)
    print(f"Varpulis binary: {bin_path}")


# ---------------------------------------------------------------------------
# Test scenarios
# ---------------------------------------------------------------------------

def scenario_a_kill_during_processing(varpulis_bin, event_count):
    """Scenario A: Kill during processing (~50% progress).

    1. Produce N events to input.
    2. Start varpulis.
    3. Wait until ~50% of events appear in output.
    4. SIGKILL varpulis.
    5. Restart varpulis (same consumer group -- resumes from committed offset).
    6. Wait for output to reach N.
    7. Verify exactly-once.
    """
    print("\n" + "=" * 70)
    print("SCENARIO A: Kill during processing (~50% progress)")
    print("=" * 70)

    # Clean slate for this scenario
    reset_output()
    reset_consumer_group()
    reset_checkpoint_dir()

    # Produce input events
    produce_events(INPUT_TOPIC, event_count)

    # Start varpulis
    proc = start_varpulis(varpulis_bin, VPL_FILE)

    # Wait for ~50% progress
    target = event_count // 2
    print(f"  Waiting for output to reach ~{target} (50%)...")
    deadline = time.perf_counter() + 60
    while time.perf_counter() < deadline:
        hw = topic_high_watermark(OUTPUT_TOPIC)
        if hw >= target:
            print(f"  Output at {hw}/{event_count} -- killing now")
            break
        time.sleep(0.5)
    else:
        hw = topic_high_watermark(OUTPUT_TOPIC)
        print(f"  Timeout waiting for 50%. Output at {hw}/{event_count} -- killing anyway")

    # Kill
    kill_varpulis(proc)

    hw_after_kill = topic_high_watermark(OUTPUT_TOPIC)
    print(f"  Output watermark after kill: {hw_after_kill}")

    # Wait a moment for Kafka group rebalance
    time.sleep(3)
    wait_for_group_empty(CONSUMER_GROUP)

    # Restart
    print("  Restarting varpulis...")
    proc = start_varpulis(varpulis_bin, VPL_FILE)

    # Wait for completion
    print(f"  Waiting for output to reach {event_count}...")
    final_hw = poll_output_watermark(event_count, timeout_s=120)
    print(f"  Final output watermark: {final_hw}")

    # Graceful stop
    stop_varpulis_graceful(proc)
    time.sleep(2)

    # Verify
    passed, details = verify_exactly_once(event_count)

    print(f"\n  Result: {'PASS' if passed else 'FAIL'}")
    for k, v in details.items():
        print(f"    {k}: {v}")

    return passed, details


def scenario_b_kill_after_short_run(varpulis_bin, event_count):
    """Scenario B: Kill after 1 second (minimal processing).

    Tests that very early kills (before any checkpoint completes)
    correctly replay all events on restart.
    """
    print("\n" + "=" * 70)
    print("SCENARIO B: Kill after 1 second (early kill)")
    print("=" * 70)

    # Clean slate
    reset_output()
    reset_consumer_group()

    produce_events(INPUT_TOPIC, event_count)

    # Start and kill quickly
    proc = start_varpulis(varpulis_bin, VPL_FILE)
    time.sleep(1)

    hw_before = topic_high_watermark(OUTPUT_TOPIC)
    print(f"  Output watermark after 1s: {hw_before}")

    kill_varpulis(proc)

    hw_after_kill = topic_high_watermark(OUTPUT_TOPIC)
    print(f"  Output watermark after kill: {hw_after_kill}")

    # Wait for group rebalance
    time.sleep(3)
    wait_for_group_empty(CONSUMER_GROUP)

    # Restart
    print("  Restarting varpulis...")
    proc = start_varpulis(varpulis_bin, VPL_FILE)

    # Wait for completion
    print(f"  Waiting for output to reach {event_count}...")
    final_hw = poll_output_watermark(event_count, timeout_s=120)
    print(f"  Final output watermark: {final_hw}")

    stop_varpulis_graceful(proc)
    time.sleep(2)

    # Verify
    passed, details = verify_exactly_once(event_count)

    print(f"\n  Result: {'PASS' if passed else 'FAIL'}")
    for k, v in details.items():
        print(f"    {k}: {v}")

    return passed, details


def scenario_c_kill_restart_3x(varpulis_bin, event_count):
    """Scenario C: Kill and restart 3 times, verify final output.

    The most aggressive test: multiple crash/restart cycles, each at
    different progress points. After the final restart, all events
    should appear exactly once.
    """
    print("\n" + "=" * 70)
    print("SCENARIO C: Kill and restart 3 times")
    print("=" * 70)

    # Clean slate
    reset_output()
    reset_consumer_group()

    produce_events(INPUT_TOPIC, event_count)

    kill_points = [0.2, 0.5, 0.8]  # Kill at ~20%, ~50%, ~80%

    for i, kill_fraction in enumerate(kill_points):
        target = int(event_count * kill_fraction)
        print(f"\n  --- Cycle {i + 1}/3: kill at ~{int(kill_fraction * 100)}% ({target} events) ---")

        proc = start_varpulis(varpulis_bin, VPL_FILE)

        # Wait for target progress
        deadline = time.perf_counter() + 60
        while time.perf_counter() < deadline:
            hw = topic_high_watermark(OUTPUT_TOPIC)
            if hw >= target:
                print(f"  Output at {hw}/{event_count} -- killing")
                break
            time.sleep(0.5)
        else:
            hw = topic_high_watermark(OUTPUT_TOPIC)
            print(f"  Timeout. Output at {hw}/{event_count} -- killing anyway")

        kill_varpulis(proc)

        hw_after = topic_high_watermark(OUTPUT_TOPIC)
        print(f"  Output watermark after kill: {hw_after}")

        time.sleep(3)
        wait_for_group_empty(CONSUMER_GROUP)

    # Final run -- let it complete
    print(f"\n  --- Final run: complete all {event_count} events ---")
    proc = start_varpulis(varpulis_bin, VPL_FILE)

    print(f"  Waiting for output to reach {event_count}...")
    final_hw = poll_output_watermark(event_count, timeout_s=180)
    print(f"  Final output watermark: {final_hw}")

    stop_varpulis_graceful(proc)
    time.sleep(2)

    # Verify
    passed, details = verify_exactly_once(event_count)

    print(f"\n  Result: {'PASS' if passed else 'FAIL'}")
    for k, v in details.items():
        print(f"    {k}: {v}")

    return passed, details


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Kafka exactly-once crash recovery test harness"
    )
    parser.add_argument(
        "--events", type=int, default=10_000,
        help="Number of input events to produce (default: 10000)"
    )
    parser.add_argument(
        "--varpulis-bin", type=Path, default=DEFAULT_VARPULIS_BIN,
        help=f"Path to varpulis binary (default: {DEFAULT_VARPULIS_BIN})"
    )
    parser.add_argument(
        "--scenario", choices=["a", "b", "c", "all"], default="all",
        help="Which scenario to run (default: all)"
    )
    args = parser.parse_args()

    print("=" * 70)
    print("  Kafka Exactly-Once Crash Recovery Test")
    print("=" * 70)
    print(f"  Events:  {args.events}")
    print(f"  Binary:  {args.varpulis_bin}")
    print(f"  VPL:     {VPL_FILE}")
    print()

    # Preflight checks
    check_redpanda()
    check_varpulis_bin(args.varpulis_bin)

    # One-time topic setup for the input (shared across scenarios)
    setup_topics()

    results = {}
    scenarios = {
        "a": ("A: Kill during processing", scenario_a_kill_during_processing),
        "b": ("B: Kill after short run", scenario_b_kill_after_short_run),
        "c": ("C: Kill/restart 3x", scenario_c_kill_restart_3x),
    }

    to_run = list(scenarios.keys()) if args.scenario == "all" else [args.scenario]

    for key in to_run:
        name, fn = scenarios[key]
        try:
            passed, details = fn(args.varpulis_bin, args.events)
            results[name] = ("PASS" if passed else "FAIL", details)
        except Exception as e:
            print(f"\n  EXCEPTION: {e}")
            results[name] = ("ERROR", {"exception": str(e)})

    # Summary
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)

    all_passed = True
    for name, (status, details) in results.items():
        icon = "OK" if status == "PASS" else "FAIL"
        print(f"  [{icon}] {name}")
        if status != "PASS":
            all_passed = False
            if "total_records" in details:
                print(f"         total={details['total_records']}, "
                      f"expected={details.get('expected', '?')}, "
                      f"duplicates={details.get('duplicate_records', '?')}, "
                      f"missing={details.get('missing_ids', '?')}")
            elif "exception" in details:
                print(f"         {details['exception']}")

    print()
    if all_passed:
        print("  ALL SCENARIOS PASSED")
    else:
        print("  SOME SCENARIOS FAILED")
    print("=" * 70)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
