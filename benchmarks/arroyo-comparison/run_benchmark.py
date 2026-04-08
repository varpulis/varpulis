#!/usr/bin/env python3
"""
Arroyo vs Varpulis benchmark runner — Kafka mode (via Redpanda).

Methodology:
  - Pre-load N events into a Redpanda topic (one-time, before any timing).
  - For each engine:
      1. Recreate the OUTPUT topic (clean slate per run).
      2. Reset/start the engine consuming the input topic from offset 0.
      3. Time wall-clock from "engine started consuming" to "output topic
         high-watermark reaches expected_out".
      4. Stop the engine.
  - Multiple runs, median throughput reported.
  - Output count is verified for correctness across both engines.

Engines:
  - Arroyo: REST API submits the SQL pipeline, polls for state=Running, then
    polls Redpanda for output offset to reach expected_out.
  - Varpulis: spawns `varpulis run --code ... ` (Kafka source pipeline),
    polls Redpanda for output offset to reach expected_out, then SIGTERMs.

Both engines see the same input data (same Redpanda topic, same offsets).
"""
import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
VARPULIS_BIN = PROJECT_ROOT / "target/release/varpulis"
ARROYO_API = "http://localhost:5115"
REDPANDA_CONTAINER = "bench-arroyo-redpanda"
REDPANDA_BROKER_INTERNAL = "redpanda:9092"
REDPANDA_BROKER_EXTERNAL = "localhost:29092"


# ---------------------------------------------------------------------------
# Redpanda helpers (rpk inside the container)
# ---------------------------------------------------------------------------

def rpk(*args, timeout=60):
    cmd = ["docker", "exec", REDPANDA_CONTAINER, "rpk"] + list(args) + [
        "--brokers", REDPANDA_BROKER_INTERNAL
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def topic_create(topic):
    rpk("topic", "create", topic, "--partitions", "1", "--replicas", "1")


def topic_delete(topic):
    rpk("topic", "delete", topic)


def group_delete(group):
    """Delete a consumer group so the next run starts from offset 0."""
    rpk("group", "delete", group)


def _wait_for_group_empty(group, timeout_s=15):
    """Poll `rpk group describe` until the group state is Empty (no
    live members) or doesn't exist. This lets `auto_offset_reset=earliest`
    actually apply on the next run — if any zombie member is still
    holding the group as Stable, a fresh Varpulis will join the existing
    group and pick up the committed/end offset instead of replaying."""
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


def topic_high_watermark(topic) -> int:
    """Return the current high-watermark of a topic (events in partition 0)."""
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


def produce_jsonl_to_topic(topic, jsonl_file):
    """Produce all lines of a JSONL file to a topic. Used for one-time prep."""
    with open(jsonl_file, "rb") as f:
        proc = subprocess.Popen(
            ["docker", "exec", "-i", REDPANDA_CONTAINER, "rpk", "topic", "produce",
             topic, "--brokers", REDPANDA_BROKER_INTERNAL],
            stdin=f, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        out, err = proc.communicate(timeout=600)
        if proc.returncode != 0:
            raise RuntimeError(
                f"rpk topic produce failed (rc={proc.returncode}):\n"
                f"  stdout: {out.decode()[-300:]}\n"
                f"  stderr: {err.decode()[-300:]}"
            )


# ---------------------------------------------------------------------------
# Arroyo helpers (REST API)
# ---------------------------------------------------------------------------

def arroyo_post(path, body):
    import urllib.request
    req = urllib.request.Request(
        f"{ARROYO_API}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def arroyo_get(path):
    import urllib.request
    with urllib.request.urlopen(f"{ARROYO_API}{path}", timeout=30) as resp:
        return json.loads(resp.read())


def arroyo_patch(path, body):
    import urllib.request
    req = urllib.request.Request(
        f"{ARROYO_API}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="PATCH",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def arroyo_delete(path):
    import urllib.request
    req = urllib.request.Request(f"{ARROYO_API}{path}", method="DELETE")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def arroyo_submit_pipeline(name, sql):
    return arroyo_post("/api/v1/pipelines", {
        "name": name,
        "query": sql,
        "udfs": [],
        "parallelism": 1,
    })


def arroyo_pipeline_state(pipeline_id):
    jobs = arroyo_get(f"/api/v1/pipelines/{pipeline_id}/jobs")
    if not jobs.get("data"):
        return "no_jobs"
    return jobs["data"][0].get("state", "?")


def arroyo_stop_pipeline(pipeline_id):
    arroyo_patch(f"/api/v1/pipelines/{pipeline_id}", {"stop": "immediate"})


def arroyo_delete_pipeline(pipeline_id):
    arroyo_delete(f"/api/v1/pipelines/{pipeline_id}")


# ---------------------------------------------------------------------------
# Engine runners
# ---------------------------------------------------------------------------

def run_arroyo(scenario, expected_out, run_idx) -> dict:
    sql_file = SCRIPT_DIR / "scenarios" / scenario / "arroyo.sql"
    sql = sql_file.read_text()

    # Recreate the output topic for this run
    out_topic = {
        "01_filter": "scenario-01-filter-out",
        "02_aggregation": "scenario-02-agg-out",
    }[scenario]
    topic_delete(out_topic)
    topic_create(out_topic)

    # Submit and wait for Running
    pipeline_name = f"bench-{scenario}-arroyo-{run_idx}-{int(time.time())}"
    resp = arroyo_submit_pipeline(pipeline_name, sql)
    pipeline_id = resp["id"]

    # Poll for Running
    deadline = time.perf_counter() + 60
    while time.perf_counter() < deadline:
        if arroyo_pipeline_state(pipeline_id) == "Running":
            break
        time.sleep(0.2)
    else:
        arroyo_stop_pipeline(pipeline_id)
        arroyo_delete_pipeline(pipeline_id)
        raise RuntimeError("Arroyo pipeline never reached Running")

    # Start the timer NOW; the source begins consuming from earliest offset
    start = time.perf_counter()

    # Poll output topic
    deadline = time.perf_counter() + 180
    out = 0
    while time.perf_counter() < deadline:
        out = topic_high_watermark(out_topic)
        if out >= expected_out:
            break
        time.sleep(0.05)

    elapsed = time.perf_counter() - start

    # Stop and delete the pipeline so the next run is clean
    try:
        arroyo_stop_pipeline(pipeline_id)
    except Exception:
        pass
    time.sleep(1)
    try:
        arroyo_delete_pipeline(pipeline_id)
    except Exception:
        pass

    return {"elapsed_s": elapsed, "output_count": out}


def run_varpulis(scenario, expected_out, run_idx) -> dict:
    """Run Varpulis as a Kafka consumer pipeline. Spawn `varpulis run`,
    poll the output topic until expected_out is reached, then SIGTERM.

    To sidestep Redpanda's consumer-group session-timeout (~10 s, which
    breaks back-to-back runs because `auto_offset_reset=earliest` only
    applies to EMPTY groups), we use a unique `group_id` per run. The
    VPL file is loaded, patched to inject the unique group_id into the
    source connector's `.from(...)` extra-params, and streamed to the
    CLI via `--code`."""
    vpl_file = SCRIPT_DIR / "scenarios" / scenario / "varpulis.vpl"
    out_topic = {
        "01_filter": "scenario-01-filter-vpl-out",
        "02_aggregation": "scenario-02-agg-vpl-out",
        "03_join": "scenario-03-join-vpl-out",
    }[scenario]
    topic_delete(out_topic)
    topic_create(out_topic)

    vpl = vpl_file.read_text()
    # Inject a unique group_id per run at the `.from(RedpandaIn)` call
    # site. The parameter is appended as the only extra-param since
    # none of our benchmark VPLs use others.
    unique_group = f"bench-{scenario}-{run_idx}-{int(time.time() * 1000)}"
    vpl = vpl.replace(
        ".from(RedpandaIn)",
        f'.from(RedpandaIn, group_id: "{unique_group}")',
    )

    cmd = [str(VARPULIS_BIN), "run", "--code", vpl, "--quiet"]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    start = time.perf_counter()
    deadline = time.perf_counter() + 180
    out = 0
    while time.perf_counter() < deadline:
        if proc.poll() is not None:
            break
        out = topic_high_watermark(out_topic)
        if out >= expected_out:
            break
        time.sleep(0.05)

    elapsed = time.perf_counter() - start

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()

    return {"elapsed_s": elapsed, "output_count": out}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

EXPECTED_OUT = {
    "01_filter": 89_000,
    "02_aggregation": 99_000,
}

INPUT_TOPIC = {
    "01_filter": "scenario-01-filter",
    "02_aggregation": "scenario-02-agg",
}


def prep_input_topic(scenario, events):
    topic = INPUT_TOPIC[scenario]
    flat_file = SCRIPT_DIR / "data" / f"{scenario}.flat.jsonl"
    print(f"  preparing topic {topic} with {events} events…")
    topic_delete(topic)
    topic_create(topic)
    if not flat_file.exists():
        subprocess.run(
            ["python3", str(SCRIPT_DIR.parent / "proton-comparison" / "generate_events.py"),
             scenario, str(events), str(SCRIPT_DIR / "data")],
            check=True,
        )
    produce_jsonl_to_topic(topic, flat_file)
    # Verify
    actual = topic_high_watermark(topic)
    if actual < events:
        raise RuntimeError(f"only {actual}/{events} events in topic")
    print(f"  topic ready: {actual} events")


def run_scenario(scenario, events, runs):
    print(f"\n=== Scenario {scenario} ({events} events, {runs} runs) ===")
    prep_input_topic(scenario, events)
    expected_out = EXPECTED_OUT[scenario]

    # Arroyo
    print("  Arroyo…")
    a_times, a_outs = [], []
    for i in range(runs):
        try:
            r = run_arroyo(scenario, expected_out, i)
            a_times.append(r["elapsed_s"])
            a_outs.append(r["output_count"])
            print(f"    run {i+1}: {r['elapsed_s']*1000:.1f}ms  out={r['output_count']}")
        except Exception as e:
            print(f"    run {i+1}: ERROR — {e}")

    # Varpulis
    print("  Varpulis…")
    v_times, v_outs = [], []
    for i in range(runs):
        try:
            r = run_varpulis(scenario, expected_out, i)
            v_times.append(r["elapsed_s"])
            v_outs.append(r["output_count"])
            print(f"    run {i+1}: {r['elapsed_s']*1000:.1f}ms  out={r['output_count']}")
        except Exception as e:
            print(f"    run {i+1}: ERROR — {e}")

    return {
        "scenario": scenario,
        "events": events,
        "runs": runs,
        "engines": {
            "arroyo": {
                "median_s": statistics.median(a_times) if a_times else None,
                "throughput_eps": events / statistics.median(a_times) if a_times else None,
                "output_count": a_outs[-1] if a_outs else -1,
            },
            "varpulis": {
                "median_s": statistics.median(v_times) if v_times else None,
                "throughput_eps": events / statistics.median(v_times) if v_times else None,
                "output_count": v_outs[-1] if v_outs else -1,
            },
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="01_filter")
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out", default=str(SCRIPT_DIR / "results" / "benchmark.json"))
    args = parser.parse_args()

    result = run_scenario(args.scenario, args.events, args.runs)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n=== Summary ===")
    a = result["engines"]["arroyo"]
    v = result["engines"]["varpulis"]
    if a["throughput_eps"] and v["throughput_eps"]:
        ratio = v["throughput_eps"] / a["throughput_eps"]
        print(f"  {result['scenario']}: "
              f"Varpulis {v['throughput_eps']:>10,.0f} eps  "
              f"|  Arroyo {a['throughput_eps']:>10,.0f} eps  "
              f"|  V/A {ratio:.2f}x")


if __name__ == "__main__":
    main()
