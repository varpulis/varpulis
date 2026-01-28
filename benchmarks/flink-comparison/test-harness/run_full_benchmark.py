#!/usr/bin/env python3
"""
Full benchmark comparison: Varpulis vs Flink CEP
Runs both systems and compares results.

Usage:
    python run_full_benchmark.py [processing|event]

    processing: Use processing time (wall-clock) - fair comparison, low latency
    event: Use event time with watermarks - proper out-of-order handling
"""

import json
import time
import subprocess
import sys
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# Time semantics mode: "processing" or "event"
TIME_MODE = sys.argv[1] if len(sys.argv) > 1 else "processing"

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
INPUT_TOPIC_PREFIX = "benchmark/input/"
VARPULIS_OUTPUT = "benchmark/output/varpulis"
FLINK_OUTPUT = "benchmark/output/flink"

# Test events: Login -> FailedTransaction pattern
# Timestamps will be set dynamically based on current time
# Offsets in milliseconds - spaced 1 second apart for clearer watermark progression
EVENTS_BASE = [
    {"type": "Login", "user_id": "user1", "ip_address": "192.168.1.1", "device": "mobile", "ts_offset": 0},
    {"type": "Login", "user_id": "user2", "ip_address": "192.168.1.2", "device": "desktop", "ts_offset": 1000},
    {"type": "Transaction", "user_id": "user1", "amount": 500.0, "status": "failed", "merchant": "store_a", "ts_offset": 2000},
    {"type": "Transaction", "user_id": "user3", "amount": 100.0, "status": "success", "merchant": "store_b", "ts_offset": 3000},
    {"type": "Login", "user_id": "user3", "ip_address": "192.168.1.3", "device": "tablet", "ts_offset": 4000},
    {"type": "Transaction", "user_id": "user2", "amount": 1500.0, "status": "failed", "merchant": "store_c", "ts_offset": 5000},
    {"type": "Transaction", "user_id": "user3", "amount": 200.0, "status": "failed", "merchant": "store_d", "ts_offset": 6000},
    {"type": "Login", "user_id": "user4", "ip_address": "192.168.1.4", "device": "mobile", "ts_offset": 7000},
    {"type": "Transaction", "user_id": "user4", "amount": 50.0, "status": "success", "merchant": "store_e", "ts_offset": 8000},
    {"type": "Transaction", "user_id": "user4", "amount": 2000.0, "status": "failed", "merchant": "store_f", "ts_offset": 9000},
]

# Heartbeat event with high timestamp to force Flink watermark progression (only needed in event-time mode)
HEARTBEAT_EVENT = {"type": "Login", "user_id": "_heartbeat", "ip_address": "0.0.0.0", "device": "system", "ts_offset": 20000}

def get_events_template():
    """Get events template based on time mode."""
    if TIME_MODE == "event":
        # In event-time mode, include heartbeat to force watermark progression
        return EVENTS_BASE + [HEARTBEAT_EVENT]
    else:
        # In processing-time mode, no heartbeat needed
        return EVENTS_BASE

def get_events_with_timestamps():
    """Generate events with timestamps based on current time."""
    base_ts = int(time.time() * 1000)
    events = []
    for e in get_events_template():
        event = {k: v for k, v in e.items() if k != 'ts_offset'}
        event['ts'] = base_ts + e['ts_offset']
        events.append(event)
    return events

EXPECTED_USERS = sorted(["user1", "user2", "user3", "user4"])


class Collector:
    def __init__(self):
        self.varpulis = []
        self.flink = []
        self.start_time = None

    def on_message(self, client, userdata, msg):
        t = time.time() * 1000
        try:
            payload = json.loads(msg.payload.decode())
            # Varpulis puts fields in "data", Flink at root level
            data = payload.get("data", payload)
            user = data.get("user_id") or data.get("userId")
            if VARPULIS_OUTPUT in msg.topic:
                self.varpulis.append((user, t))
                print(f"  [VARPULIS] {user}")
            elif FLINK_OUTPUT in msg.topic:
                self.flink.append((user, t))
                print(f"  [FLINK]    {user}")
        except:
            pass


def main():
    print("="*70)
    print(" BENCHMARK: Varpulis vs Flink CEP")
    print(" Pattern: Login -> FailedTransaction (same user)")
    print(f" Time Mode: {TIME_MODE.upper()}")
    print("="*70)

    # Check MQTT
    try:
        test_client = mqtt.Client(client_id="test", protocol=mqtt.MQTTv311)
        test_client.connect(MQTT_BROKER, MQTT_PORT)
        test_client.disconnect()
        print(f"\n[OK] MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
    except Exception as e:
        print(f"\n[ERROR] Cannot connect to MQTT: {e}")
        sys.exit(1)

    collector = Collector()

    # Setup subscriber
    sub = mqtt.Client(client_id="benchmark_sub", protocol=mqtt.MQTTv311)
    sub.on_message = collector.on_message
    sub.connect(MQTT_BROKER, MQTT_PORT)
    sub.subscribe(VARPULIS_OUTPUT + "/#")
    sub.subscribe(FLINK_OUTPUT)
    sub.loop_start()

    # Setup publisher
    pub = mqtt.Client(client_id="benchmark_pub", protocol=mqtt.MQTTv311)
    pub.connect(MQTT_BROKER, MQTT_PORT)

    # Paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    vpl_file = os.path.join(base_dir, "..", "scenario2-sequence", "varpulis.vpl")
    flink_jar = os.path.join(base_dir, "target/flink-varpulis-comparison-1.0-SNAPSHOT.jar")
    varpulis_bin = "/home/cpo/cep/target/release/varpulis"

    # Start Varpulis
    print("\n[*] Starting Varpulis...")
    varpulis_proc = subprocess.Popen(
        [varpulis_bin, "run", "-f", vpl_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # Start Flink
    print(f"[*] Starting Flink ({TIME_MODE} time)...")
    flink_proc = subprocess.Popen(
        ["java", "-jar", flink_jar, TIME_MODE],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # Wait for both to connect
    print("[*] Waiting for systems to connect (5s)...")
    time.sleep(5)

    # Check processes
    if varpulis_proc.poll() is not None:
        print("[ERROR] Varpulis failed to start")
        out = varpulis_proc.stdout.read().decode()[:500]
        print(out)
        flink_proc.terminate()
        sys.exit(1)

    if flink_proc.poll() is not None:
        print("[ERROR] Flink failed to start")
        out = flink_proc.stdout.read().decode()[:500]
        print(out)
        varpulis_proc.terminate()
        sys.exit(1)

    # Publish events with real timestamps
    events = get_events_with_timestamps()
    print(f"\n[*] Publishing {len(events)} events...")
    collector.start_time = time.time() * 1000

    for i, event in enumerate(events):
        topic = f"{INPUT_TOPIC_PREFIX}{event['type']}"
        pub.publish(topic, json.dumps(event))
        print(f"  [{i+1:2}] {event['type']:12} user={event['user_id']}")
        time.sleep(0.1)

    pub_end = time.time() * 1000

    # Wait for results (7s to allow Flink watermark idleness to kick in)
    print("\n[*] Waiting for alerts (7s)...")
    time.sleep(7)

    # Cleanup
    sub.loop_stop()
    sub.disconnect()
    pub.disconnect()
    varpulis_proc.terminate()
    flink_proc.terminate()
    varpulis_proc.wait(timeout=3)
    flink_proc.wait(timeout=3)

    # Results
    print("\n" + "="*70)
    print(" RESULTS")
    print("="*70)

    v_users = sorted(set(u for u, _ in collector.varpulis))
    f_users = sorted(set(u for u, _ in collector.flink))

    print(f"\n Expected: {EXPECTED_USERS}")
    print(f" Varpulis: {v_users}")
    print(f" Flink:    {f_users}")

    print("\n VALIDATION:")
    v_ok = v_users == EXPECTED_USERS
    f_ok = f_users == EXPECTED_USERS
    print(f"   Varpulis: {'PASS' if v_ok else 'FAIL'} ({len(collector.varpulis)}/{len(EXPECTED_USERS)} alerts)")
    print(f"   Flink:    {'PASS' if f_ok else 'FAIL'} ({len(collector.flink)}/{len(EXPECTED_USERS)} alerts)")

    # Latencies
    if collector.varpulis:
        v_lats = [t - collector.start_time for _, t in collector.varpulis]
        print(f"\n Varpulis latencies: min={min(v_lats):.0f}ms, max={max(v_lats):.0f}ms, avg={sum(v_lats)/len(v_lats):.0f}ms")

    if collector.flink:
        f_lats = [t - collector.start_time for _, t in collector.flink]
        print(f" Flink latencies:    min={min(f_lats):.0f}ms, max={max(f_lats):.0f}ms, avg={sum(f_lats)/len(f_lats):.0f}ms")

    # Comparison
    if collector.varpulis and collector.flink:
        v_avg = sum(t - collector.start_time for _, t in collector.varpulis) / len(collector.varpulis)
        f_avg = sum(t - collector.start_time for _, t in collector.flink) / len(collector.flink)
        diff = f_avg - v_avg
        if diff > 0:
            print(f"\n => Varpulis is {diff:.0f}ms faster on average")
        else:
            print(f"\n => Flink is {-diff:.0f}ms faster on average")

    # Save results
    results = {
        "timestamp": datetime.now().isoformat(),
        "scenario": "Login -> FailedTransaction",
        "time_mode": TIME_MODE,
        "events_count": len(events),
        "expected_alerts": EXPECTED_USERS,
        "varpulis": {
            "alerts": v_users,
            "correct": v_ok,
            "latencies_ms": [t - collector.start_time for _, t in collector.varpulis]
        },
        "flink": {
            "alerts": f_users,
            "correct": f_ok,
            "latencies_ms": [t - collector.start_time for _, t in collector.flink]
        }
    }

    results_dir = "/home/cpo/cep/benchmarks/flink-comparison/results"
    os.makedirs(results_dir, exist_ok=True)
    results_file = f"{results_dir}/benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n Results saved: {results_file}")

    print("\n" + "="*70 + "\n")

    return 0 if (v_ok and f_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
