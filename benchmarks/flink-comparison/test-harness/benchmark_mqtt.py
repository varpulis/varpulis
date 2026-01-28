#!/usr/bin/env python3
"""
MQTT-based benchmark comparison between Varpulis and Flink.

This script:
1. Publishes test events to MQTT input topics
2. Collects alerts from both Varpulis and Flink output topics
3. Compares the results and measures performance

Usage:
    # First, start the MQTT broker (e.g., mosquitto)
    # Then start Varpulis:
    #   cargo run --features mqtt -- --run scenario2_mqtt.vpl
    # Then start Flink:
    #   mvn exec:java -Dexec.mainClass="com.benchmark.flink.Scenario2FlinkMqtt"
    # Finally run this benchmark:
    python benchmark_mqtt.py --scenario scenario2
"""

import argparse
import json
import time
import threading
from datetime import datetime
from collections import defaultdict

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt not installed. Run: pip install paho-mqtt")
    exit(1)

# MQTT Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

# Topics
INPUT_TOPIC_PREFIX = "benchmark/input/"
VARPULIS_OUTPUT_TOPIC = "benchmark/output/varpulis"
FLINK_OUTPUT_TOPIC = "benchmark/output/flink"

# Test data for Scenario 2: Login -> FailedTransaction
SCENARIO2_EVENTS = [
    {"type": "Login", "user_id": "user1", "ip_address": "192.168.1.1", "device": "mobile", "ts": 1000},
    {"type": "Login", "user_id": "user2", "ip_address": "192.168.1.2", "device": "desktop", "ts": 2000},
    {"type": "Transaction", "user_id": "user1", "amount": 500.0, "status": "failed", "merchant": "store_a", "ts": 3000},
    {"type": "Transaction", "user_id": "user3", "amount": 100.0, "status": "success", "merchant": "store_b", "ts": 4000},
    {"type": "Login", "user_id": "user3", "ip_address": "192.168.1.3", "device": "tablet", "ts": 5000},
    {"type": "Transaction", "user_id": "user2", "amount": 1500.0, "status": "failed", "merchant": "store_c", "ts": 6000},
    {"type": "Transaction", "user_id": "user3", "amount": 200.0, "status": "failed", "merchant": "store_d", "ts": 7000},
    {"type": "Login", "user_id": "user4", "ip_address": "192.168.1.4", "device": "mobile", "ts": 8000},
    {"type": "Transaction", "user_id": "user4", "amount": 50.0, "status": "success", "merchant": "store_e", "ts": 9000},
    {"type": "Transaction", "user_id": "user4", "amount": 2000.0, "status": "failed", "merchant": "store_f", "ts": 10000},
]

# Expected alerts for Scenario 2
# Pattern: Login followed by failed Transaction (same user) within 10 minutes
SCENARIO2_EXPECTED_ALERTS = [
    {"user_id": "user1", "failed_amount": 500.0, "severity": "medium", "merchant": "store_a"},
    {"user_id": "user2", "failed_amount": 1500.0, "severity": "high", "merchant": "store_c"},
    {"user_id": "user3", "failed_amount": 200.0, "severity": "medium", "merchant": "store_d"},
    {"user_id": "user4", "failed_amount": 2000.0, "severity": "high", "merchant": "store_f"},
]


class BenchmarkCollector:
    """Collects alerts from both Varpulis and Flink via MQTT."""

    def __init__(self):
        self.varpulis_alerts = []
        self.flink_alerts = []
        self.varpulis_times = []
        self.flink_times = []
        self.event_send_times = {}  # Maps event index to send timestamp
        self.lock = threading.Lock()

    def on_varpulis_alert(self, alert, receive_time):
        with self.lock:
            self.varpulis_alerts.append(alert)
            self.varpulis_times.append(receive_time)

    def on_flink_alert(self, alert, receive_time):
        with self.lock:
            self.flink_alerts.append(alert)
            self.flink_times.append(receive_time)

    def record_event_sent(self, idx, send_time):
        with self.lock:
            self.event_send_times[idx] = send_time


def normalize_alert(alert):
    """Normalize alert fields for comparison (handle different field naming)."""
    user_id = alert.get("user_id") or alert.get("userId")
    failed_amount = alert.get("failed_amount") or alert.get("failedAmount")
    severity = alert.get("severity")
    merchant = alert.get("merchant")

    return {
        "user_id": user_id,
        "failed_amount": float(failed_amount) if failed_amount else 0.0,
        "severity": severity,
        "merchant": merchant,
    }


def alerts_match(expected, actual):
    """Check if an actual alert matches expected values."""
    norm = normalize_alert(actual)
    return (
        norm["user_id"] == expected["user_id"] and
        abs(norm["failed_amount"] - expected["failed_amount"]) < 0.01 and
        norm["severity"] == expected["severity"] and
        norm["merchant"] == expected["merchant"]
    )


def run_benchmark(scenario: str, delay_ms: int = 100, wait_seconds: int = 10):
    """Run the MQTT benchmark for the specified scenario."""

    if scenario != "scenario2":
        print(f"Unknown scenario: {scenario}")
        return

    events = SCENARIO2_EVENTS
    expected = SCENARIO2_EXPECTED_ALERTS

    collector = BenchmarkCollector()

    # Setup MQTT subscriber for outputs
    def on_message(client, userdata, msg):
        receive_time = time.time() * 1000  # ms
        try:
            alert = json.loads(msg.payload.decode())
            if msg.topic == VARPULIS_OUTPUT_TOPIC:
                collector.on_varpulis_alert(alert, receive_time)
                print(f"  [VARPULIS] Alert received: {normalize_alert(alert).get('user_id', 'N/A')}")
            elif msg.topic == FLINK_OUTPUT_TOPIC:
                collector.on_flink_alert(alert, receive_time)
                print(f"  [FLINK]    Alert received: {normalize_alert(alert).get('user_id', 'N/A')}")
        except json.JSONDecodeError as e:
            print(f"  Error decoding message: {e}")

    # Connect subscriber
    sub_client = mqtt.Client(client_id="benchmark_subscriber", protocol=mqtt.MQTTv311)
    sub_client.on_message = on_message

    try:
        sub_client.connect(MQTT_BROKER, MQTT_PORT)
    except Exception as e:
        print(f"\nERROR: Could not connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        print(f"       {e}")
        print("\nMake sure an MQTT broker (e.g., mosquitto) is running.")
        return

    sub_client.subscribe(VARPULIS_OUTPUT_TOPIC)
    sub_client.subscribe(FLINK_OUTPUT_TOPIC)
    sub_client.loop_start()

    # Connect publisher
    pub_client = mqtt.Client(client_id="benchmark_publisher", protocol=mqtt.MQTTv311)
    pub_client.connect(MQTT_BROKER, MQTT_PORT)

    print(f"\n{'='*70}")
    print(f" MQTT Benchmark: {scenario}")
    print(f"{'='*70}")
    print(f" Events to send:     {len(events)}")
    print(f" Expected alerts:    {len(expected)}")
    print(f" Delay between:      {delay_ms}ms")
    print(f" Wait for results:   {wait_seconds}s")
    print(f"{'='*70}")
    print(f"\n NOTE: Make sure both Varpulis and Flink are running and connected:")
    print(f"   Varpulis: cargo run --features mqtt -- --run scenario2_mqtt.vpl")
    print(f"   Flink:    mvn exec:java -Dexec.mainClass=\"com.benchmark.flink.Scenario2FlinkMqtt\"")
    print(f"\n{'='*70}\n")

    input("Press Enter when both systems are ready...")

    # Publish events
    print("\nPublishing events...")
    start_time = time.time() * 1000

    for i, event in enumerate(events):
        topic = f"{INPUT_TOPIC_PREFIX}{event['type']}"
        payload = json.dumps(event)
        send_time = time.time() * 1000
        pub_client.publish(topic, payload)
        collector.record_event_sent(i, send_time)
        print(f"  [{i+1:2}/{len(events)}] {event['type']:12} -> {event.get('user_id', 'N/A')}")

        if delay_ms > 0 and i < len(events) - 1:
            time.sleep(delay_ms / 1000)

    publish_end_time = time.time() * 1000

    # Wait for all alerts to arrive
    print(f"\nWaiting for alerts ({wait_seconds} seconds)...")
    time.sleep(wait_seconds)

    # Stop subscriber
    sub_client.loop_stop()
    sub_client.disconnect()
    pub_client.disconnect()

    # Results
    print(f"\n{'='*70}")
    print(" RESULTS")
    print(f"{'='*70}")

    print(f"\n Varpulis alerts received: {len(collector.varpulis_alerts)}")
    for alert in collector.varpulis_alerts:
        norm = normalize_alert(alert)
        print(f"   - user={norm['user_id']}, amount={norm['failed_amount']}, severity={norm['severity']}, merchant={norm['merchant']}")

    print(f"\n Flink alerts received: {len(collector.flink_alerts)}")
    for alert in collector.flink_alerts:
        norm = normalize_alert(alert)
        print(f"   - user={norm['user_id']}, amount={norm['failed_amount']}, severity={norm['severity']}, merchant={norm['merchant']}")

    # Validate results
    print(f"\n{'='*70}")
    print(" VALIDATION")
    print(f"{'='*70}")

    def validate_alerts(name, alerts, expected):
        matched = []
        unmatched_expected = list(expected)
        extra = []

        for alert in alerts:
            found = False
            for exp in unmatched_expected:
                if alerts_match(exp, alert):
                    matched.append(exp)
                    unmatched_expected.remove(exp)
                    found = True
                    break
            if not found:
                extra.append(normalize_alert(alert))

        return matched, unmatched_expected, extra

    v_matched, v_missing, v_extra = validate_alerts("Varpulis", collector.varpulis_alerts, expected)
    f_matched, f_missing, f_extra = validate_alerts("Flink", collector.flink_alerts, expected)

    print(f"\n Expected alerts: {len(expected)}")
    for exp in expected:
        print(f"   - user={exp['user_id']}, amount={exp['failed_amount']}, severity={exp['severity']}")

    print(f"\n Varpulis validation:")
    print(f"   Matched:  {len(v_matched)}/{len(expected)}")
    if v_missing:
        print(f"   Missing:  {[e['user_id'] for e in v_missing]}")
    if v_extra:
        print(f"   Extra:    {[e['user_id'] for e in v_extra]}")

    if len(v_matched) == len(expected) and not v_extra:
        print(f"   Status:   PASS")
    else:
        print(f"   Status:   FAIL")

    print(f"\n Flink validation:")
    print(f"   Matched:  {len(f_matched)}/{len(expected)}")
    if f_missing:
        print(f"   Missing:  {[e['user_id'] for e in f_missing]}")
    if f_extra:
        print(f"   Extra:    {[e['user_id'] for e in f_extra]}")

    if len(f_matched) == len(expected) and not f_extra:
        print(f"   Status:   PASS")
    else:
        print(f"   Status:   FAIL")

    # Compare Varpulis vs Flink
    print(f"\n{'='*70}")
    print(" COMPARISON: Varpulis vs Flink")
    print(f"{'='*70}")

    varpulis_users = {normalize_alert(a)['user_id'] for a in collector.varpulis_alerts}
    flink_users = {normalize_alert(a)['user_id'] for a in collector.flink_alerts}

    if varpulis_users == flink_users:
        print(f"\n   BOTH SYSTEMS PRODUCE IDENTICAL RESULTS")
        print(f"   Users detected: {sorted(varpulis_users)}")
    else:
        print(f"\n   SYSTEMS PRODUCE DIFFERENT RESULTS")
        only_varpulis = varpulis_users - flink_users
        only_flink = flink_users - varpulis_users
        common = varpulis_users & flink_users

        if common:
            print(f"   Common:         {sorted(common)}")
        if only_varpulis:
            print(f"   Only Varpulis:  {sorted(only_varpulis)}")
        if only_flink:
            print(f"   Only Flink:     {sorted(only_flink)}")

    # Performance metrics
    if collector.varpulis_times or collector.flink_times:
        print(f"\n{'='*70}")
        print(" PERFORMANCE")
        print(f"{'='*70}")

        print(f"\n Total event publish time: {publish_end_time - start_time:.1f}ms")

        if collector.varpulis_times:
            varpulis_latencies = [t - start_time for t in collector.varpulis_times]
            print(f"\n Varpulis latencies (from first event to alert):")
            for i, lat in enumerate(varpulis_latencies):
                user = normalize_alert(collector.varpulis_alerts[i])['user_id']
                print(f"   {user}: {lat:.1f}ms")
            avg_v = sum(varpulis_latencies) / len(varpulis_latencies)
            print(f"   Average: {avg_v:.1f}ms")

        if collector.flink_times:
            flink_latencies = [t - start_time for t in collector.flink_times]
            print(f"\n Flink latencies:")
            for i, lat in enumerate(flink_latencies):
                user = normalize_alert(collector.flink_alerts[i])['user_id']
                print(f"   {user}: {lat:.1f}ms")
            avg_f = sum(flink_latencies) / len(flink_latencies)
            print(f"   Average: {avg_f:.1f}ms")

        if collector.varpulis_times and collector.flink_times:
            avg_v = sum([t - start_time for t in collector.varpulis_times]) / len(collector.varpulis_times)
            avg_f = sum([t - start_time for t in collector.flink_times]) / len(collector.flink_times)
            diff = avg_f - avg_v
            if diff > 0:
                print(f"\n   Varpulis is {diff:.1f}ms faster on average")
            else:
                print(f"\n   Flink is {-diff:.1f}ms faster on average")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT benchmark for Varpulis vs Flink")
    parser.add_argument("--scenario", default="scenario2", help="Scenario to run")
    parser.add_argument("--delay", type=int, default=100, help="Delay between events in ms")
    parser.add_argument("--wait", type=int, default=10, help="Seconds to wait for results")
    args = parser.parse_args()

    run_benchmark(args.scenario, args.delay, args.wait)
