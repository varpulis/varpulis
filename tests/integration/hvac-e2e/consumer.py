#!/usr/bin/env python3
"""HVAC alert consumer/validator for E2E integration test.

Reads from Kafka topic 'hvac-alerts' and validates:
- Exactly 5 events (3 HIGH_TEMPERATURE + 2 LOW_TEMPERATURE)
- Each event has required fields
- Temperature thresholds are correct
"""

import json
import os
import sys
import time

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "hvac-alerts")
TIMEOUT_SECONDS = 60
EXPECTED_HIGH = 3
EXPECTED_LOW = 2
EXPECTED_TOTAL = EXPECTED_HIGH + EXPECTED_LOW


def main():
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "hvac-test-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 10000,
    })

    consumer.subscribe([KAFKA_TOPIC])
    print(f"Subscribed to {KAFKA_TOPIC} at {KAFKA_BOOTSTRAP}")
    print(f"Waiting up to {TIMEOUT_SECONDS}s for {EXPECTED_TOTAL} events...")

    received = []
    start = time.time()

    while len(received) < EXPECTED_TOTAL and (time.time() - start) < TIMEOUT_SECONDS:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {msg.error()}", file=sys.stderr)
            continue

        try:
            value = json.loads(msg.value().decode("utf-8"))
            received.append(value)
            print(f"  Received [{len(received)}/{EXPECTED_TOTAL}]: {json.dumps(value, indent=None)}")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  Failed to decode message: {e}", file=sys.stderr)

    consumer.close()

    elapsed = time.time() - start
    print(f"\nReceived {len(received)} events in {elapsed:.1f}s")

    # === Validation ===
    errors = []

    if len(received) != EXPECTED_TOTAL:
        errors.append(f"Expected {EXPECTED_TOTAL} events, got {len(received)}")

    high_alerts = []
    low_alerts = []

    for i, event in enumerate(received):
        # Check required top-level field
        if "event_type" not in event:
            errors.append(f"Event {i}: missing 'event_type'")
            continue

        # Events from .emit() come with data fields at top level
        data = event.get("data", event)

        alert_type = data.get("alert_type")
        if alert_type is None:
            errors.append(f"Event {i}: missing 'alert_type'")
            continue

        # Validate required fields
        for field in ["zone", "sensor", "temperature"]:
            if field not in data:
                errors.append(f"Event {i}: missing '{field}'")

        temp = data.get("temperature")
        if temp is None:
            continue

        if alert_type == "HIGH_TEMPERATURE":
            high_alerts.append(event)
            if temp <= 28:
                errors.append(
                    f"Event {i}: HIGH_TEMPERATURE but temperature={temp} (should be >28)"
                )
        elif alert_type == "LOW_TEMPERATURE":
            low_alerts.append(event)
            if temp >= 16:
                errors.append(
                    f"Event {i}: LOW_TEMPERATURE but temperature={temp} (should be <16)"
                )
        else:
            errors.append(f"Event {i}: unexpected alert_type '{alert_type}'")

    if len(high_alerts) != EXPECTED_HIGH:
        errors.append(
            f"Expected {EXPECTED_HIGH} HIGH_TEMPERATURE alerts, got {len(high_alerts)}"
        )

    if len(low_alerts) != EXPECTED_LOW:
        errors.append(
            f"Expected {EXPECTED_LOW} LOW_TEMPERATURE alerts, got {len(low_alerts)}"
        )

    # === Report ===
    if errors:
        print("\n=== VALIDATION FAILED ===")
        for err in errors:
            print(f"  ERROR: {err}")
        print(f"\nReceived events dump:")
        for i, ev in enumerate(received):
            print(f"  [{i}] {json.dumps(ev)}")
        sys.exit(1)
    else:
        print("\n=== VALIDATION PASSED ===")
        print(f"  HIGH_TEMPERATURE alerts: {len(high_alerts)}")
        print(f"  LOW_TEMPERATURE alerts:  {len(low_alerts)}")
        print(f"  Total events validated:  {len(received)}")
        sys.exit(0)


if __name__ == "__main__":
    main()
