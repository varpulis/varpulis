#!/usr/bin/env python3
"""Generate simulated HVAC temperature readings over MQTT."""

import json
import random
import time
import sys

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Install paho-mqtt: pip install paho-mqtt")
    sys.exit(1)

BROKER = "localhost"
PORT = 1883
TOPIC_PREFIX = "sensors/temp"

ZONES = ["zone-a", "zone-b", "zone-c"]
SENSORS_PER_ZONE = 2

# Normal range: 20-26 C, spike to 32-38 C
NORMAL_RANGE = (20.0, 26.0)
SPIKE_RANGE = (32.0, 38.0)
SPIKE_PROBABILITY = 0.05  # 5% chance of spike per reading


def generate_reading(zone: str, sensor_idx: int) -> dict:
    """Generate a single temperature reading."""
    if random.random() < SPIKE_PROBABILITY:
        value = round(random.uniform(*SPIKE_RANGE), 1)
    else:
        value = round(random.uniform(*NORMAL_RANGE), 1)

    return {
        "sensor_id": f"{zone}-sensor-{sensor_idx}",
        "zone": zone,
        "value": value,
        "ts": int(time.time() * 1_000_000_000),  # nanoseconds
    }


def main():
    client = mqtt.Client(client_id="hvac-generator")
    client.connect(BROKER, PORT)
    client.loop_start()

    print(f"Publishing to {BROKER}:{PORT}/{TOPIC_PREFIX}/...")
    print("Press Ctrl+C to stop\n")

    count = 0
    try:
        while True:
            for zone in ZONES:
                for sensor_idx in range(SENSORS_PER_ZONE):
                    reading = generate_reading(zone, sensor_idx)
                    topic = f"{TOPIC_PREFIX}/{zone}/{reading['sensor_id']}"
                    payload = json.dumps(reading)
                    client.publish(topic, payload)
                    count += 1

            if count % 30 == 0:
                print(f"  Published {count} readings")

            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nStopped after {count} readings")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
