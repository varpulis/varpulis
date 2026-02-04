#!/usr/bin/env python3
"""HVAC temperature sensor producer for E2E integration test.

Publishes a deterministic sequence of temperature readings to MQTT:
- 5 normal readings (20-25C)  -> no alerts expected
- 3 high readings (30,33,35C) -> 3 HIGH_TEMPERATURE alerts expected
- 2 low readings (12,14C)     -> 2 LOW_TEMPERATURE alerts expected
"""

import json
import os
import sys
import time
import socket

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TOPIC = "varpulis/events/TemperatureReading"

# Deterministic event sequence
EVENTS = [
    # Normal readings (no alerts)
    {"sensor_id": "S1", "zone": "A", "value": 22.0},
    {"sensor_id": "S2", "zone": "B", "value": 21.5},
    {"sensor_id": "S1", "zone": "A", "value": 23.0},
    {"sensor_id": "S3", "zone": "C", "value": 20.0},
    {"sensor_id": "S2", "zone": "B", "value": 25.0},
    # High readings (3 alerts)
    {"sensor_id": "S1", "zone": "A", "value": 30.0},
    {"sensor_id": "S2", "zone": "B", "value": 33.0},
    {"sensor_id": "S3", "zone": "C", "value": 35.0},
    # Low readings (2 alerts)
    {"sensor_id": "S1", "zone": "A", "value": 12.0},
    {"sensor_id": "S2", "zone": "B", "value": 14.0},
]


def wait_for_mqtt(host, port, timeout=30):
    """Wait until MQTT broker is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.create_connection((host, port), timeout=2)
            sock.close()
            return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(1)
    return False


def main():
    # Install paho-mqtt at runtime
    import subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", "paho-mqtt>=2.0.0"],
        stdout=subprocess.DEVNULL,
    )

    import paho.mqtt.client as mqtt

    print(f"Waiting for MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
    if not wait_for_mqtt(MQTT_HOST, MQTT_PORT):
        print("ERROR: MQTT broker not available", file=sys.stderr)
        sys.exit(1)

    # paho-mqtt v2 API
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="hvac-producer")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    print(f"Connected to MQTT. Publishing {len(EVENTS)} events to {TOPIC}")

    for i, event_data in enumerate(EVENTS):
        payload = {
            "event_type": "TemperatureReading",
            **event_data,
        }
        msg = json.dumps(payload)
        result = client.publish(TOPIC, msg, qos=1)
        result.wait_for_publish()
        print(f"  [{i+1}/{len(EVENTS)}] sensor={event_data['sensor_id']} "
              f"zone={event_data['zone']} value={event_data['value']}")
        time.sleep(0.2)

    print("All events published.")
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
