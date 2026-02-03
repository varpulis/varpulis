#!/usr/bin/env python3
"""
Simple HVAC event generator for quickstart demo.
Generates temperature and humidity readings with occasional anomalies.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime

import paho.mqtt.client as mqtt

# Unbuffered output for Docker logs
sys.stdout.reconfigure(line_buffering=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second")
    args = parser.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="hvac-generator")

    print(f"Connecting to MQTT broker at {args.broker}:{args.port}...")
    client.connect(args.broker, args.port, 60)
    client.loop_start()
    print("Connected! Generating events...")

    zones = ["Zone_A", "Zone_B", "Zone_C", "server_room"]
    event_count = 0

    # Base values per zone
    base_temp = {"Zone_A": 22.0, "Zone_B": 21.5, "Zone_C": 22.5, "server_room": 19.0}
    base_humidity = {"Zone_A": 45, "Zone_B": 50, "Zone_C": 48, "server_room": 40}

    try:
        while True:
            for zone in zones:
                # Temperature reading
                temp = base_temp[zone] + random.gauss(0, 0.5)

                # Inject anomaly ~10% of time
                if random.random() < 0.1:
                    temp += random.choice([-8, 10])  # Cold or hot spike
                    print(f"  [ANOMALY] {zone}: temp={temp:.1f}°C")

                temp_event = {
                    "type": "TemperatureReading",
                    "sensor_id": f"temp-{zone.lower()}",
                    "zone": zone,
                    "value": round(temp, 1),
                    "ts": datetime.now().isoformat()
                }
                client.publish("sensors/temperature", json.dumps(temp_event))

                # Humidity reading
                humidity = base_humidity[zone] + random.gauss(0, 3)

                # Inject anomaly ~5% of time
                if random.random() < 0.05:
                    humidity = random.choice([15, 85])  # Too dry or too humid
                    print(f"  [ANOMALY] {zone}: humidity={humidity}%")

                humidity_event = {
                    "type": "HumidityReading",
                    "sensor_id": f"hum-{zone.lower()}",
                    "zone": zone,
                    "value": round(humidity, 1),
                    "ts": datetime.now().isoformat()
                }
                client.publish("sensors/humidity", json.dumps(humidity_event))

                event_count += 2

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {event_count} events total")
            time.sleep(1.0 / args.rate)

    except KeyboardInterrupt:
        print(f"\nStopped. Total events: {event_count}")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
