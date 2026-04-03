#!/usr/bin/env python3
"""
Varpulis → ThingsBoard Alarm Bridge

Subscribes to Varpulis CEP alarm output on Mosquitto and pushes them
to ThingsBoard as real alarms via the REST API.

This makes Varpulis alerts visible in the ThingsBoard Alarms dashboard.

Usage:
    python alarm-bridge.py

Environment variables:
    TB_URL:       ThingsBoard URL (default: http://localhost:18080)
    TB_USER:      ThingsBoard username (default: tenant@thingsboard.org)
    TB_PASSWORD:  ThingsBoard password (default: tenant)
    MQTT_HOST:    Mosquitto host (default: localhost)
    MQTT_PORT:    Mosquitto port (default: 11884)
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

# ── Configuration ────────────────────────────────────────────────

TB_URL = os.environ.get("TB_URL", "http://localhost:18080")
TB_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASSWORD = os.environ.get("TB_PASSWORD", "tenant")
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "11884"))
ALARM_TOPIC = "tb/cep/alarms"

# Severity mapping: Varpulis → ThingsBoard
SEVERITY_MAP = {
    "CRITICAL": "CRITICAL",
    "MAJOR": "MAJOR",
    "MINOR": "MINOR",
    "WARNING": "WARNING",
    "critical": "CRITICAL",
    "major": "MAJOR",
    "minor": "MINOR",
    "warning": "WARNING",
}

# ── ThingsBoard API client ───────────────────────────────────────

class ThingsBoardClient:
    def __init__(self, url, user, password):
        self.url = url.rstrip("/")
        self.token = None
        self.token_expiry = 0
        self.user = user
        self.password = password
        self.device_cache = {}  # deviceName → device_id

    def _request(self, method, path, data=None, auth=True):
        url = f"{self.url}{path}"
        headers = {"Content-Type": "application/json"}
        if auth and self.token:
            headers["X-Authorization"] = f"Bearer {self.token}"

        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else ""
            print(f"  TB API error {e.code}: {error_body[:200]}")
            return None

    def login(self):
        resp = self._request("POST", "/api/auth/login",
                             {"username": self.user, "password": self.password},
                             auth=False)
        if resp and "token" in resp:
            self.token = resp["token"]
            self.token_expiry = time.time() + 800  # refresh before 900s expiry
            print(f"  ThingsBoard authenticated as {self.user}")
            return True
        print(f"  ThingsBoard auth failed!")
        return False

    def ensure_auth(self):
        if time.time() > self.token_expiry:
            self.login()

    def find_device_id(self, device_name):
        if device_name in self.device_cache:
            return self.device_cache[device_name]

        self.ensure_auth()
        resp = self._request("GET",
            f"/api/tenant/devices?deviceName={urllib.request.quote(device_name)}")
        if resp and "id" in resp:
            dev_id = resp["id"]["id"]
            self.device_cache[device_name] = dev_id
            return dev_id
        return None

    def create_alarm(self, device_name, alarm_type, severity, message, details=None):
        self.ensure_auth()

        device_id = self.find_device_id(device_name)
        if not device_id:
            print(f"  Device '{device_name}' not found in TB, skipping alarm")
            return False

        alarm = {
            "originator": {
                "entityType": "DEVICE",
                "id": device_id
            },
            "type": alarm_type,
            "severity": SEVERITY_MAP.get(severity, "WARNING"),
            "status": "ACTIVE_UNACK",
            "details": details or {}
        }

        resp = self._request("POST", "/api/alarm", alarm)
        if resp and "id" in resp:
            alarm_id = resp["id"]["id"]
            print(f"  ALARM created: {alarm_type} ({severity}) on {device_name} → {alarm_id[:8]}...")
            return True
        return False


# ── MQTT subscriber ──────────────────────────────────────────────

def on_alarm_message(tb_client, payload_str):
    """Process a Varpulis CEP alarm and push it to ThingsBoard."""
    try:
        data = json.loads(payload_str)
    except json.JSONDecodeError:
        return

    # Extract fields from Varpulis alert
    device_name = data.get("deviceName") or data.get("hot_device")
    alarm_type = data.get("alert_type", data.get("event_type", "CEP_ALERT"))
    severity = data.get("severity", "WARNING")
    message = data.get("message", "")

    if not device_name:
        return

    # Build details from remaining fields
    details = {k: v for k, v in data.items()
               if k not in ("event_type", "timestamp", "deviceName", "deviceType",
                            "alert_type", "severity", "message")}
    details["source"] = "varpulis-cep"
    details["original_message"] = message

    tb_client.create_alarm(device_name, alarm_type, severity, message, details)


def run_with_mosquitto_sub():
    """Fallback: use mosquitto_sub subprocess if paho-mqtt not available."""
    import subprocess

    tb = ThingsBoardClient(TB_URL, TB_USER, TB_PASSWORD)
    if not tb.login():
        sys.exit(1)

    print(f"\nListening for Varpulis alarms on mqtt://{MQTT_HOST}:{MQTT_PORT}/{ALARM_TOPIC}")
    print("Press Ctrl+C to stop.\n")

    cmd = ["mosquitto_sub", "-h", MQTT_HOST, "-p", str(MQTT_PORT), "-t", ALARM_TOPIC]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

    try:
        for line in proc.stdout:
            line = line.strip()
            if line:
                on_alarm_message(tb, line)
    except KeyboardInterrupt:
        proc.terminate()


def run_with_paho():
    """Primary: use paho-mqtt for reliable MQTT subscription."""
    import paho.mqtt.client as mqtt

    tb = ThingsBoardClient(TB_URL, TB_USER, TB_PASSWORD)
    if not tb.login():
        sys.exit(1)

    def on_connect(client, userdata, flags, rc, *args):
        if rc == 0 or rc == mqtt.CONNACK_ACCEPTED if hasattr(mqtt, 'CONNACK_ACCEPTED') else rc == 0:
            client.subscribe(ALARM_TOPIC)
            print(f"\nSubscribed to {ALARM_TOPIC}")
        else:
            print(f"MQTT connect failed: rc={rc}")

    def on_message(client, userdata, msg):
        on_alarm_message(tb, msg.payload.decode())

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="tb-alarm-bridge")
    except (AttributeError, TypeError):
        client = mqtt.Client(client_id="tb-alarm-bridge")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)

    print(f"\nListening for Varpulis alarms on mqtt://{MQTT_HOST}:{MQTT_PORT}/{ALARM_TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()


if __name__ == "__main__":
    try:
        import paho.mqtt.client
        run_with_paho()
    except ImportError:
        print("paho-mqtt not found, using mosquitto_sub fallback")
        run_with_mosquitto_sub()
