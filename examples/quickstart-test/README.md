# HVAC Quickstart Test

Test the `hvac_quickstart.vpl` example with a real MQTT broker.

## Prerequisites

- Docker and Docker Compose
- Varpulis CLI built (`cargo build --release`)

## Run the test

### 1. Start MQTT broker + event generator

```bash
cd examples/quickstart-test
docker compose up -d
```

This starts:
- **Mosquitto** MQTT broker on port 1883
- **Generator** that sends temperature/humidity events with ~10% anomalies

### 2. (Optional) Monitor MQTT traffic

```bash
# In another terminal - requires mosquitto-clients
mosquitto_sub -h localhost -t '#' -v
```

### 3. Run Varpulis

```bash
# From project root
./target/release/varpulis run --file examples/hvac_quickstart.vpl
```

You should see alerts when:
- Temperature > 28°C or < 16°C
- Humidity < 30% or > 70%
- Zone average temperature > 26°C (after 5 min window)

### 4. Monitor alerts

```bash
mosquitto_sub -h localhost -t 'alerts/#' -v
```

## Stop

```bash
docker compose down
```

## Expected output

The generator injects anomalies ~10% of the time. You should see output like:

```
[14:23:45] Sent 8 events total
  [ANOMALY] Zone_B: temp=31.2°C
[14:23:46] Sent 16 events total
```

Varpulis will display alerts as they're generated:

```
ALERT: stream_output - Output from stream HighTempAlert
ALERT: stream_output - Output from stream LowTempAlert
ALERT: stream_output - Output from stream HumidityAlert
Events: 65 | Alerts: 8 | Rate: 7/s
```

## Event format

The generator sends JSON events with a `type` field to identify the event type:

```json
{
  "type": "TemperatureReading",
  "sensor_id": "temp-zone_a",
  "zone": "Zone_A",
  "value": 22.5,
  "ts": "2026-02-02T10:30:00"
}
```

Varpulis uses this `type` field to route events to the correct streams.
