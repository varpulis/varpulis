# IoT HVAC Monitoring Starter

Real-time temperature monitoring with Varpulis and MQTT.

## What it does

- Ingests temperature readings from 3 zones via MQTT
- Alerts when any sensor exceeds 30 C
- Computes 5-minute rolling averages per zone
- Alerts when a zone's average exceeds 28 C

## Quick Start

### 1. Start the stack

```bash
docker compose up -d
```

This starts Mosquitto (MQTT broker) and Varpulis with the pipeline.

### 2. Generate test data

```bash
pip install paho-mqtt
python generate.py
```

### 3. Watch the output

```bash
docker compose logs -f varpulis
```

You'll see `HIGH_TEMPERATURE` alerts on spikes and `ZONE_OVERHEATING` alerts when zone averages climb.

## Files

| File | Description |
|------|-------------|
| `pipeline.vpl` | Varpulis pipeline definition |
| `docker-compose.yml` | Mosquitto + Varpulis services |
| `generate.py` | MQTT event generator (Python) |

## Next steps

- Edit `pipeline.vpl` to change thresholds or add patterns
- Add a Kafka sink: `.to(KafkaConnector, topic: "alerts")`
- Add sequence detection for rapid temperature swings
- See [full HVAC example](../../examples/hvac_quickstart.vpl) for more patterns
- Read the [VPL documentation](../../docs/)
