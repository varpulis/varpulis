# IoT Sensor Monitoring Demo

Real-time IoT sensor anomaly detection using Varpulis.

## Quick Start

```bash
docker-compose up
```

Then open:
- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Varpulis API**: http://localhost:9000/health

## Patterns Detected

| Pattern | Description | VPL |
|---------|-------------|-----|
| Temperature Spike | Single reading > 50C | Filter |
| Temperature Drift | Window avg > 35C over 10 readings | Window + aggregate |
| High Humidity | Humidity > 80% | Filter |

## Architecture

```
[IoT Generator] --readings--> [Varpulis Engine] --alerts--> [Grafana]
  (8 sensors)                   (iot_pipeline.vpl)           (dashboard)
```

## Sensor Zones

- `zone_a` through `zone_d` — 2 sensors per zone
- ~3% anomaly injection rate (temperature spikes)

## Customization

Edit `iot_pipeline.vpl` to adjust thresholds or add patterns.

```bash
# Higher sensor rate
docker-compose run generator generate --schema iot --rate 500 --duration 60s
```
