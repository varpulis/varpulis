# Use Case: HVAC Building Supervision

## Overview

Smart building supervision with real-time monitoring, anomaly detection, and **attention-based predictive maintenance** for HVAC equipment.

## Business Context

A smart building equipped with IoT sensors to monitor:
- **Temperature** by zone (offices, server room, reception)
- **Humidity** by zone
- **HVAC equipment status** (AHU, air conditioning)
- **Energy consumption**

### Objectives

1. **Temperature anomaly detection** (overheating, under-cooling)
2. **Temperature/HVAC correlation** (is the equipment responding correctly?)
3. **Predictive maintenance** using attention-based degradation detection
4. **Comfort scoring** by zone
5. **Energy optimization**

## Building Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ALPHA BUILDING                        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   Zone A    │  │   Zone B    │  │   Zone C    │     │
│  │  (Offices)  │  │(Server Room)│  │ (Reception) │     │
│  │             │  │             │  │             │     │
│  │ T: 21-23°C  │  │ T: 18-20°C  │  │ T: 20-24°C  │     │
│  │ H: 40-60%   │  │ H: 45-55%   │  │ H: 40-60%   │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
│         │                │                │             │
│         └────────────────┼────────────────┘             │
│                          │                              │
│                   ┌──────▼──────┐                       │
│                   │    HVAC     │                       │
│                   │   Central   │                       │
│                   └─────────────┘                       │
└─────────────────────────────────────────────────────────┘
```

## Alert Types

```
┌─────────────────────────────────────────────────────────────┐
│                      HVAC ALERTS                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ENVIRONMENTAL                                              │
│  ├── TEMPERATURE_ANOMALY    - Out of range temperature     │
│  ├── SERVER_ROOM_CRITICAL   - Critical server room temp    │
│  └── HUMIDITY_ANOMALY       - Out of range humidity        │
│                                                             │
│  EQUIPMENT                                                  │
│  ├── HVAC_POWER_SPIKE       - Unusual power consumption    │
│  ├── COMPRESSOR_DEGRADATION - Progressive wear detected    │
│  ├── REFRIGERANT_LEAK       - Possible leak detected       │
│  └── FAN_MOTOR_DEGRADATION  - Fan motor issues             │
│                                                             │
│  MAINTENANCE                                                │
│  ├── MAINTENANCE_REMINDER   - Runtime-based reminder       │
│  └── LOW_HEALTH_SCORE       - Overall health below 70%     │
│                                                             │
│  ENERGY                                                     │
│  └── ENERGY_ANOMALY         - Unusual energy consumption   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Attention-Based Degradation Detection

The key innovation in this example is using the **attention mechanism** to detect progressive equipment degradation that simple threshold-based rules would miss.

### Compressor Degradation Pattern

```varpulis
stream CompressorDegradation = HVAC
    .partition_by(unit_id)
    .attention_window(duration: 1h, heads: 4, embedding: "rule_based")
    .pattern(
        degradation: events =>
            # Calculate trends
            let pressure_trend = linear_regression_slope(events.map(e => e.compressor_pressure))
            let power_trend = linear_regression_slope(events.map(e => e.power_consumption))
            
            # Calculate attention scores for correlation
            let attention_scores = events.sliding_pairs()
                .map((e1, e2) => attention_score(e1, e2))
            
            # Pattern: high correlation + declining pressure + increasing power
            let high_correlation = attention_scores.filter(s => s > 0.8).count() > events.len() * 0.7
            
            high_correlation and pressure_trend < -0.01 and power_trend > 0.05
    )
```

### Why Attention Works

| Traditional Rule-Based | Attention-Based |
|------------------------|-----------------|
| Fixed thresholds | Learns normal patterns |
| Misses gradual drift | Detects progressive changes |
| High false positives | Correlates multiple signals |
| Per-metric analysis | Holistic event comparison |

## Health Score Calculation

Each HVAC unit gets a real-time health score (0-100):

```varpulis
health_score = 100
    - (if avg_power > 5.0 then 10 else 0)           # Power consumption penalty
    - (if pressure out of range then 15 else 0)     # Pressure penalty
    - (if refrigerant_temp > 50 then 20 else 0)     # Temperature penalty
    - (if runtime_hours > 5000 then 10 else 0)      # Age penalty
```

## Example Output

```json
{
  "alert_type": "COMPRESSOR_DEGRADATION",
  "severity": "warning",
  "unit_id": "hvac_unit_01",
  "zone": "server_room",
  "confidence": 0.75,
  "recommendation": "Schedule preventive maintenance - compressor showing signs of wear",
  "reason": "Progressive decline in compressor pressure with increasing power consumption",
  "timestamp": "2026-01-23T01:20:00Z"
}
```

## Running the Example

```bash
# Check syntax
varpulis check examples/hvac_demo.vpl

# Run demo with built-in simulator
varpulis demo --duration 60 --anomalies --degradation --metrics

# Run with custom data source
varpulis run examples/hvac_demo.vpl \
  --source kafka://building-sensors \
  --sink kafka://hvac-alerts
```

## Metrics Exposed

| Metric | Description |
|--------|-------------|
| `hvac_alerts_total{type,severity,zone}` | Alert counter |
| `hvac_health_score{unit_id}` | Current health score |
| `hvac_comfort_index{zone}` | Comfort index per zone |
| `hvac_energy_kw{zone}` | Energy consumption |

## See Also

- [Financial Markets Example](financial-markets.md)
- [Attention Engine Architecture](../architecture/attention-engine.md)
- [VarpulisQL Syntax](../language/syntax.md)
