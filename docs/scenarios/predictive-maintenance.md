# Predictive Equipment Failure

## Executive Summary

Unplanned downtime costs manufacturers $50 billion per year. Varpulis correlates vibration, temperature, and pressure readings across sensors in real-time, detecting equipment degradation hours before failure -- with explainable alerts that tell maintenance teams exactly *why* the alarm fired.

## The Business Problem

Manufacturing equipment does not fail instantly. It degrades over hours and days through subtle changes in vibration amplitude, operating temperature, and pressure readings. A bearing wearing out produces progressively stronger vibrations. An overheating zone spreads heat to adjacent equipment.

The challenge is that each individual sensor reading may look acceptable in isolation. A vibration of 0.8 mm is within tolerance. A temperature of 55 degrees Celsius is normal for that zone. But when vibration has been rising for the past hour *and* temperature is climbing *at the same time*, the combination signals imminent failure.

Current monitoring systems fall into two camps:
- **Threshold-based:** Alert when a single reading exceeds a fixed limit. Misses gradual degradation until it is too late.
- **Machine learning black boxes:** Flag anomalies without explaining *why*. Maintenance teams ignore alerts they cannot understand.

## How It Works

Varpulis monitors the stream of sensor readings and detects *patterns of change over time*, not just individual thresholds.

```
Bearing Degradation Detection:

Time ─────────────────────────────────────►

  t=0           t=30s           t=60s
  │              │               │
  ▼              ▼               ▼
┌──────┐    ┌──────┐
│ 0.5mm│───►│ 0.8mm│───► ALERT!
│      │    │      │    60% increase
└──────┘    └──────┘    (threshold: 30%)
 CNC-01      CNC-01

Same machine, rising amplitude → degradation
```

## What Varpulis Detects

### Bearing Degradation
Vibration readings from a CNC machine bearing are getting progressively stronger: 0.5 mm, then 0.8 mm over 30 seconds. That is a 60% increase, well above the 30% threshold. That bearing is wearing out -- schedule replacement before it seizes.

### Overheating Cascade
Zone A temperature jumps from 45 degrees to 62 degrees in 15 seconds -- a 17-degree spike where the threshold is 15. The heat is spreading. Shut down before it reaches the control electronics.

### Healthy Equipment Confirmation
Machine CNC-02 shows vibration at 0.3 mm, then 0.31 mm -- a 3% change. Well within normal operating range. No alert generated. Maintenance teams trust the system because it does not cry wolf.

## Why Competitors Miss This

Rule-based systems require engineers to pre-program every failure scenario. If they have not seen this specific combination of vibration and temperature before, they cannot write a rule for it.

Machine learning systems detect anomalies but produce opaque alerts: "anomaly score 0.87." The maintenance technician has no idea which sensor is the problem or how urgent it is. They learn to ignore the alerts.

Varpulis provides the best of both worlds: pattern-based detection that is deterministic and explainable. Every alert includes the specific sensor readings that triggered it, the rate of change, and which threshold was exceeded.

## Measurable Impact

- **40% reduction in unplanned downtime** -- catch degradation before failure
- **Alerts hours before failure** -- not after the line stops
- **Explainable alerts** -- every alert says *which* sensor, *what* changed, and *by how much*
- **Zero false positives on healthy equipment** -- maintenance teams trust the alerts

## Live Demo Walkthrough

```bash
cargo test -p varpulis-runtime --test cxo_scenario_tests cxo_maintenance
```

**Input events:**
1. CNC-01 vibration: 0.5 mm (normal)
2. CNC-01 vibration: 0.8 mm (60% increase -- degradation)
3. Zone A temperature: 45 degrees (normal)
4. Zone A temperature: 62 degrees (17-degree spike -- overheating)
5. CNC-02 vibration: 0.3 mm then 0.31 mm (healthy, no alert)

**Alerts generated:**
- `bearing_degradation` -- CNC-01, amplitude 0.5 to 0.8 mm
- `overheating` -- ZoneA, 45 to 62 degrees

**No false positives:** CNC-02 (healthy machine) generates zero alerts.

<details>
<summary>Technical Appendix</summary>

### VPL Pattern: Bearing Degradation
```
stream BearingDegradation = VibrationReading as first
    -> VibrationReading where machine_id == first.machine_id as second
    .within(2h)
    .where(second.amplitude > first.amplitude * 1.3)
    .emit(alert_type: "bearing_degradation", ...)
```

### Test Files
- Rules: `tests/scenarios/cxo_predictive_maintenance.vpl`
- Events: `tests/scenarios/cxo_predictive_maintenance.evt`
</details>
