#!/bin/bash
# ThingsBoard + Varpulis CEP — Deterministic Demo Scenarios
#
# Injects specific event sequences to reliably trigger each
# advanced CEP pattern. Run this to demonstrate capabilities
# that are impossible in ThingsBoard.
#
# Usage: ./demo-scenarios.sh [scenario|all]
#   ./demo-scenarios.sh threshold       # Basic threshold alerts
#   ./demo-scenarios.sh spike           # Rapid temperature spike (SASE+)
#   ./demo-scenarios.sh cascade         # Cross-device zone cascade
#   ./demo-scenarios.sh stress          # 3-step equipment stress sequence
#   ./demo-scenarios.sh kleene          # Kleene closure (hot+ → recovery)
#   ./demo-scenarios.sh zone-stats      # Zone-level statistical anomaly
#   ./demo-scenarios.sh all             # Run all scenarios in sequence

CONTAINER="thingsboard-mosquitto-1"
TOPIC="tb/telemetry"

pub() {
  local device="$1" zone="$2" temp="$3" hum="$4" pres="$5"
  docker exec "$CONTAINER" mosquitto_pub -t "$TOPIC/$device" \
    -m "{\"event_type\":\"TbTelemetry\",\"deviceName\":\"$device\",\"deviceType\":\"IoT Sensor\",\"zone\":\"$zone\",\"temperature\":$temp,\"humidity\":$hum,\"pressure\":$pres,\"power_kw\":1.5}"
}

scenario_threshold() {
  echo "━━━ Scenario 1: Threshold Alerts ━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  TB equivalent: 'Create Alarm' rule chain node"
  echo "  Sending: sensor-zone-a-01 at 92°C (>85 threshold)"
  echo ""

  pub "sensor-zone-a-01" "zone-a" 92.0 55.0 1013.0
  sleep 0.5

  echo "  Sending: sensor-zone-b-01 at 105°C (>100 → CRITICAL)"
  pub "sensor-zone-b-01" "zone-b" 105.0 60.0 1012.0
  sleep 0.5

  echo "  Sending: sensor-zone-a-02 at 22°C, humidity 97% (>95 threshold)"
  pub "sensor-zone-a-02" "zone-a" 22.0 97.0 1013.5
  sleep 1

  echo "  ✓ Expected: HIGH_TEMPERATURE (MAJOR), HIGH_TEMPERATURE (CRITICAL), HIGH_HUMIDITY (WARNING)"
  echo ""
}

scenario_spike() {
  echo "━━━ Scenario 2: Rapid Temperature Spike (SASE+ Sequence) ━━"
  echo "  TB equivalent: IMPOSSIBLE — no temporal sequence operators"
  echo "  Pattern: temp rises >15°C within 5 minutes on same device"
  echo ""

  echo "  Step 1: sensor-zone-b-02 at 22°C (baseline)"
  pub "sensor-zone-b-02" "zone-b" 22.0 55.0 1012.5
  sleep 1

  echo "  Step 2: sensor-zone-b-02 at 45°C (23°C rise → triggers spike)"
  pub "sensor-zone-b-02" "zone-b" 45.0 55.0 1012.5
  sleep 1

  echo "  ✓ Expected: RAPID_TEMP_SPIKE (CRITICAL) — delta 23°C in <5min"
  echo ""
}

scenario_cascade() {
  echo "━━━ Scenario 3: Cross-Device Zone Cascade ━━━━━━━━━━━━━━━━━"
  echo "  TB equivalent: IMPOSSIBLE — rule chains process one device at a time"
  echo "  Pattern: device A overheats (>80°C), then device B in same zone >60°C"
  echo ""

  echo "  Step 1: sensor-zone-c-01 at 88°C (zone-c overheating)"
  pub "sensor-zone-c-01" "zone-c" 88.0 50.0 1014.0
  sleep 1

  echo "  Step 2: sensor-zone-c-02 at 67°C (zone-c neighbor also warming)"
  pub "sensor-zone-c-02" "zone-c" 67.0 52.0 1014.5
  sleep 1

  echo "  ✓ Expected: ZONE_CASCADE_RISK (CRITICAL) — hot=c-01(88°C), neighbor=c-02(67°C)"
  echo ""
}

scenario_stress() {
  echo "━━━ Scenario 4: 3-Step Equipment Stress Sequence ━━━━━━━━━━"
  echo "  TB equivalent: IMPOSSIBLE — no multi-step temporal patterns"
  echo "  Pattern: humidity >85% → temp >70°C → pressure >1050 within 2h"
  echo ""

  echo "  Step 1: sensor-zone-a-01 — humidity 90% (stress begins)"
  pub "sensor-zone-a-01" "zone-a" 25.0 90.0 1013.0
  sleep 1

  echo "  Step 2: sensor-zone-a-01 — temperature 78°C (stress escalates)"
  pub "sensor-zone-a-01" "zone-a" 78.0 60.0 1013.0
  sleep 1

  echo "  Step 3: sensor-zone-a-01 — pressure 1060 hPa (stress completes)"
  pub "sensor-zone-a-01" "zone-a" 30.0 55.0 1060.0
  sleep 1

  echo "  ✓ Expected: EQUIPMENT_STRESS_SEQUENCE (MAJOR) — 3-stage on sensor-zone-a-01"
  echo ""
}

scenario_kleene() {
  echo "━━━ Scenario 5: Kleene Closure — Transient Spike ━━━━━━━━━━"
  echo "  TB equivalent: IMPOSSIBLE — no Kleene operators (A+)"
  echo "  Pattern: 2+ readings >75°C followed by recovery ≤75°C"
  echo ""

  echo "  Hot 1: sensor-zone-b-01 at 80°C"
  pub "sensor-zone-b-01" "zone-b" 80.0 55.0 1012.0
  sleep 0.5

  echo "  Hot 2: sensor-zone-b-01 at 83°C"
  pub "sensor-zone-b-01" "zone-b" 83.0 55.0 1012.0
  sleep 0.5

  echo "  Hot 3: sensor-zone-b-01 at 79°C"
  pub "sensor-zone-b-01" "zone-b" 79.0 55.0 1012.0
  sleep 0.5

  echo "  Recovery: sensor-zone-b-01 at 22°C (back to normal)"
  pub "sensor-zone-b-01" "zone-b" 22.0 55.0 1012.0
  sleep 1

  echo "  ✓ Expected: TRANSIENT_TEMP_SPIKE (WARNING) — hot+ then recovery"
  echo ""
}

scenario_zone_stats() {
  echo "━━━ Scenario 6: Zone-Level Statistical Anomaly ━━━━━━━━━━━━"
  echo "  TB equivalent: IMPOSSIBLE — no cross-device aggregation,"
  echo "    no stddev, no percentiles (aggregation is PE-only)"
  echo "  Pattern: zone stddev >10 or p95 >90°C"
  echo ""

  echo "  Sending varied temperatures across zone-a devices..."
  pub "sensor-zone-a-01" "zone-a" 20.0 55.0 1013.0
  sleep 0.3
  pub "sensor-zone-a-02" "zone-a" 95.0 57.0 1013.5
  sleep 0.3
  pub "sensor-zone-a-01" "zone-a" 15.0 55.0 1013.0
  sleep 0.3
  pub "sensor-zone-a-02" "zone-a" 98.0 57.0 1013.5
  sleep 1

  echo "  ✓ Expected: ZONE_STATISTICAL_ANOMALY (MAJOR) — high stddev and p95"
  echo ""
}

run_all() {
  echo ""
  echo "╔════════════════════════════════════════════════════════════╗"
  echo "║  Varpulis CEP Demo — Patterns Impossible in ThingsBoard  ║"
  echo "╚════════════════════════════════════════════════════════════╝"
  echo ""

  scenario_threshold
  sleep 2
  scenario_spike
  sleep 2
  scenario_cascade
  sleep 2
  scenario_stress
  sleep 2
  scenario_kleene
  sleep 2
  scenario_zone_stats

  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Demo complete. Check alarms at:"
  echo "    ThingsBoard:  http://95.216.191.129:18080 → Alarms tab"
  echo "    MQTT monitor: mosquitto_sub -h 95.216.191.129 -p 11884 -t 'tb/cep/#' -v"
  echo "    Varpulis API: curl http://95.216.191.129:19000/health"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

case "${1:-all}" in
  threshold)  scenario_threshold ;;
  spike)      scenario_spike ;;
  cascade)    scenario_cascade ;;
  stress)     scenario_stress ;;
  kleene)     scenario_kleene ;;
  zone-stats) scenario_zone_stats ;;
  all)        run_all ;;
  *)          echo "Usage: $0 [threshold|spike|cascade|stress|kleene|zone-stats|all]" ;;
esac
