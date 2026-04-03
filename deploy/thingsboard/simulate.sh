#!/bin/bash
# Quick telemetry simulator using mosquitto_pub (no Python deps needed)
# Publishes directly to the Mosquitto bridge to test Varpulis CEP
#
# Usage: ./simulate.sh [rate_hz] [duration_secs]
#   rate_hz:       events per second (default: 2)
#   duration_secs: how long to run, 0=infinite (default: 60)

RATE=${1:-2}
DURATION=${2:-60}
CONTAINER="thingsboard-mosquitto-1"
TOPIC_PREFIX="tb/telemetry"
COUNT=0
ANOMALIES=0
START=$(date +%s)

DEVICES=("sensor-zone-a-01:zone-a" "sensor-zone-a-02:zone-a" "sensor-zone-b-01:zone-b" "sensor-zone-b-02:zone-b" "sensor-zone-c-01:zone-c" "sensor-zone-c-02:zone-c")

echo "ThingsBoard + Varpulis CEP Telemetry Simulator"
echo "================================================"
echo "  Rate:     $RATE events/sec"
echo "  Duration: ${DURATION}s (0=infinite)"
echo "  Devices:  ${#DEVICES[@]}"
echo ""

INTERVAL=$(python3 -c "print(1.0/$RATE)")

while true; do
  for DEV_ZONE in "${DEVICES[@]}"; do
    NAME="${DEV_ZONE%%:*}"
    ZONE="${DEV_ZONE##*:}"

    # Base values with noise
    TEMP=$(python3 -c "import random,math; print(round(22 + 3*math.sin($COUNT/30.0) + random.gauss(0, 0.5), 2))")
    HUM=$(python3 -c "import random,math; print(round(55 + 5*math.sin($COUNT/45.0) + random.gauss(0, 1), 2))")
    PRES=$(python3 -c "import random; print(round(1013 + random.gauss(0, 0.3), 2))")
    POWER=$(python3 -c "import random; print(round(random.uniform(0.5, 3.0), 2))")

    # Inject anomalies (~5% chance)
    ANOMALY=""
    R=$(python3 -c "import random; print(random.random())")
    if python3 -c "exit(0 if $R < 0.02 else 1)"; then
      TEMP=$(python3 -c "import random; print(round(random.uniform(88, 105), 2))")
      ANOMALY="HIGH_TEMP"
    elif python3 -c "exit(0 if $R < 0.04 else 1)"; then
      HUM=$(python3 -c "import random; print(round(random.uniform(87, 95), 2))")
      ANOMALY="HIGH_HUM"
    elif python3 -c "exit(0 if $R < 0.05 else 1)"; then
      PRES=$(python3 -c "import random; print(round(random.uniform(1052, 1070), 2))")
      ANOMALY="HIGH_PRES"
    fi

    PAYLOAD="{\"event_type\":\"TbTelemetry\",\"deviceName\":\"$NAME\",\"deviceType\":\"IoT Sensor\",\"zone\":\"$ZONE\",\"temperature\":$TEMP,\"humidity\":$HUM,\"pressure\":$PRES,\"power_kw\":$POWER}"

    docker exec "$CONTAINER" mosquitto_pub -t "$TOPIC_PREFIX/$NAME" -m "$PAYLOAD" 2>/dev/null

    COUNT=$((COUNT + 1))
    if [ -n "$ANOMALY" ]; then
      ANOMALIES=$((ANOMALIES + 1))
      echo "  [$NAME] ANOMALY ($ANOMALY): temp=$TEMP hum=$HUM pres=$PRES"
    fi
  done

  # Progress every 10 cycles
  if [ $((COUNT % (${#DEVICES[@]} * 10))) -eq 0 ]; then
    ELAPSED=$(( $(date +%s) - START ))
    RATE_ACTUAL=$(python3 -c "print(round($COUNT / max(1,$ELAPSED), 1))")
    echo "  [${ELAPSED}s] $COUNT events sent ($RATE_ACTUAL evt/s), $ANOMALIES anomalies"
  fi

  # Check duration
  if [ "$DURATION" -gt 0 ]; then
    ELAPSED=$(( $(date +%s) - START ))
    if [ "$ELAPSED" -ge "$DURATION" ]; then
      break
    fi
  fi

  sleep "$INTERVAL"
done

echo ""
echo "Done: $COUNT events, $ANOMALIES anomalies in ${ELAPSED}s"
