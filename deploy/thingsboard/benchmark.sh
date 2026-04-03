#!/bin/bash
# ThingsBoard vs Varpulis — Performance Benchmark
#
# Measures Varpulis CEP performance via MQTT injection:
#   - Throughput at increasing event rates
#   - Processing latency (from Prometheus histograms)
#   - Memory usage
#   - Pattern match counts
#   - DLQ (dropped events)
#
# Usage: ./benchmark.sh

set -e

CONTAINER="thingsboard-mosquitto-1"
VARPULIS_URL="http://localhost:19000"
METRICS_URL="http://localhost:19090/metrics"
API_KEY="varpulis-tb-demo"
TOPIC="tb/telemetry"

DEVICES=("sensor-zone-a-01:zone-a" "sensor-zone-a-02:zone-a" "sensor-zone-b-01:zone-b" "sensor-zone-b-02:zone-b" "sensor-zone-c-01:zone-c" "sensor-zone-c-02:zone-c")

# ── Helper functions ─────────────────────────────────────────────

get_metric() {
  curl -s "$METRICS_URL" 2>/dev/null | grep "^$1" | head -1 | awk '{print $2}'
}

get_total_events() {
  curl -s "$METRICS_URL" 2>/dev/null | grep 'varpulis_events_total{' | awk '{s+=$2} END {print s+0}'
}

get_total_output() {
  curl -s "$METRICS_URL" 2>/dev/null | grep 'varpulis_output_events_total{' | awk '{s+=$2} END {print s+0}'
}

get_latency_p99() {
  # Approximate p99 from histogram buckets for Telemetry stream
  curl -s "$METRICS_URL" 2>/dev/null | grep 'varpulis_processing_latency_seconds_bucket{stream="Telemetry"' | \
    awk -F'[="}]' '{le=$4; count=$NF} {print le, count}' | \
    python3 -c "
import sys
buckets = []
for line in sys.stdin:
    parts = line.strip().split()
    if len(parts) == 2:
        buckets.append((float(parts[0]) if parts[0] != '+Inf' else 999, float(parts[1])))
if not buckets: print('N/A'); exit()
total = buckets[-1][1]
if total == 0: print('N/A'); exit()
p99_target = total * 0.99
for le, count in buckets:
    if count >= p99_target:
        if le < 0.001: print(f'{le*1000000:.0f}µs')
        elif le < 1: print(f'{le*1000:.1f}ms')
        else: print(f'{le:.2f}s')
        break
" 2>/dev/null || echo "N/A"
}

get_memory_mb() {
  docker stats --no-stream --format '{{.MemUsage}}' thingsboard-varpulis-1 2>/dev/null | \
    awk -F'/' '{print $1}' | sed 's/[^0-9.]//g'
}

get_tb_memory_mb() {
  docker stats --no-stream --format '{{.MemUsage}}' thingsboard-thingsboard-1 2>/dev/null | \
    awk -F'/' '{print $1}' | sed 's/[^0-9.]//g'
}

inject_burst() {
  local count=$1 rate=$2
  local interval=$(python3 -c "print(max(0.001, 1.0/$rate))")
  local sent=0
  local dev_count=${#DEVICES[@]}

  while [ $sent -lt $count ]; do
    for DEV_ZONE in "${DEVICES[@]}"; do
      [ $sent -ge $count ] && break
      local NAME="${DEV_ZONE%%:*}"
      local ZONE="${DEV_ZONE##*:}"
      local TEMP=$(python3 -c "import random; print(round(22 + random.gauss(0, 15), 2))")
      local HUM=$(python3 -c "import random; print(round(55 + random.gauss(0, 10), 2))")

      docker exec "$CONTAINER" mosquitto_pub -t "$TOPIC/$NAME" \
        -m "{\"event_type\":\"TbTelemetry\",\"deviceName\":\"$NAME\",\"deviceType\":\"IoT Sensor\",\"zone\":\"$ZONE\",\"temperature\":$TEMP,\"humidity\":$HUM,\"pressure\":1013.0,\"power_kw\":1.5}" 2>/dev/null &

      sent=$((sent + 1))
    done

    # Rate limiting: wait per batch of 6
    if [ $rate -lt 100 ]; then
      sleep "$interval"
    fi
  done
  wait
}

# ── Benchmark runs ───────────────────────────────────────────────

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Phase 4: Varpulis CEP Performance Benchmark                 ║"
echo "║  Server: $(curl -s $VARPULIS_URL/health 2>/dev/null | python3 -c "import sys,json; print(f'v{json.load(sys.stdin)[\"version\"]}')" 2>/dev/null || echo 'unknown')"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Baseline measurements
VARPULIS_MEM_IDLE=$(get_memory_mb)
TB_MEM=$(get_tb_memory_mb)
echo "── Baseline Memory ─────────────────────────────────────────────"
echo "  Varpulis (idle):     ${VARPULIS_MEM_IDLE} MiB"
echo "  ThingsBoard (idle):  ${TB_MEM} MiB"
echo ""

# Test 1: Throughput at 10 evt/s (60 events)
echo "── Test 1: Low rate (10 evt/s × 60 events) ────────────────────"
EVENTS_BEFORE=$(get_total_events)
START=$(date +%s%N)
inject_burst 60 10
END=$(date +%s%N)
EVENTS_AFTER=$(get_total_events)
DURATION_MS=$(( (END - START) / 1000000 ))
PROCESSED=$((EVENTS_AFTER - EVENTS_BEFORE))
RATE_ACTUAL=$(python3 -c "print(round($PROCESSED / ($DURATION_MS / 1000.0), 1))")
P99=$(get_latency_p99)
echo "  Injected:    60 events in ${DURATION_MS}ms"
echo "  Processed:   $PROCESSED events"
echo "  Throughput:  $RATE_ACTUAL evt/s"
echo "  Latency p99: $P99"
echo ""

sleep 2

# Test 2: Throughput at 50 evt/s (300 events)
echo "── Test 2: Medium rate (50 evt/s × 300 events) ────────────────"
EVENTS_BEFORE=$(get_total_events)
START=$(date +%s%N)
inject_burst 300 50
END=$(date +%s%N)
EVENTS_AFTER=$(get_total_events)
sleep 1
EVENTS_AFTER=$(get_total_events)
DURATION_MS=$(( (END - START) / 1000000 ))
PROCESSED=$((EVENTS_AFTER - EVENTS_BEFORE))
RATE_ACTUAL=$(python3 -c "print(round($PROCESSED / ($DURATION_MS / 1000.0), 1))")
P99=$(get_latency_p99)
echo "  Injected:    300 events in ${DURATION_MS}ms"
echo "  Processed:   $PROCESSED events"
echo "  Throughput:  $RATE_ACTUAL evt/s"
echo "  Latency p99: $P99"
echo ""

sleep 2

# Test 3: Burst (1000 events as fast as possible)
echo "── Test 3: Burst (1000 events, max rate) ──────────────────────"
EVENTS_BEFORE=$(get_total_events)
START=$(date +%s%N)
inject_burst 1000 10000
END=$(date +%s%N)
sleep 2
EVENTS_AFTER=$(get_total_events)
DURATION_MS=$(( (END - START) / 1000000 ))
PROCESSED=$((EVENTS_AFTER - EVENTS_BEFORE))
RATE_ACTUAL=$(python3 -c "print(round($PROCESSED / ($DURATION_MS / 1000.0), 1))")
P99=$(get_latency_p99)
VARPULIS_MEM_LOAD=$(get_memory_mb)
echo "  Injected:    1000 events in ${DURATION_MS}ms"
echo "  Processed:   $PROCESSED events"
echo "  Throughput:  $RATE_ACTUAL evt/s"
echo "  Latency p99: $P99"
echo "  Memory:      ${VARPULIS_MEM_LOAD} MiB"
echo ""

sleep 2

# Test 4: Sustained (2000 events over 30s)
echo "── Test 4: Sustained (2000 events over 30s) ───────────────────"
EVENTS_BEFORE=$(get_total_events)
OUTPUT_BEFORE=$(get_total_output)
DLQ_BEFORE=$(get_metric "varpulis_dlq_events_total")
START=$(date +%s%N)
inject_burst 2000 66
END=$(date +%s%N)
sleep 3
EVENTS_AFTER=$(get_total_events)
OUTPUT_AFTER=$(get_total_output)
DLQ_AFTER=$(get_metric "varpulis_dlq_events_total")
DURATION_MS=$(( (END - START) / 1000000 ))
PROCESSED=$((EVENTS_AFTER - EVENTS_BEFORE))
OUTPUTS=$((OUTPUT_AFTER - OUTPUT_BEFORE))
DROPPED=$((DLQ_AFTER - DLQ_BEFORE))
RATE_ACTUAL=$(python3 -c "print(round($PROCESSED / ($DURATION_MS / 1000.0), 1))")
P99=$(get_latency_p99)
VARPULIS_MEM_SUSTAINED=$(get_memory_mb)
echo "  Injected:    2000 events in ${DURATION_MS}ms"
echo "  Processed:   $PROCESSED events"
echo "  Output:      $OUTPUTS pattern matches"
echo "  Dropped:     $DROPPED (DLQ)"
echo "  Throughput:  $RATE_ACTUAL evt/s"
echo "  Latency p99: $P99"
echo "  Memory:      ${VARPULIS_MEM_SUSTAINED} MiB"
echo ""

# ── Summary ──────────────────────────────────────────────────────

TOTAL_EVENTS=$(get_total_events)
TOTAL_OUTPUTS=$(get_total_output)
TOTAL_DLQ=$(get_metric "varpulis_dlq_events_total")
ACTIVE_STREAMS=$(get_metric "varpulis_active_streams")

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  BENCHMARK RESULTS SUMMARY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Varpulis CEP Engine"
echo "  ├── Active streams:     $ACTIVE_STREAMS"
echo "  ├── Total events:       $TOTAL_EVENTS"
echo "  ├── Total outputs:      $TOTAL_OUTPUTS"
echo "  ├── Dropped (DLQ):      $TOTAL_DLQ"
echo "  ├── Memory (idle):      ${VARPULIS_MEM_IDLE} MiB"
echo "  ├── Memory (burst):     ${VARPULIS_MEM_LOAD} MiB"
echo "  └── Memory (sustained): ${VARPULIS_MEM_SUSTAINED} MiB"
echo ""
echo "  ThingsBoard CE"
echo "  └── Memory:             ${TB_MEM} MiB"
echo ""
echo "  ┌─────────────────────────────────────────────────────────┐"
echo "  │ Comparison: Varpulis vs ThingsBoard CE                  │"
echo "  ├─────────────┬──────────────┬────────────────────────────┤"
echo "  │ Metric      │ ThingsBoard  │ Varpulis                   │"
echo "  ├─────────────┼──────────────┼────────────────────────────┤"
echo "  │ Memory      │ ${TB_MEM} MiB     │ ${VARPULIS_MEM_SUSTAINED} MiB                    │"
echo "  │ Patterns    │ 0 (none)     │ 5 SASE+ + forecast         │"
echo "  │ Windows     │ 0 (PE only)  │ sliding + zone aggregation │"
echo "  │ Latency p99 │ 2-5 seconds  │ $P99                   │"
echo "  │ DLQ/drops   │ N/A          │ $TOTAL_DLQ                         │"
echo "  └─────────────┴──────────────┴────────────────────────────┘"
echo ""
echo "  Note: ThingsBoard throughput limited to ~5K msg/s."
echo "  Varpulis CLI mode reaches 410K evt/s (not MQTT-bound)."
echo "  MQTT transport is the bottleneck in this benchmark."
echo ""
