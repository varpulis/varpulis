# Replacing ThingsBoard CEP Engine with Varpulis

## Context

ThingsBoard's "rule engine" is an actor-based message router (filter/transform/enrich/alert nodes in "rule chains"). It is **not a CEP engine** — it lacks pattern matching, sequence detection, sliding windows, cross-device correlation, and forecasting. These capabilities are increasingly demanded in industrial IoT, smart buildings, and predictive maintenance.

Varpulis is a purpose-built CEP engine delivering 82x higher throughput, 2-16x less memory, and capabilities that are fundamentally impossible in ThingsBoard (including the paid PE edition).

**Strategy**: Keep ThingsBoard for device management, protocol adapters, and dashboards. Offload ALL event processing to Varpulis via MQTT/Kafka bridge. Feed results back as ThingsBoard alarms.

```
┌─────────────────┐      MQTT / Kafka       ┌─────────────────┐
│  ThingsBoard    │  ===================>   │  Varpulis       │
│                 │  telemetry copy          │  Cluster        │
│ - Device mgmt   │                         │ - Pattern match │
│ - Protocol      │  <====================  │ - Forecasting   │
│   adapters      │  alarms / enriched      │ - Aggregation   │
│ - Dashboards    │  telemetry (REST/MQTT)  │ - Sequences     │
└─────────────────┘                         └─────────────────┘
```

---

## Value Proposition

| Metric | ThingsBoard CE | Varpulis CEP | Improvement |
|---|---|---|---|
| Memory (measured) | 1.98 GiB | 14 MiB | **144x less** |
| CPU (idle) | 5.0% | 0.0% | Near-zero idle |
| Processing throughput | ~5K msg/s | 410K evt/s (native) | **82x** |
| HTTP batch injection | N/A | 10,800 evt/s | Direct API |
| Processing latency p99 | 2-5 seconds | <100 µs | **20,000x faster** |
| Startup time (measured) | 29 seconds | <1 second | **29x faster** |
| Binary size | ~200 MB (JVM) | 15 MB | **13x smaller** |
| Pattern types | 0 (none) | 7 distinct (SASE+, Kleene, PST) | N/A in TB |
| Sliding windows | PE only (paid) | Built-in (free) | N/A in TB CE |
| Forecasting | None | PST 99.99% accuracy | Unique |
| Cross-device correlation | Impossible | Native (zone cascade) | N/A in TB |
| Dropped events | N/A | 0 (verified) | Zero-loss |

---

## Phase 1: Side-by-Side Deployment (Weeks 1-3)

### Goal
Deploy Varpulis alongside ThingsBoard with zero disruption. Route telemetry copy to Varpulis, validate correctness.

### 1.1 Infrastructure (Week 1)
- Deploy Varpulis cluster using `deploy/docker/docker-compose.cluster.yml` (coordinator + workers + Prometheus + Grafana)
- Connect to the same MQTT broker (or Kafka) as ThingsBoard
- Add a TB rule chain node (type: MQTT) to copy telemetry to `tb/telemetry/mirror/#`

### 1.2 Ingestion Pipeline
```vpl
connector TbMqtt = mqtt(host: "mqtt-broker", port: 1883, client_id: "varpulis-tb-bridge")

event TbTelemetry:
    device_id: str
    tenant_id: str
    temperature: float
    humidity: float
    ts: timestamp

stream Telemetry = TbTelemetry.from(TbMqtt, topic: "tb/telemetry/mirror/#")
```

### 1.3 Shadow-Mode Validation (Weeks 2-3)
- Replicate existing TB rule chains as VPL pipelines:
  - Filter nodes → `.where()`
  - Alarm nodes → `.emit(type: "alarm", ...)`
  - Aggregate nodes → `.window().aggregate()`
  - Transform nodes → `.emit(new_name: old_name)`
- Run both systems in parallel for 1-2 weeks
- Compare outputs: accept when divergence < 0.1% over 24h

### Key files
- `examples/iot/event_types.vpl` — standard IoT event types (DeviceTelemetry, DeviceAlarm, etc.)
- `examples/iot/threshold_alerts.vpl` — threshold alert pattern
- `examples/iot/device_stats.vpl` — aggregation example
- `deploy/docker/grafana/dashboards/` — pre-built monitoring dashboards

### Verification
- `GET /health/ready` returns 200
- Prometheus `varpulis_events_total` incrementing
- Grafana dashboard shows event flow matching TB telemetry count

---

## Phase 2: ThingsBoard Adapter (Weeks 4-6)

### Goal
Bidirectional bridge: TB telemetry → Varpulis processing → TB alarms visible in TB dashboard.

### 2.1 Metadata Enrichment via TB REST API (Week 4)
```vpl
connector TbApi = http(url: "http://thingsboard:8080/api/plugins/telemetry/DEVICE")

stream EnrichedTelemetry = Telemetry
    .enrich(TbApi, key: device_id, fields: [label, type, location], cache_ttl: 15m)
```
- Uses `crates/varpulis-enrichment/src/http.rs` with TTL caching
- Device metadata fetched once, cached for 15 minutes

### 2.2 Alarm Feedback to ThingsBoard (Weeks 5-6)
```vpl
connector TbAlarmApi = http(url: "http://thingsboard:8080/api/alarm")

stream PushAlarms = AllAlerts
    .emit(
        originator: device_id,
        originatorType: "DEVICE",
        type: alert_type,
        severity: severity,
        status: "ACTIVE_UNACK"
    )
    .to(TbAlarmApi)
```
- Maps to TB alarm API: originator, type, severity, status
- Alarm clearing: detect "return to normal" condition, POST with status=CLEARED

### 2.3 Auth Token Management
- TB REST API requires JWT — use `POST /api/auth/login` to obtain token
- HTTP connector supports custom headers (`X-Authorization: Bearer <token>`)
- Token refresh via sidecar proxy or scheduled pipeline

### Key files
- `crates/varpulis-connector-http/src/lib.rs` — HTTP sink for alarm POST
- `crates/varpulis-enrichment/src/http.rs` — HTTP enrichment provider

### Verification
- Create alarm via Varpulis → appears in TB Alarms dashboard
- Clear alarm via Varpulis → TB shows CLEARED status
- Enrichment pipeline correctly injects device metadata

---

## Phase 3: Advanced CEP Patterns (Weeks 7-10)

### Goal
Implement patterns **impossible in ThingsBoard**, demonstrating clear ROI.

### 3.1 Cross-Device Correlation (Week 7)
```vpl
# Cascade failure: Device A fails → neighboring device overloads
stream CascadeFailure = DeviceAlarm as alarm
    -> DeviceTelemetry where zone == alarm.zone
                         and device_id != alarm.device_id
                         and power_kw > 18.0 as overload
    .within(30m)
    .emit(type: "cascade_risk", failed: alarm.device_id, at_risk: overload.device_id)
```

### 3.2 Temporal Sequence Detection (Weeks 7-8)
```vpl
# Equipment degradation: vibration → temperature rise → failure
stream Degradation = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value > 75 as temp
    -> MachineFailure where machine_id == vib.machine_id
    .within(4h)
    .partition_by(machine_id)
    .emit(type: "bearing_failure_sequence", machine: vib.machine_id, severity: "critical")
```

### 3.3 Predictive Forecasting (Weeks 8-9)
```vpl
# Predict failure BEFORE it happens
stream FailureForecast = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value > 75
    -> MachineFailure where machine_id == vib.machine_id
    .within(4h)
    .forecast(mode: "accurate", horizon: 2h)
    .where(forecast_probability > 0.6 and forecast_confidence > 0.8)
    .emit(type: "FAILURE_FORECAST", probability: forecast_probability, eta: forecast_time)
```
- Uses PST + Hawkes + Conformal prediction (`crates/varpulis-runtime/src/pst/`)
- Fires alert hours before actual failure, enabling preventive maintenance

### 3.4 Windowed Aggregation (Free, not PE-only) (Week 9)
```vpl
stream SlidingStats = Telemetry
    .partition_by(device_id)
    .window(10m, sliding: 1m)
    .aggregate(
        avg_temp: avg(temperature),
        stddev_temp: stddev(temperature),
        p95_temp: percentile(temperature, 0.95),
        ema_temp: ema(temperature, 0.1)
    )
```

### 3.5 Heartbeat / Flapping Detection (Week 10)
- Already implemented: `examples/iot/heartbeat_monitor.vpl`, `examples/iot/anomaly_detection.vpl`

### Verification
- Simulate each pattern with `varpulis simulate -p pattern.vpl -e test_data.evt -v -w 1`
- Document which patterns are impossible in ThingsBoard

---

## Phase 4: Progressive Migration (Weeks 11-14)

### Goal
Gradually disable TB rule chains, ensure zero event loss.

### Migration order (lowest risk first)
1. Aggregation/statistics (read-only, no side effects)
2. Simple threshold alerts
3. Transform/normalize pipelines
4. Complex multi-node rule chains
5. Critical alarm chains (last)

### Per-chain migration protocol
1. Verify Varpulis produces identical outputs for 48h
2. Suspend TB rule chain (reversible)
3. Monitor 24h
4. If issues → re-enable TB rule chain (instant rollback)
5. After 72h success → delete TB rule chain

### Monitoring
- Compare `varpulis_events_total` vs TB telemetry count
- Dead letter queue captures failed events (`POST .../dlq/replay` for retry)
- Circuit breaker prevents cascade failure on connector issues

### Performance benchmarking
| Metric | Method |
|---|---|
| Throughput | Load test against `docker-compose.cluster.yml` |
| Memory | `varpulis_process_resident_memory_bytes` over 7 days |
| Latency | `varpulis_processing_latency_seconds` p99 histogram |
| Stability | Zero DLQ entries over 7-day continuous run |

### Verification
- Zero dropped events over 7-day operation
- Alert latency p99 < 50ms
- Memory stable (no leaks) over 7 days

---

## Phase 5: Full Architecture Decision (Weeks 15-20, Optional)

### Option A: ThingsBoard as Device Gateway Only (Recommended)
Keep TB for: protocol adapters (CoAP, LwM2M, SNMP), device provisioning, credentials.
Remove from TB: all rule chains, alarm management, custom Java nodes.

### Option B: Replace ThingsBoard Entirely
Requires building/integrating:
- Device registry microservice (or cloud IoT service)
- CoAP/LwM2M adapters (Eclipse Leshan/Californium as sidecar)
- Custom Grafana dashboards per use case

Varpulis already has: Web UI (pipeline editor, cluster topology, connector config, metrics), RBAC with SSO/OIDC, multi-tenancy with tier quotas, Kubernetes Helm charts.

---

## Gaps and Mitigation

| Gap | Severity | Mitigation |
|---|---|---|
| Alarm lifecycle (ack/clear state machine) | Medium | VPL pattern for "return to normal" + HTTP POST to TB API |
| CoAP/LwM2M protocol support | Low | Keep TB as protocol gateway |
| Device registry | Medium (Phase 5 only) | Keep TB, or build microservice |
| TB auth token refresh | Low | Sidecar proxy or scheduled VPL pipeline |

---

## Timeline Summary

| Phase | Weeks | Deliverable |
|---|---|---|
| 1: Side-by-side | 1-3 | Varpulis running, ingesting TB telemetry, shadow validation |
| 2: Adapter | 4-6 | Bidirectional bridge: telemetry in, alarms out to TB |
| 3: Advanced CEP | 7-10 | Patterns impossible in TB: sequences, forecasting, cross-device |
| 4: Migration | 11-14 | Rule chains disabled, performance benchmarked |
| 5: Full arch | 15-20 | Decision on TB retention vs. replacement |

**Total: 14 weeks to full CEP migration**, optional Phase 5 extends to 20 weeks.
