# Varpulis CEP Engine — SLO/SLI Definitions

This document defines the Service Level Indicators (SLIs) and Service Level Objectives
(SLOs) for the Varpulis Complex Event Processing engine. It is the authoritative reference
for on-call engineers, capacity planners, and product stakeholders when assessing service
health and negotiating reliability commitments.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Service Level Indicators](#2-service-level-indicators)
   - 2.1 [Availability](#21-availability)
   - 2.2 [Processing Latency](#22-processing-latency)
   - 2.3 [Throughput](#23-throughput)
   - 2.4 [Error Rate](#24-error-rate)
   - 2.5 [Recovery](#25-recovery)
3. [Service Level Objectives](#3-service-level-objectives)
4. [Prometheus Metrics Mapping](#4-prometheus-metrics-mapping)
5. [Error Budget Policy](#5-error-budget-policy)
6. [Grafana SLO Dashboard](#6-grafana-slo-dashboard)
7. [Review Cadence](#7-review-cadence)

---

## 1. Overview

### What are SLIs and SLOs?

A **Service Level Indicator (SLI)** is a specific, quantitative measure of some aspect of
the service that matters to users. Good SLIs are:

- Measurable from existing telemetry (Prometheus metrics in this case)
- Clearly defined: the numerator and denominator are unambiguous
- Reflective of user experience, not just internal health

A **Service Level Objective (SLO)** is a target value or range for an SLI, expressed as a
percentage of time that the SLI must meet the target over a rolling window (typically 30
days). SLOs drive error budget policy — the amount of unreliability the team can "spend"
before reliability work must take priority over feature work.

### Why they matter for Varpulis

Varpulis processes time-sensitive event streams where late or dropped events have direct
downstream consequences (missed alerts, incorrect pattern matches, stale forecasts). The
SLOs below reflect the operational characteristics established during benchmark testing:

| Workload | Observed throughput | Source |
|----------|--------------------|-------------------------------------------------|
| Filter (CLI preload) | 234K events/sec/worker | Apama comparison benchmark, scenario 01 |
| Sequence (CLI preload) | 256K events/sec/worker | Apama comparison benchmark, scenario 07 |
| Kleene (CLI preload, normalized by matches) | 97K events/sec/worker | Apama comparison benchmark, scenario 04 |
| Hamlet aggregation (single query) | 6.9M events/sec | Hamlet vs ZDD benchmark |
| PST prediction | 51 ns per call | PST forecast benchmark |
| MQTT connector ceiling | ~6K events/sec | I/O-bound, QoS 0 single-message |

These numbers establish what "normal" looks like and inform the objective thresholds below.

---

## 2. Service Level Indicators

### 2.1 Availability

Availability measures whether the service is reachable and can process requests. Varpulis
has two availability surfaces:

**Coordinator availability** — the Raft-leader coordinator must be reachable for pipeline
deployments and cluster management operations. Its HTTP API at port 9100 is the probe target.

**Worker availability** — each worker node must be heartbeating successfully and carrying
assigned pipelines. Availability is measured as the fraction of workers that are in `ready`
status at any point in time.

SLI formula (coordinator):

```
coordinator_availability =
  (minutes_coordinator_api_responded_2xx / total_minutes_in_window) * 100
```

SLI formula (worker pool):

```
worker_pool_availability =
  avg_over_time(
    varpulis_cluster_workers_total{status="ready"} /
    (varpulis_cluster_workers_total{status="ready"} +
     varpulis_cluster_workers_total{status="unhealthy"})
  [30d]
  ) * 100
```

### 2.2 Processing Latency

Processing latency is measured from the moment an event is received by the engine to the
moment it has been fully evaluated against all applicable stream definitions on a worker.
The `varpulis_processing_latency_seconds` histogram tracks this per stream.

Two latency classes exist because pattern complexity varies significantly:

- **Simple filter** — a stream containing only `.where()` predicates and/or
  `.trend_aggregate()`. No SASE NFA is constructed. Latency target: p99 < 10ms.
- **SASE pattern** — a stream containing a sequence or Kleene pattern. The SASE+ engine
  runs an NFA traversal with active run management. Latency target: p99 < 100ms.

SLI formula (per stream):

```
latency_sli(quantile, stream) =
  histogram_quantile(
    quantile,
    sum(rate(varpulis_processing_latency_seconds_bucket[5m])) by (le, stream)
  )
```

### 2.3 Throughput

Throughput measures sustained event ingestion and processing capacity. It is a saturation
SLI: when throughput falls below the objective, the engine is either degraded or
under-provisioned relative to the incoming load.

SLI formula:

```
throughput_sli(worker) =
  sum(rate(varpulis_events_processed[5m])) by (instance)
```

The per-worker floor is set conservatively at 100K events/sec because:

- Benchmark filter mode achieves 234K events/sec at full CPU saturation
- MQTT connector I/O limits cap ingress at ~6K events/sec in connector-bound scenarios
- 100K provides a safe margin for mixed workloads with pattern matching overhead

### 2.4 Error Rate

Error rate measures the fraction of ingested events that fail to complete processing. A
failure includes: schema parse errors, runtime evaluation panics, connector delivery
failures that route to the dead letter queue, and events dropped due to queue overflow.

SLI formula:

```
error_rate_sli =
  1 - (
    sum(rate(varpulis_events_processed[5m]))
    /
    sum(rate(varpulis_events_total[5m]))
  )
```

A separate DLQ accumulation SLI tracks events that completed engine processing but could
not be delivered to sinks:

```
dlq_rate_sli =
  sum(rate(varpulis_dlq_events_total[5m]))
  /
  sum(rate(varpulis_output_events_total[5m]))
```

### 2.5 Recovery

Recovery SLIs measure how quickly the cluster self-heals after a failure event.

**Leader failover time** — after the current Raft leader becomes unavailable, how many
seconds until a new leader is elected and coordinator API requests succeed again. Chaos
tests exercise this path (see `tests/e2e-raft/`).

SLI proxy (Raft role stabilisation):

```
leader_failover_proxy =
  time from last varpulis_cluster_raft_role{role="leader"} == 1
  to next varpulis_cluster_raft_role{role="leader"} == 1
  (measured by changes(varpulis_cluster_raft_role[window]))
```

**Pipeline migration time** — after a worker goes unhealthy, how many seconds until its
pipelines are reassigned and running on a healthy worker.

SLI proxy:

```
migration_p99 =
  histogram_quantile(
    0.99,
    sum(rate(varpulis_cluster_migration_duration_seconds_bucket[5m])) by (le, result)
  )
```

**Checkpoint restore time** — not currently exposed as a Prometheus metric. Tracked via
structured logs (`tracing` spans tagged `checkpoint_restore`). Target: < 5s for a 1 GB
state snapshot.

---

## 3. Service Level Objectives

The table below states all objectives as percentages over a **30-day rolling window**.

| # | SLO Name | SLI | Objective | Window |
|---|----------|-----|-----------|--------|
| 1 | Coordinator Availability | HTTP 2xx response fraction | **99.9%** | 30 days |
| 2 | Worker Pool Availability | Fraction of workers in `ready` state | **99.5%** | 30 days |
| 3 | Filter Latency p99 | `varpulis_processing_latency_seconds` p99 (filter streams) | **< 10ms** | 5 min evaluation window |
| 4 | SASE Latency p99 | `varpulis_processing_latency_seconds` p99 (pattern streams) | **< 100ms** | 5 min evaluation window |
| 5 | Throughput Floor | events/sec/worker (sum across all streams) | **> 100K/worker** | 5 min evaluation window |
| 6 | Event Processing Error Rate | failed events / total events | **< 0.1%** | 30 days |
| 7 | DLQ Delivery Error Rate | DLQ events / output events | **< 0.5%** | 30 days |
| 8 | Leader Failover | Time from leader loss to new leader elected | **< 30s** | per incident |
| 9 | Pipeline Migration p99 | `varpulis_cluster_migration_duration_seconds` p99 | **< 10s** | 5 min evaluation window |

### Rationale for latency targets

The 10ms p99 target for simple filter streams is calibrated from the benchmark showing
234K events/sec throughput with a 4 µs average per-event cost. Even accounting for
connector I/O overhead, lock contention, and GC pressure, the 10ms budget provides
2500x headroom over the observed per-event cost.

The 100ms p99 target for SASE pattern streams reflects the SASE active run overhead. With
10K+ active runs (`varpulis_sase_peak_active_runs`), each event must traverse all active
run states. The alert threshold in `alerts.yml` fires at 100ms for exactly this reason.

### Rationale for availability targets

99.9% coordinator uptime permits ~43 minutes of downtime per month. Because Raft provides
automatic leader re-election (tested in chaos scenarios), sustained outages should be rare.
The 0.1% unplanned budget covers:

- Rolling upgrades (coordinator restart ~30s, covered by Raft failover)
- Unexpected leader election storms (mitigated by `VarpulisRaftLeaderChurn` alert)
- Infrastructure maintenance windows

99.5% worker pool availability permits ~3.6 hours of reduced worker capacity per month.
Workers can be added or replaced without stopping pipelines (migrations handle reassignment).

---

## 4. Prometheus Metrics Mapping

### 4.1 Complete metrics inventory

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `varpulis_events_total` | Counter | `event_type` | Events received by the engine |
| `varpulis_events_processed` | Counter | `stream` | Events fully processed per stream |
| `varpulis_output_events_total` | Counter | `stream`, `event_type` | Events emitted to sinks |
| `varpulis_processing_latency_seconds` | Histogram | `stream` | End-to-end per-event processing time |
| `varpulis_stream_queue_size` | Gauge | `stream` | Pending events in stream input queue |
| `varpulis_active_streams` | Gauge | — | Number of deployed stream definitions |
| `varpulis_sase_events_total` | Counter | — | Events evaluated by SASE+ engine |
| `varpulis_sase_matches_total` | Counter | — | Pattern matches emitted |
| `varpulis_sase_peak_active_runs` | Gauge | — | Peak concurrent NFA runs |
| `varpulis_dlq_events_total` | Counter | — | Events written to dead letter queue |
| `varpulis_connector_healthy` | Gauge | `connector`, `connector_type` | 1=healthy, 0=unhealthy |
| `varpulis_cluster_workers_total` | Gauge | `status` | Workers by status (ready/unhealthy/draining) |
| `varpulis_cluster_raft_role` | Gauge | — | 0=Follower, 1=Candidate, 2=Leader |
| `varpulis_cluster_raft_term` | Gauge | — | Current Raft consensus term |
| `varpulis_cluster_raft_commit_index` | Gauge | — | Last committed log entry index |
| `varpulis_cluster_pipeline_groups_total` | Gauge | — | Pipeline groups registered |
| `varpulis_cluster_deployments_total` | Counter | — | Cumulative pipeline deployments |
| `varpulis_cluster_deploy_duration_seconds` | Histogram | `result` | Pipeline deployment latency |
| `varpulis_cluster_migration_duration_seconds` | Histogram | `result` | Pipeline migration latency |
| `varpulis_cluster_migrations_total` | Counter | `result` | Pipeline migrations by result |
| `varpulis_cluster_health_sweep_duration_seconds` | Histogram | `workers_checked` | Coordinator health sweep latency |
| `process_resident_memory_bytes` | Gauge | `job` | RSS memory per process (node_exporter) |

### 4.2 PromQL queries for each SLI

#### SLO 1 — Coordinator Availability

Use the Prometheus `up` metric (set by the scrape loop) as a proxy for API availability.
For full HTTP-level probing, configure `blackbox_exporter` against `http://coordinator:9100/health`.

```promql
# Fraction of scrape intervals where coordinator was reachable (30d window)
avg_over_time(up{job="varpulis-coordinator"}[30d])
```

Error budget remaining (as seconds):

```promql
# Minutes of downtime budget remaining in the 30-day window
(
  avg_over_time(up{job="varpulis-coordinator"}[30d]) - 0.999
) * 30 * 24 * 60
```

#### SLO 2 — Worker Pool Availability

```promql
# Current worker pool availability ratio
varpulis_cluster_workers_total{status="ready"}
/
(
  varpulis_cluster_workers_total{status="ready"}
  + varpulis_cluster_workers_total{status="unhealthy"}
)
```

```promql
# Rolling 30-day worker pool availability
avg_over_time(
  (
    varpulis_cluster_workers_total{status="ready"}
    /
    (
      varpulis_cluster_workers_total{status="ready"}
      + varpulis_cluster_workers_total{status="unhealthy"}
    )
  )[30d:5m]
)
```

#### SLO 3 — Filter Latency p99

```promql
# p99 processing latency per stream (5-minute rate window)
histogram_quantile(
  0.99,
  sum(rate(varpulis_processing_latency_seconds_bucket[5m])) by (le, stream)
)
```

Fraction of 5-minute windows where p99 was within target (used for burn rate):

```promql
# Good evaluation windows: p99 < 10ms
(
  histogram_quantile(
    0.99,
    sum(rate(varpulis_processing_latency_seconds_bucket[5m])) by (le, stream)
  ) < 0.010
) or vector(1)
```

#### SLO 4 — SASE Latency p99

```promql
# p99 for pattern (SASE) streams — same metric, different threshold
histogram_quantile(
  0.99,
  sum(rate(varpulis_processing_latency_seconds_bucket[5m])) by (le, stream)
) > 0.100
```

The alert rule `VarpulisHighProcessingLatency` in `deploy/prometheus/alerts.yml` already
fires at exactly this threshold (0.1 seconds for 5 minutes).

#### SLO 5 — Throughput Floor

```promql
# Per-worker events processed per second
sum(rate(varpulis_events_processed[5m])) by (instance)
```

```promql
# Cluster-wide aggregate throughput
sum(rate(varpulis_events_processed[5m]))
```

Alert condition (throughput below floor per worker):

```promql
sum(rate(varpulis_events_processed[5m])) by (instance) < 100000
```

#### SLO 6 — Event Processing Error Rate

```promql
# Error rate as a ratio (0.0 to 1.0)
1 - (
  sum(rate(varpulis_events_processed[5m]))
  /
  sum(rate(varpulis_events_total[5m]))
)
```

```promql
# Error rate as a percentage — SLO breached when > 0.1%
(
  1 - (
    sum(rate(varpulis_events_processed[5m]))
    /
    sum(rate(varpulis_events_total[5m]))
  )
) * 100 > 0.1
```

Note: the existing alert `VarpulisHighErrorRate` fires at > 1% (10x the SLO threshold).
The 0.1% SLO is tracked separately for budget accounting.

#### SLO 7 — DLQ Delivery Error Rate

```promql
# Fraction of output events that ended up in the DLQ
sum(rate(varpulis_dlq_events_total[5m]))
/
sum(rate(varpulis_output_events_total[5m]))
```

#### SLO 8 — Leader Failover Time

This cannot be expressed directly in PromQL because it requires knowing the exact instant
the leader became unavailable. Use the following proxy to detect failover events and measure
duration:

```promql
# Time series of Raft role changes (1 when leader, 0 otherwise)
varpulis_cluster_raft_role == 2

# Number of leader elections in a rolling 15-minute window
changes(varpulis_cluster_raft_role[15m])
```

For post-incident measurement, query the gap in leader continuity:

```promql
# Periods with no leader elected (all nodes are follower or candidate)
sum(varpulis_cluster_raft_role == 2) == 0
```

#### SLO 9 — Pipeline Migration p99

```promql
# p99 migration duration for successful migrations
histogram_quantile(
  0.99,
  sum(rate(varpulis_cluster_migration_duration_seconds_bucket[5m])) by (le, result)
)
```

```promql
# Migration failure rate (should be 0)
rate(varpulis_cluster_migrations_total{result="failure"}[5m])
```

---

## 5. Error Budget Policy

### Error budget calculation

An error budget is the complement of the SLO: the amount of unreliability that is
acceptable over the measurement window.

```
error_budget = (1 - SLO_target) * window_duration

Example: Coordinator availability (SLO 1)
  error_budget = (1 - 0.999) * 30 days * 24 hours * 60 min = 43.2 minutes/month
```

### Budget consumption and policy tiers

| Budget consumed | Status | Policy |
|-----------------|--------|--------|
| < 50% | Green | Normal operations. Feature work proceeds at full pace. |
| 50% – 75% | Yellow | Reliability review in the next sprint. Identify top incident contributors. Increase test coverage for affected components. |
| 75% – 95% | Orange | Reliability freeze. No new features merged to production until root cause of consumption is resolved and a remediation plan is reviewed. On-call rotations reviewed. |
| > 95% (breached or at risk) | Red | Incident declared. All engineering focus shifts to reliability. Postmortem required within 5 business days. SLO targets reviewed for realism. |

### Budget reset events

The following events reset the budget consumption clock:

- A new 30-day window begins (rolling window, so this is continuous)
- The SLO target is revised downward (agreed with stakeholders after postmortem)
- An infrastructure tier change (e.g., scaling from 2 to 4 workers) that materially
  improves the SLI baseline

### Exclusions from budget consumption

The following periods are excluded from budget calculation when approved in advance:

- Announced maintenance windows (max 4 hours per calendar month, requires 48-hour notice)
- Force-majeure infrastructure events (cloud provider outages affecting the entire region)
- Events caused by customer misconfiguration rather than engine defects

Exclusions must be recorded in the incident log with a start/end timestamp before they
are applied.

---

## 6. Grafana SLO Dashboard

The SLO dashboard supplements the existing `varpulis.json` and `varpulis-cluster.json`
dashboards. It provides burn rate panels that surface budget consumption faster than
simple threshold alerts.

### Multi-window burn rate

Google SRE recommends alerting on SLO burn rate across two time windows simultaneously
to reduce alert noise while maintaining sensitivity to fast budget drain. The standard
windows are 1-hour/5-minute (fast burn detection) and 6-hour/30-minute (slow burn
detection).

**Burn rate definition:**

```
burn_rate = error_rate_in_window / (1 - SLO_target)

A burn rate of 1 means you are exactly on track to use 100% of the budget.
A burn rate of 14.4 over 1 hour means you will exhaust a 30-day budget in 2 hours.
```

### Panel definitions

#### Panel 1: Coordinator Availability Burn Rate (6h / 1h windows)

```promql
# 6-hour window burn rate
(1 - avg_over_time(up{job="varpulis-coordinator"}[6h])) / (1 - 0.999)

# 1-hour window burn rate (fast burn)
(1 - avg_over_time(up{job="varpulis-coordinator"}[1h])) / (1 - 0.999)
```

Alert thresholds (based on Google's multiwindow model, 30-day budget):

| Condition | Severity | Meaning |
|-----------|----------|---------|
| 6h burn rate > 14.4 AND 1h burn rate > 14.4 | critical | Exhausts monthly budget in 2 hours |
| 6h burn rate > 6 AND 1h burn rate > 6 | warning | Exhausts monthly budget in 5 hours |
| 6h burn rate > 3 AND 30m burn rate > 3 | warning | Exhausts monthly budget in 10 hours |
| 6h burn rate > 1 | info | Burning faster than sustainable |

#### Panel 2: Event Processing Error Budget (30-day rolling)

```promql
# Budget remaining as a percentage of total budget
(
  0.001                                         -- SLO allowance (0.1%)
  - (
      1 - (
        sum(rate(varpulis_events_processed[30d]))
        /
        sum(rate(varpulis_events_total[30d]))
      )
    )
) / 0.001 * 100
```

Display as a gauge: green above 50%, yellow 25-50%, red below 25%.

#### Panel 3: Processing Latency SLO Compliance (30-day)

Since `varpulis_processing_latency_seconds` is a histogram, compliance is computed using
the ratio of good requests (latency within budget) to total requests:

```promql
# Good requests: processed within 100ms (SASE target)
sum(rate(varpulis_processing_latency_seconds_bucket{le="0.1"}[30d]))
/
sum(rate(varpulis_processing_latency_seconds_count[30d]))
```

```promql
# Good requests: processed within 10ms (filter target)
sum(rate(varpulis_processing_latency_seconds_bucket{le="0.01"}[30d]))
/
sum(rate(varpulis_processing_latency_seconds_count[30d]))
```

#### Panel 4: Latency Burn Rate (6h window)

```promql
# SASE latency burn rate (threshold = 100ms, SLO = 99.9% compliance)
(
  1 - (
    sum(rate(varpulis_processing_latency_seconds_bucket{le="0.1"}[6h]))
    /
    sum(rate(varpulis_processing_latency_seconds_count[6h]))
  )
) / 0.001
```

#### Panel 5: Worker Pool Health Timeline

```promql
# Worker availability ratio over time (for burn rate visualization)
varpulis_cluster_workers_total{status="ready"}
/
(
  varpulis_cluster_workers_total{status="ready"}
  + varpulis_cluster_workers_total{status="unhealthy"}
)
```

```promql
# Worker pool burn rate (6h)
(
  1 - avg_over_time(
    (
      varpulis_cluster_workers_total{status="ready"}
      /
      (
        varpulis_cluster_workers_total{status="ready"}
        + varpulis_cluster_workers_total{status="unhealthy"}
      )
    )[6h:1m]
  )
) / 0.005
```

#### Panel 6: DLQ Accumulation Rate

```promql
# Events entering DLQ per minute
sum(rate(varpulis_dlq_events_total[5m])) * 60

# DLQ burn rate relative to the 0.5% budget
(
  sum(rate(varpulis_dlq_events_total[1h]))
  /
  sum(rate(varpulis_output_events_total[1h]))
) / 0.005
```

#### Panel 7: SASE Active Runs vs Threshold

```promql
# Current peak active SASE runs (memory saturation indicator)
varpulis_sase_peak_active_runs

# Alert band at 10K (existing alert threshold)
# Show as a reference line at 10000
```

#### Panel 8: Migration Latency p99 vs SLO

```promql
# p99 migration duration (target < 10s)
histogram_quantile(
  0.99,
  sum(rate(varpulis_cluster_migration_duration_seconds_bucket[5m])) by (le)
)
```

### Alert rules for SLO burn rates

Add the following groups to `deploy/prometheus/alerts.yml`:

```yaml
- name: varpulis_slo_burn_rate
  rules:
    # Coordinator availability: fast burn (2-hour exhaustion)
    - alert: VarpulisCoordinatorBurnRateCritical
      expr: >
        (1 - avg_over_time(up{job="varpulis-coordinator"}[6h])) / (1 - 0.999) > 14.4
        and
        (1 - avg_over_time(up{job="varpulis-coordinator"}[1h])) / (1 - 0.999) > 14.4
      labels:
        severity: critical
        slo: coordinator_availability
      annotations:
        summary: "Coordinator SLO burning at critical rate"
        description: >
          At the current error rate, the monthly coordinator availability budget
          will be exhausted in approximately 2 hours.

    # Coordinator availability: slow burn (5-hour exhaustion)
    - alert: VarpulisCoordinatorBurnRateWarning
      expr: >
        (1 - avg_over_time(up{job="varpulis-coordinator"}[6h])) / (1 - 0.999) > 6
        and
        (1 - avg_over_time(up{job="varpulis-coordinator"}[1h])) / (1 - 0.999) > 6
      for: 5m
      labels:
        severity: warning
        slo: coordinator_availability
      annotations:
        summary: "Coordinator SLO burning at elevated rate"

    # Error rate SLO burn (0.1% target)
    - alert: VarpulisErrorBudgetBurnRateCritical
      expr: >
        (
          1 - (
            sum(rate(varpulis_events_processed[6h]))
            / sum(rate(varpulis_events_total[6h]))
          )
        ) / 0.001 > 14.4
        and
        (
          1 - (
            sum(rate(varpulis_events_processed[1h]))
            / sum(rate(varpulis_events_total[1h]))
          )
        ) / 0.001 > 14.4
      labels:
        severity: critical
        slo: event_error_rate
      annotations:
        summary: "Event processing error budget burning critically fast"

    # Latency SLO burn (SASE p99 < 100ms)
    - alert: VarpulisLatencyBudgetBurnRateCritical
      expr: >
        (
          1 - (
            sum(rate(varpulis_processing_latency_seconds_bucket{le="0.1"}[6h]))
            / sum(rate(varpulis_processing_latency_seconds_count[6h]))
          )
        ) / 0.001 > 14.4
      labels:
        severity: critical
        slo: sase_latency_p99
      annotations:
        summary: "SASE latency SLO burning critically fast"
        description: >
          More than 0.1% of event processing operations are exceeding the 100ms
          p99 latency target at a rate that will exhaust the monthly budget in ~2 hours.
```

---

## 7. Review Cadence

| Activity | Frequency | Owner |
|----------|-----------|-------|
| SLO status review (budget consumption) | Weekly (Monday standup) | On-call engineer |
| Error budget report to stakeholders | Monthly | Engineering lead |
| SLO target review and adjustment | Quarterly | Engineering + product |
| Postmortem after SLO breach | Within 5 business days of breach | Incident commander |
| Dashboard and alert rule audit | Quarterly (coincides with target review) | Platform team |

### Related documents

- `docs/operations/alerting.md` — alert rule descriptions and investigation guidance
- `docs/operations/runbook.md` — step-by-step operational procedures
- `deploy/prometheus/alerts.yml` — live alert rule definitions
- `deploy/docker/grafana/dashboards/varpulis.json` — engine metrics dashboard
- `deploy/docker/grafana/dashboards/varpulis-cluster.json` — cluster health dashboard
