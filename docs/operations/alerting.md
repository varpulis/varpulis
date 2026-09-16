# Varpulis Alerting Reference

This document describes the Prometheus alerting rules defined in
`deploy/prometheus/alerts.yml` and provides guidance on investigating
and resolving each alert.

## Engine Alerts

### VarpulisHighProcessingLatency

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | p99 processing latency > 100ms for 5 minutes |
| Metric | `varpulis_processing_latency_seconds` (histogram) |

**Cause:** A stream's event processing pipeline is taking longer than expected. Common
causes include expensive `.where()` predicates, large aggregation windows, or high
Kleene closure fan-out.

**Response:**
1. Identify the affected stream from the `stream` label.
2. Check if the stream has unbounded Kleene patterns -- add `.within()` constraints.
3. Review aggregation window sizes -- consider reducing window duration.
4. Check system CPU and I/O load on the worker hosting the stream.

### VarpulisHighErrorRate

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | > 1% of events failing processing for 5 minutes |
| Metrics | `varpulis_events_total`, `varpulis_events_processed` |

**Cause:** Events are being received but not successfully processed. This can indicate
schema mismatches, runtime evaluation errors, or connector delivery failures.

**Response:**
1. Check application logs for processing errors.
2. Verify event schemas match the VPL event type definitions.
3. Check if a recent pipeline deployment introduced a bug.

### VarpulisCriticalErrorRate

| Field | Value |
|-------|-------|
| Severity | critical |
| Condition | > 5% of events failing processing for 5 minutes |
| Metrics | `varpulis_events_total`, `varpulis_events_processed` |

**Cause:** Same as VarpulisHighErrorRate but at a severity requiring immediate action.

**Response:**
1. Consider rolling back the most recent pipeline deployment.
2. Check dead letter queue files for patterns in the failing events.
3. Escalate if the root cause is not immediately apparent.

### VarpulisStreamQueueBacklog

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Stream queue size > 10,000 events for 5 minutes |
| Metric | `varpulis_stream_queue_size` (gauge) |

**Cause:** The engine is not consuming events as fast as they arrive. Backpressure
is building up in the stream's input queue.

**Response:**
1. Check processing latency for the affected stream.
2. Consider adding more workers and partitioning the stream with `.partition_by()`.
3. Verify upstream sources are not sending burst traffic.

### VarpulisSaseRunBacklog

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Peak active SASE runs > 10,000 for 5 minutes |
| Metric | `varpulis_sase_peak_active_runs` (gauge) |

**Cause:** The SASE pattern engine has accumulated a large number of concurrent partial
matches. This typically happens with broad patterns that lack window constraints.

**Response:**
1. Add or tighten `.within()` time constraints on sequence patterns.
2. Add more selective `.where()` predicates to reduce match candidates.
3. If using Kleene closures, verify `max_kleene_events` is appropriately bounded.

### VarpulisNoEventsReceived

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Zero events received for 10 minutes |
| Metric | `varpulis_events_total` (counter) |

**Cause:** No events are arriving at the engine. Upstream sources or connectors
may be down.

**Response:**
1. Check connector health status.
2. Verify MQTT broker or Kafka cluster availability.
3. Check network connectivity between Varpulis and the event source.

## Cluster Alerts

### VarpulisWorkerUnhealthy

| Field | Value |
|-------|-------|
| Severity | critical |
| Condition | Any workers in "unhealthy" status for 2 minutes |
| Metric | `varpulis_cluster_workers_total{status="unhealthy"}` |

**Cause:** A worker has missed heartbeat deadlines. It may have crashed, lost
network connectivity, or be under extreme resource pressure.

**Response:**
1. Check if the worker process is still running.
2. Verify network connectivity between coordinator and worker.
3. Check worker host for resource exhaustion (CPU, memory, disk).
4. Pipelines will be auto-migrated; verify migration completes.

### VarpulisNoReadyWorkers

| Field | Value |
|-------|-------|
| Severity | critical |
| Condition | Zero ready workers for 1 minute |
| Metric | `varpulis_cluster_workers_total{status="ready"}` |

**Cause:** All workers are either down, draining, or unhealthy. No pipelines
can be executed.

**Response:**
1. Immediately check all worker processes and hosts.
2. Check coordinator logs for mass worker deregistration.
3. Verify no infrastructure-wide issue (network partition, DNS failure).

### VarpulisSplitBrain

| Field | Value |
|-------|-------|
| Severity | **critical** |
| Condition | `sum(varpulis_cluster_is_leader) > 1` for 1 minute |
| Metric | `varpulis_cluster_is_leader` |

**Cause:** more than one coordinator believes it holds the control-plane lease.
The lease is a compare-and-swap on a single key, so this should be impossible
for more than the moment it takes a stale holder's next renewal to be refused.
A minute of it means either two coordinators share a `--coordinator-id` — each
then accepts the other's renewal as its own, which is the one way past the CAS
— or the metric is being scraped from a coordinator that has stopped sweeping.

**Response:**
1. `curl /api/v1/cluster/consensus` on every coordinator and compare `leader`.
2. Check for duplicate `--coordinator-id` values (and for two pods sharing a
   `$HOSTNAME`, which is the default identity).
3. Writes are still safe — each is its own CAS — but stop the duplicate before
   it does anything at scale.

### VarpulisNoLeader

| Field | Value |
|-------|-------|
| Severity | **critical** |
| Condition | `sum(varpulis_cluster_is_leader) == 0` for 2 minutes |
| Metric | `varpulis_cluster_is_leader` |

**Cause:** no coordinator holds the lease. Reads still work everywhere; writes
are refused. Normal for the length of one TTL after a holder is killed
(`VARPULIS_CONTROL_PLANE_TTL_SECS`, default 30s); two minutes means the
coordinators cannot reach the control plane at all.

**Response:**
1. Check the NATS cluster, not the coordinators.
2. `nats kv status` on the control bucket: does it exist, and does it have the
   replication that was asked for?
3. See "Control-Plane Recovery" in the runbook.

### VarpulisLeadershipChurn

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | `increase(varpulis_cluster_leadership_changes_total[15m]) > 4` |
| Metric | `varpulis_cluster_leadership_changes_total` |

**Cause:** leadership is moving repeatedly. The usual cause is a health-sweep
interval too close to the bucket TTL: the holder's own record ages out between
its renewals, a standby acquires, and it bounces. The coordinator warns about
this configuration at startup.

**Response:**
1. Check `--heartbeat-interval` against `VARPULIS_CONTROL_PLANE_TTL_SECS`. The
   interval must be well under half the TTL.
2. Check latency to the NATS cluster — a renewal that times out stands the
   holder down, by design.
3. Check whether any coordinator is under enough CPU or I/O pressure to miss
   its own sweep.

### VarpulisMigrationFailures

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Any migration failures in 10 minutes |
| Metric | `varpulis_cluster_migrations_total{result="failure"}` |

**Cause:** Pipeline state migration between workers is failing. This can happen
when target workers are unreachable or have insufficient resources.

**Response:**
1. Check coordinator logs for migration error details.
2. Verify the target worker has capacity for additional pipelines.
3. Check if state checkpoints are corrupted or too large to transfer.

### VarpulisDeploymentFailures

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Any deployment failures in 10 minutes |
| Metric | `varpulis_cluster_deploy_duration_seconds_count{result="failure"}` |

**Cause:** Pipeline group deployments are failing on workers.

**Response:**
1. Validate VPL source with the `/api/v1/cluster/validate` endpoint.
2. Check connector configurations (broker addresses, credentials).
3. Verify workers have the required feature flags enabled.

### VarpulisSlowHealthSweeps

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Health sweep p99 > 50ms |
| Metric | `varpulis_cluster_health_sweep_duration_seconds` (histogram) |

**Cause:** The coordinator's periodic health check of all workers is slow.

**Response:**
1. Check coordinator CPU and network I/O.
2. Reduce the number of workers per coordinator if at scale.

## Infrastructure Alerts

### VarpulisHighMemoryUsage

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Process RSS > 2 GB for 5 minutes |
| Metric | `process_resident_memory_bytes` (standard Prometheus metric) |

**Cause:** The Varpulis process is consuming significant memory. Potential causes
include large event windows, many concurrent SASE runs, or connector buffer growth.

**Response:**
1. Check `varpulis_sase_peak_active_runs` for unbounded pattern growth.
2. Review window sizes and aggregation state.
3. Check stream queue sizes for backlog buildup.
4. Consider deploying more workers to distribute load.

### VarpulisCriticalMemoryUsage

| Field | Value |
|-------|-------|
| Severity | critical |
| Condition | Process RSS > 4 GB for 2 minutes |
| Metric | `process_resident_memory_bytes` |

**Cause:** Same as VarpulisHighMemoryUsage but at an urgent level.

**Response:**
1. Consider restarting the process with state persistence enabled.
2. Immediately investigate and address unbounded state growth.
3. Set container memory limits to prevent host-level OOM.

### VarpulisDlqGrowing

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | > 100 DLQ events in 10 minutes |
| Metric | `varpulis_dlq_events_total` (counter) |

**Cause:** Events are being written to the dead letter queue, indicating
that a sink connector is rejecting or failing to deliver events.

**Response:**
1. Check which connector is failing from the DLQ file entries.
2. Verify the downstream system (Kafka, database, HTTP endpoint) is operational.
3. Check circuit breaker state -- it may be open due to repeated failures.

### VarpulisDlqCritical

| Field | Value |
|-------|-------|
| Severity | critical |
| Condition | > 1,000 DLQ events in 10 minutes |
| Metric | `varpulis_dlq_events_total` (counter) |

**Cause:** A sink connector is experiencing sustained failures.

**Response:**
1. Immediately investigate the downstream system health.
2. Check the DLQ file for the error messages attached to each event.
3. If the downstream system is irrecoverable, consider deploying a fallback sink.
4. Plan to replay DLQ events once the sink is restored.

### VarpulisConnectorUnhealthy

| Field | Value |
|-------|-------|
| Severity | critical |
| Condition | Connector health check reports unhealthy for 2 minutes |
| Metric | `varpulis_connector_healthy` (gauge, 0 or 1) |

**Cause:** A connector has failed its health check. The circuit breaker may
be open, and events are likely being routed to the DLQ.

**Response:**
1. Check connector logs for connection errors.
2. Verify the upstream/downstream system is reachable.
3. Check credentials and TLS certificates.
4. The circuit breaker will attempt half-open probes automatically.

### VarpulisSlowDeploys

| Field | Value |
|-------|-------|
| Severity | warning |
| Condition | Deploy p99 latency > 30 seconds |
| Metric | `varpulis_cluster_deploy_duration_seconds` (histogram) |

**Cause:** Pipeline deployments are slow, affecting responsiveness of
hot-reload and new pipeline creation.

**Response:**
1. Check worker load and available resources.
2. Review the size of pipeline state being transferred.
3. Check network latency between coordinator and workers.
