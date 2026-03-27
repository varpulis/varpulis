---
layout: home

hero:
  name: Varpulis
  text: Streaming Analytics Engine
  tagline: Real-time complex event processing with pattern matching, trend aggregation, and forecasting.
  actions:
    - theme: brand
      text: Get Started
      link: /tutorials/getting-started
    - theme: alt
      text: VPL Language
      link: /language/overview
    - theme: alt
      text: GitHub
      link: https://github.com/varpulis/varpulis

features:
  - icon: "\u26A1"
    title: SASE+ Pattern Matching
    details: Detect complex event sequences, Kleene closures, and temporal patterns in real-time streams.
  - icon: "\uD83D\uDCC8"
    title: Trend Aggregation
    details: Hamlet engine for multi-query aggregation with 100x speedup over naive approaches.
  - icon: "\uD83D\uDD2E"
    title: Pattern Forecasting
    details: PST-based prediction of event sequences with sub-microsecond inference latency.
  - icon: "\uD83C\uDF10"
    title: Distributed Execution
    details: Cluster mode with coordinator/worker architecture, Raft consensus, and horizontal scaling.
  - icon: "\uD83D\uDD12"
    title: Multi-Tenant SaaS
    details: Hierarchical tenancy, RBAC, per-tenant isolation with Kubernetes namespaces and Kafka topics.
  - icon: "\uD83D\uDCDD"
    title: VPL Language
    details: Declarative domain-specific language for event patterns, aggregations, joins, and connectors.
---

## What's New in v0.9.0

- **Interactive shell** — type VPL + events directly like a Python interpreter (`varpulis interactive`)
- **TUI mode** — split-pane terminal UI with topology, event stream, and metrics (`--tui`)
- **Pipeline trace** — explain mode showing per-event operator pass/block (`--trace`)
- **Schema inference** — generate event declarations from sample data (`varpulis infer`)
- **Watch mode** — auto-reload simulation on file changes (`--watch`)
- **`.alert()` operator** — webhook notifications with `{field}` template interpolation
- **Connector discovery** — `varpulis connector list/info/test`
- **Agent integration** — JSON-line protocol + MCP tools for AI-driven stream analysis
- **Pipeline graph API** — VPL-to-graph and graph-to-VPL endpoints for visual builders

[Full changelog](/development/STATUS) | [Getting started](/tutorials/getting-started)

## Why Varpulis?

**Varpulis** is a next-generation streaming analytics engine built in Rust for low-latency complex event processing.

```vpl
// Detect suspicious login patterns in real-time
event Login:
    user_id: string
    city: string
    success: bool

stream SuspiciousLogin = Login as a -> Login as b
    .within(5m)
    .where(a.user_id == b.user_id && a.city != b.city)
    .emit(user: a.user_id, from: a.city, to: b.city)
```

## Performance

| Benchmark | Varpulis | Apama | Advantage |
|-----------|----------|-------|-----------|
| Filter (100K events) | 234K/s | 199K/s | 1.2x faster |
| Sequence detection | 256K/s | 221K/s | 1.2x faster |
| Memory usage | 36-58 MB | 166-190 MB | **3-5x less** |
| Kleene match accuracy | 99.6K matches | 20K matches | **5x more complete** |

*Benchmarked on CPU-bound workloads (100K events, ramdisk, preloaded). See also the [Varpulis vs Flink CEP](/comparisons/varpulis-vs-flink) comparison.*

## Key Capabilities

- **14 production connectors** — MQTT, Kafka, NATS, Redis, PostgreSQL CDC, AWS Kinesis, S3, Pulsar, and more
- **Helm chart for Kubernetes** — deploy coordinator/worker clusters with autoscaling
- **RocksDB checkpointing** — durable state snapshots for exactly-once processing
- **Prometheus metrics** — built-in `/metrics` endpoint for monitoring and alerting
- **ONNX model scoring** — embed ML models directly in streaming pipelines
- **AES-256 encryption at rest** — secure credentials and sensitive configuration
- **OpenTelemetry tracing** — distributed trace propagation across pipeline stages
- **Multi-cluster federation** — coordinate workloads across geographically distributed clusters

## Connectors

| Connector | Input | Output | Status |
|-----------|-------|--------|--------|
| **MQTT** | Yes | Yes | Production |
| **Kafka** | Yes | Yes | Production |
| **NATS** | Yes | Yes | Production |
| **Redis** | Yes | Yes | Production |
| **PostgreSQL CDC** | Yes | No | Production |
| **PostgreSQL** | No | Yes | Production |
| **MySQL** | No | Yes | Production |
| **SQLite** | No | Yes | Production |
| **AWS Kinesis** | Yes | Yes | Production |
| **AWS S3** | Yes | Yes | Production |
| **Apache Pulsar** | Yes | Yes | Production |
| **Elasticsearch** | No | Yes | Production |
| **HTTP/Webhooks** | No | Yes | Production |
| **Console** | No | Yes | Debug |
