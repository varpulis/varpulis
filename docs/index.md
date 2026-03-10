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

## Connectors

| Connector | Input | Output | Status |
|-----------|-------|--------|--------|
| **MQTT** | Yes | Yes | Production |
| **Kafka** | Yes | Yes | Production |
| **NATS** | Yes | Yes | Production |
| **PostgreSQL CDC** | Yes | No | Production |
| **HTTP** | No | Yes | Webhooks |
