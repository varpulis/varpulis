# System Architecture

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   Varpulis Runtime Engine                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Compiler   │  │   Optimizer  │  │   Validator  │       │
│  │ (VPL) │  │              │  │              │       │
│  └──────┬───────┘  └──────┬───────┘  └───────┬──────┘       │
│         │                 │                  │              │
│         └─────────────────▼──────────────────┘              │
│                    Execution Graph                          │
│         ┌───────────────────────────────────┐               │
│         │                                   │               │
├─────────▼───────────────────────────────────▼───────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Ingestion  │  │    Pattern   │  │   Attention  │       │
│  │    Layer     │──│    Matcher   │──│    Engine    │       │
│  │              │  │  (Hypertrees)│  │(Deterministic)│      │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Embedding   │  │   State Mgmt │  │ Aggregation  │       │
│  │   Engine     │  │  (RocksDB*/  │  │   Engine     │       │
│  │              │  │   In-Memory) │  │              │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Observability│  │ Parallelism  │  │  Checkpoint  │       │
│  │    Layer     │  │   Manager    │  │   Manager    │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐                         │
│  │   Context    │  │  Multi-      │                         │
│  │ Orchestrator │  │  Tenant Mgr  │                         │
│  │(thread isol.)│  │  (SaaS API)  │                         │
│  └──────────────┘  └──────────────┘                         │
└─────────────────────────────────────────────────────────────┘
```

*\* RocksDB requires the `persistence` feature flag: `cargo build --features persistence`*

## Processing Flow

```
Event Sources → Ingestion → Embedding → Pattern Matching → Aggregation → Output (.to)
                                 ↓              ↑
                            Attention      Hypertree
                             Scores       Structures
```

## Components

### Compiler
- Parse VPL via Pest PEG parser
- Generates IR (Intermediate Representation)
- Static optimizations

### Execution Graph
- DAG (Directed Acyclic Graph) of operations
- Intelligent scheduling
- Operator fusion when possible

### Ingestion Layer
- Source connectors (Kafka, files, HTTP, etc.)
- Deserialization (JSON, Avro, Protobuf)
- Schema validation

### Pattern Matcher (SASE+ with ZDD)
- NFA-based SASE+ engine for sequence and Kleene pattern detection
- **ZDD** (`varpulis-zdd` crate): compactly represents Kleene capture combinations — e.g., 100 matching events produce ~100 ZDD nodes instead of 2^100 explicit subsets
- Temporal constraints, negation, partition-by support

### Attention Engine
- See [attention-engine.md](attention-engine.md)

### Embedding Engine
- Vector generation for events
- Rule-based or learned mode

### State Manager
- See [state-management.md](state-management.md)

### Aggregation Engine (Hamlet)
- Aggregation functions (sum, avg, count, min, max, stddev, etc.)
- Temporal windows (tumbling, sliding, session)
- Key-based grouping
- **Hamlet** (`hamlet/` module): multi-query trend aggregation with graphlet-based sharing — O(1) per-event propagation, 3-100x faster than ZDD-based aggregation
- See [trend-aggregation.md](trend-aggregation.md)

### Parallelism Manager
- See [parallelism.md](parallelism.md)

### Context Orchestrator
- Named execution contexts with OS thread isolation
- CPU affinity pinning via `core_affinity`
- Cross-context routing via bounded `mpsc` channels
- Zero overhead when no contexts are declared
- See [contexts guide](../guides/contexts.md)

### Observability Layer
- See [observability.md](observability.md)

### Checkpoint Manager
- State snapshots
- Crash recovery
- S3, local filesystem support
