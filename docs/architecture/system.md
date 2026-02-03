# System Architecture

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   Varpulis Runtime Engine                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Compiler   │  │   Optimizer  │  │   Validator  │       │
│  │ (VarpulisQL) │  │              │  │              │       │
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
│  │   Engine     │  │   (RocksDB/  │  │   Engine     │       │
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

## Processing Flow

```
Event Sources → Ingestion → Embedding → Pattern Matching → Aggregation → Sink
                                 ↓              ↑
                            Attention      Hypertree
                             Scores       Structures
```

## Components

### Compiler
- Parse VarpulisQL via Pest PEG parser
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

### Pattern Matcher
- Hypertree structures for efficient matching
- Temporal pattern support
- Sequence detection

### Attention Engine
- See [attention-engine.md](attention-engine.md)

### Embedding Engine
- Vector generation for events
- Rule-based or learned mode

### State Manager
- See [state-management.md](state-management.md)

### Aggregation Engine
- Aggregation functions (sum, avg, count, min, max, stddev, etc.)
- Temporal windows (tumbling, sliding, session)
- Key-based grouping

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
