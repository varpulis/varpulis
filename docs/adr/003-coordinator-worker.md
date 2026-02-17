# ADR-003: Coordinator/Worker Cluster Architecture

**Status:** Accepted
**Date:** 2026-02-17
**Authors:** Varpulis Team

## Context

A Varpulis runtime instance (`varpulis server`) is a self-contained process: it parses VPL, compiles it to a pipeline graph, manages pattern-matching state, and handles event ingestion from connectors. For many use cases, a single server is sufficient. However, several requirements push toward multi-process deployment:

1. **Horizontal scale**: A single server process is bounded by one machine's CPU and memory. Event workloads may outgrow a single host.
2. **Pipeline isolation**: Long-running pipelines in a shared service should not interfere with each other; a runaway pipeline on one process should not starve another.
3. **Fault tolerance**: A single server is a single point of failure. Events in flight when it crashes are lost.
4. **Deployment lifecycle**: Updating a pipeline (redeploying new VPL) currently requires dropping its state. In a multi-process model, a new pipeline can be started on a fresh worker while the old one drains.

The architecture must also preserve backward compatibility: existing users running a single `varpulis server` must not be forced to adopt cluster mode.

## Decision

Varpulis adopts a **Coordinator/Worker** architecture for distributed deployment.

### Roles

**Workers** (`varpulis server --coordinator <url>`) are standard single-server processes. They self-register with the coordinator on startup via `POST /api/v1/cluster/workers/register` and send periodic heartbeats (`POST /api/v1/cluster/workers/{id}/heartbeat`) every 5 seconds. Workers run assigned pipelines using their existing ContextOrchestrator and are accessible via their own REST/WebSocket API.

**The Coordinator** (`varpulis coordinator`) is a stateless control-plane process. It:
- Maintains a registry of live workers with their capacity (`WorkerNode`, `WorkerCapacity`)
- Accepts pipeline-group deployment requests from operators and distributes pipelines to workers using a placement strategy
- Routes externally injected events to the correct worker based on the group's routing table
- Monitors worker health via heartbeat timeouts and marks workers `Unhealthy` after 15 seconds of silence
- Exposes the cluster topology and aggregated metrics via REST

The coordinator does **not** process events itself. Event data flows directly from external producers to worker APIs; the coordinator only routes management traffic (deploy, delete, inject-one-off) and heartbeats.

### Pipeline Groups

Pipelines are deployed in **Pipeline Groups** (`PipelineGroupSpec`): named collections of VPL pipelines with explicit routing rules. A routing rule maps a wildcard event-type pattern (e.g., `ComputeTile0*`) to a named pipeline within the group. When an event is injected via the coordinator, the routing table resolves which pipeline (and therefore which worker) receives it.

### Placement Strategies

The coordinator uses a pluggable `PlacementStrategy` trait:
- **RoundRobinPlacement** (default): distributes pipelines sequentially across available workers
- **LeastLoadedPlacement**: assigns to the worker with the fewest running pipelines
- **Worker affinity**: a `worker_affinity` field in `PipelinePlacement` pins a pipeline to a specific worker ID, falling back to the default strategy if that worker is unavailable

### High Availability (optional)

When the `raft` feature is enabled, coordinators can form a Raft consensus group using `openraft`. Write operations (deploy, delete) go through the Raft leader; followers proxy requests. This is an optional overlay on top of the base coordinator/worker model.

On Kubernetes, the `k8s` feature enables a `k8s_watcher` that promotes/demotes coordinators to leader/follower roles based on a Kubernetes leader-election lease.

### Inter-Pipeline Communication

Pipelines in different workers communicate via MQTT (the same connector infrastructure already used for external event ingestion). The coordinator generates MQTT topics for each routing rule; workers subscribe to their assigned topics automatically. This reuses existing infrastructure and avoids introducing a proprietary inter-process protocol.

### Backward Compatibility

When `--coordinator` is not set, `varpulis server` runs in standalone mode. The cluster crate (`varpulis-cluster`) is a separate dependency; it is only compiled into the coordinator binary and not required by standalone workers.

## Alternatives Considered

### Peer-to-Peer (Gossip-based) Architecture

In a P2P model, every node knows about every other node via a gossip protocol (e.g., SWIM). Deployment and routing decisions are made collectively, without a single coordinator.

Rejected because:
- Gossip protocols converge eventually but not immediately. The time between deploying a pipeline and all nodes agreeing on its location creates a window of inconsistency.
- Event routing in a gossip model requires every node to maintain a full routing table and update it as the cluster evolves. For large clusters, this overhead is non-trivial.
- Implementing a correct gossip protocol (membership, failure detection, anti-entropy) is significantly more complex than a coordinator managing a worker registry. The coordinator's single-writer model for state changes is easy to reason about.
- The coordinator can be made HA via Raft. A P2P model would require solving the same consensus problem at every node.

### Single-Process with Thread Parallelism

Instead of multiple processes, VPL pipelines could run as isolated Tokio tasks or OS threads within a single process, using the existing context/ContextOrchestrator model.

Rejected as the primary scaling strategy because:
- A single process shares one memory space. A runaway pipeline (memory leak, CPU-intensive Kleene enumeration) degrades all other pipelines in the process.
- Scaling to multiple machines is impossible without a multi-process model. The coordinator/worker model solves both intra-machine (multiple workers on one host) and inter-machine scaling.
- That said, the context system (`context ingest (cores: [0, 1])`) remains the recommended mechanism for fine-grained within-process parallelism. The coordinator/worker model operates at a coarser granularity (worker processes), and the two are complementary: each worker uses the context system internally.

### Kubernetes Operator Pattern (Custom Resource Definitions)

An alternative is to express Varpulis clusters purely as Kubernetes resources, using a custom controller to reconcile `VarpulisPipeline` CRDs into Pods.

Rejected as the primary architecture because:
- It requires Kubernetes, excluding Docker, bare-metal, and on-premises deployments.
- The coordinator/worker model is deployable everywhere: local laptop, Docker Compose, Kubernetes, VM. Kubernetes support is layered on top via Helm charts and the optional `k8s` feature flag, not required.
- Implementing a Kubernetes controller correctly (reconcile loops, finalizers, status conditions) is a significant engineering investment that would delay shipping a functional cluster mode.

### Akka / Actor-Based Distributed System

An actor system (Akka in JVM, or equivalent in Rust via `actix`) would model each pipeline as an actor, with the actor framework handling distribution, supervision, and message routing.

Rejected because:
- Varpulis is designed for a Rust/Tokio async ecosystem. An actor framework adds a second concurrency model on top of Tokio.
- Actor systems do not have a natural fit for VPL's batch event injection and synchronous query semantics.
- The coordinator/worker model achieves supervision and routing via simpler constructs (health sweeps, routing tables, HTTP) without adding a framework dependency.

## Consequences

### Positive

- Single-server mode is fully preserved. Users with simple workloads need not adopt cluster mode; the `--coordinator` flag is opt-in.
- The coordinator is stateless with respect to event data. It holds only cluster topology state (worker registry, pipeline group specs), which is small and fast to reconstruct from worker heartbeats after a coordinator restart.
- Pipeline placement strategies are pluggable (`PlacementStrategy` trait). Adding a new strategy (e.g., topology-aware, GPU-affinity) does not require changing the coordinator API.
- Placement migrations (`MigrationTask`) are tracked explicitly: the coordinator can detect when a worker becomes unhealthy, pick a replacement worker, redeploy the pipeline there, and update the routing table atomically.
- The architecture is incrementally adoptable: start with one coordinator and one worker, scale to N workers as load grows, add a second coordinator with Raft when availability requirements increase.

### Negative

- The coordinator is an additional operational component. Deployments require running at least two processes (coordinator + worker) instead of one. Docker Compose and Helm charts mitigate this for standard deployments.
- The coordinator is a potential bottleneck for very high-rate event injection via the coordinator's inject endpoint. For production high-throughput use cases, events should be sent directly to worker APIs (or via MQTT); the coordinator inject path is intended for testing and one-off operations.
- Inter-pipeline communication via MQTT introduces an external dependency (an MQTT broker). In environments without MQTT, inter-pipeline routing is not available.
- Pipeline state is local to each worker. When a pipeline migrates (due to worker failure or rebalancing), its SASE+ NFA run state is lost unless the worker persisted a checkpoint before the migration. Checkpoint-based migration is partially implemented but not yet the default behavior.

## References

- `crates/varpulis-cluster/src/coordinator.rs` — Coordinator state machine
- `crates/varpulis-cluster/src/worker.rs` — Worker types and heartbeat protocol
- `crates/varpulis-cluster/src/routing.rs` — Event routing table with wildcard matching
- `crates/varpulis-cluster/src/health.rs` — Heartbeat failure detection
- `crates/varpulis-cluster/src/migration.rs` — Pipeline migration tracking
- `crates/varpulis-cluster/src/raft/` — Optional Raft consensus for coordinator HA
- `docs/architecture/cluster.md` — Full cluster architecture documentation
- [Raft Consensus Algorithm](https://raft.github.io/) — basis for coordinator HA
- [OpenRaft](https://docs.rs/openraft) — Rust Raft implementation used
