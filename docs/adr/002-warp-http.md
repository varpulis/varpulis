# ADR-002: Warp as the HTTP Framework

**Status:** Accepted
**Date:** 2026-02-17
**Authors:** Varpulis Team

## Context

Varpulis exposes HTTP APIs in two distinct roles:

1. **Worker API** (`crates/varpulis-cli/src/api.rs`): RESTful pipeline management endpoints (`/api/v1/pipelines/...`) served by each `varpulis server` process. These handle pipeline deployment, event injection (single and batch), checkpoint operations, metric queries, and tenant/quota management. The server also provides WebSocket connections for real-time event streaming.

2. **Coordinator API** (`crates/varpulis-cluster/src/api.rs`): Cluster control-plane endpoints (`/api/v1/cluster/...`) served by `varpulis coordinator`. These handle worker registration, heartbeat receipt, pipeline-group deployment, topology queries, and Raft RPC forwarding (when the `raft` feature is enabled).

Both services share requirements: TLS support (including mTLS), request body size limits, API key authentication via custom headers, JSON serialization, and WebSocket upgrades. They must run within Tokio's async runtime, since the core engine (`varpulis-runtime`) is async-first.

The HTTP framework was chosen at project inception and has remained stable across all subsequent feature additions (mTLS, RBAC, Raft, distributed tracing, rate limiting).

## Decision

Both APIs are implemented using **Warp** (`warp = { version = "0.3", features = ["tls"] }`).

Warp routes are composed as combinatorial `Filter` chains. Each route is a value of type `impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone`. The full route tree is assembled by chaining filters with `.and()` (conjunction) and `.or()` (alternative) and registered with `warp::serve()` at startup.

Key Warp features used:

- **Path combinators**: `warp::path("api").and(warp::path("v1")).and(warp::path("pipelines"))` builds hierarchical URL prefixes at compile time.
- **Method filters**: `warp::post()`, `warp::get()`, `warp::delete()` narrow routes by HTTP verb.
- **Body extraction**: `warp::body::content_length_limit(JSON_BODY_LIMIT).and(warp::body::json())` enforces size limits and deserializes JSON into typed request structs. `LARGE_BODY_LIMIT` is used for batch injection endpoints.
- **Query parameters**: `warp::query::<PaginationParams>()` deserializes URL query strings into typed structs.
- **Path parameters**: `warp::path::param::<String>()` captures typed URL segments (pipeline IDs, worker IDs).
- **WebSocket upgrade**: `warp::ws()` handles WebSocket handshake; the coordinator uses this for persistent worker connections in `ws.rs`.
- **TLS**: `warp::serve(...).tls().cert_path(...).key_path(...)` enables HTTPS. The `reqwest` HTTP client uses `rustls-tls` for outbound mTLS from coordinator to workers.
- **CORS**: `warp::cors()` is configured on the coordinator API, with production hardening expected via a reverse proxy.

Shared state (the tenant manager, coordinator state) is passed through filters using helper functions (`with_manager`, `with_coordinator`) that clone an `Arc<RwLock<T>>`.

## Alternatives Considered

### Axum

Axum is the HTTP framework produced by the Tokio team, released in 2021 as a successor to `warp`. It uses the `tower` ecosystem for middleware (`tower::ServiceBuilder`, `tower::Layer`) and `axum::Router` for route definitions.

Axum is the current community recommendation for new Rust HTTP services. It offers:
- Ergonomic extractors (function arguments, not filter chains)
- `tower` middleware composability
- Better integration with `tower-http` for tracing, compression, CORS

Warp was chosen over Axum because:
- **Project timeline**: Varpulis's HTTP API was designed before Axum reached 1.0 stability. Migrating would require rewriting all route handlers with no functional change.
- **Filter model is explicit**: Warp's combinatorial filter type makes the data flow through a route explicit in the type signature. The compiler rejects routes that fail to extract required values. Axum's macro-based routing and implicit extractor injection are ergonomically simpler but less type-visible.
- **Stability of the existing codebase**: Both the worker API (`api.rs` in `varpulis-cli`) and coordinator API (`api.rs` in `varpulis-cluster`) use the same Warp patterns. A migration would need to touch both, plus shared auth and middleware code.

The Warp-to-Axum migration is a known future consideration but carries no current functional gap. ADR-002 will be superseded if this migration is performed.

### Actix-web

Actix-web is a high-performance HTTP framework built on its own actor runtime (`actix`). It historically had the highest throughput in benchmarks.

Rejected because:
- Actix-web uses an actor model that is architecturally at odds with Tokio-native async code. Bridging between `actix` and Tokio requires careful thread boundary management.
- `varpulis-runtime` is designed around Tokio tasks and channels. Running Actix-web in the same process would require either running two runtimes or converting the runtime to `actix`.
- Actix-web's actor model adds conceptual overhead disproportionate to Varpulis's API surface, which is moderate-volume management traffic, not a high-concurrency web service.

### Hyper (raw)

`hyper` is the underlying HTTP/1.1 and HTTP/2 library that both Warp and Axum build on.

Rejected because:
- Using raw `hyper` requires manually routing requests, parsing path segments, deserializing bodies, and handling errors — all functionality that Warp provides declaratively.
- The resulting code would be larger and harder to maintain than the Warp filter DSL for equivalent functionality.

### Poem / Salvo / Ntex

Other frameworks were not evaluated in depth. They are smaller ecosystems with less community vetting, fewer TLS integration examples, and less proven production use.

## Consequences

### Positive

- Both APIs (worker and coordinator) use identical Warp patterns for authentication, body parsing, and error handling. Developers familiar with one can immediately read the other.
- Warp's type-level filter composition catches routing mistakes at compile time: a handler that expects a deserialized body but is attached to a route without `.and(warp::body::json())` fails to compile.
- The filter combinator model composes cleanly with the `raft` feature flag: `cluster_routes_with_raft` wraps the base `cluster_routes` with an `.or(raft_routes)` combinator, adding Raft RPC endpoints conditionally at compile time.
- `warp::body::content_length_limit` is applied uniformly before body parsing, enforcing the `JSON_BODY_LIMIT` (for standard requests) and `LARGE_BODY_LIMIT` (for batch injection) security constraints at the framework level rather than in handler code.
- TLS configuration (certificate path, key path, optional client CA for mTLS) is handled by the Warp TLS builder, keeping cryptographic concerns out of business logic.

### Negative

- Warp's `Filter` trait and its associated types are complex. Type errors in filter composition produce lengthy compiler messages that can be difficult to interpret. The pattern of extracting reusable filters into helper functions (`with_manager`, `with_coordinator`) mitigates this but does not eliminate it.
- Warp 0.3 is in maintenance mode. New features (HTTP/2 push, HTTP/3, native OTEL integration) will not be added upstream. The project is effectively in a stable-but-not-growing state.
- Rejection handling requires implementing a custom `handle_rejection` function to convert Warp's opaque `Rejection` type into structured `ApiError` JSON responses. This boilerplate exists in both `api.rs` files.
- Warp's middleware story (equivalent to Axum's `tower` layers) is weaker. Rate limiting (`rate_limit.rs`) and distributed tracing are applied manually in handler code rather than as declarative middleware layers.

## References

- [Warp documentation](https://docs.rs/warp)
- [Axum documentation](https://docs.rs/axum)
- `crates/varpulis-cli/src/api.rs` — Worker REST API routes
- `crates/varpulis-cluster/src/api.rs` — Coordinator REST API routes
- `crates/varpulis-core/src/security.rs` — `JSON_BODY_LIMIT`, `LARGE_BODY_LIMIT` constants
- `crates/varpulis-cli/src/rate_limit.rs` — Rate limiting middleware
