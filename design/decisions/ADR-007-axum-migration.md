# ADR-007: Axum Migration

**Status:** Accepted
**Date:** 2026-02-28
**Authors:** Varpulis Team

**Supersedes:** [ADR-002 (Warp as the HTTP Framework)](../../docs/adr/002-warp-http.md)

## Context

ADR-002 chose Warp as the HTTP framework for both the worker REST API and the coordinator control-plane API. That decision was sound at the time (pre-Axum 1.0), but the landscape has changed:

1. **Warp is in maintenance mode.** Warp 0.3 receives security patches but no new features. The project has not had a significant release since 2021. hyper 1.x support is not planned.

2. **hyper 1.x migration.** The broader Rust HTTP ecosystem is converging on `hyper` 1.x and `http` 1.x. Warp depends on `hyper` 0.14, which pulls in the older `http` 0.2 types. This causes duplicate `http`, `http-body`, and `hyper` versions in the dependency tree, as other crates (reqwest, tonic, tower-http) have migrated to the 1.x line.

3. **Tower middleware ecosystem.** Warp's middleware story is ad-hoc: rate limiting, tracing, and CORS are implemented as custom filters or manual handler code. The `tower` and `tower-http` ecosystem provides composable, reusable middleware layers (compression, tracing, CORS, timeout, request ID, body limits) that Axum integrates natively.

4. **Ecosystem alignment.** Axum is maintained by the Tokio team, the same team behind the runtime Varpulis depends on. tonic (gRPC, used for Raft RPC) and tower are designed to compose with Axum. Using Warp alongside these crates creates an impedance mismatch.

5. **Developer ergonomics.** Warp's `Filter` trait produces deeply nested type signatures that generate verbose and often opaque compiler error messages. Axum's extractor-based function handlers are more approachable and produce clearer errors.

6. **Dependency count.** A `cargo tree` audit showed ~15 duplicate crate versions attributable to the Warp / hyper 0.14 split. Migrating to Axum (which uses hyper 1.x) is expected to eliminate these duplicates, reducing compile times and binary size.

## Decision

Varpulis migrates both HTTP APIs (worker and coordinator) from Warp to **Axum** (`axum` 0.8.x, backed by `hyper` 1.x and `tower` 0.4).

### Migration patterns

The following table maps Warp patterns to their Axum equivalents as applied during the migration:

| Warp Pattern | Axum Equivalent |
|---|---|
| `warp::path("api").and(warp::path("v1"))` | `Router::new().nest("/api/v1", sub_router)` |
| `warp::get().and(warp::path::param::<String>())` | `async fn handler(Path(id): Path<String>)` |
| `warp::body::json::<T>()` | `Json<T>` extractor in handler signature |
| `warp::body::content_length_limit(N)` | `DefaultBodyLimit::max(N)` layer or `RequestBodyLimit` |
| `warp::query::<T>()` | `Query<T>` extractor in handler signature |
| `warp::ws()` | `axum::extract::ws::WebSocketUpgrade` extractor |
| `warp::reply::json(&v)` | `axum::Json(v)` (implements `IntoResponse`) |
| `with_manager(arc.clone())` filter | `State(arc)` extractor via `Router::with_state()` |
| `warp::cors()` | `tower_http::cors::CorsLayer` |
| `handle_rejection()` | `IntoResponse` impls on error types |
| `.and().or()` filter chains | `Router::route().route()` method chains |
| `warp::serve().tls()` | `axum_server::tls_rustls::RustlsConfig` or `rustls` with `hyper` directly |

### Key architectural changes

**Shared state**: Warp passes state through `Filter` combinators (`with_manager`, `with_coordinator`). Axum uses `State(T)` as a function extractor, backed by `Router::with_state(arc)`. The `Arc<RwLock<T>>` pattern for the tenant manager and coordinator state is preserved; only the injection mechanism changes.

**Error handling**: Warp's `Rejection` type is opaque and requires a `handle_rejection` function at the end of the filter chain to convert rejections into HTTP responses. Axum uses the `IntoResponse` trait: error types implement `IntoResponse` directly, returning the appropriate status code and body. The existing `ApiError` enum gains an `IntoResponse` implementation, replacing the rejection handler.

**Middleware stack**: The `tower::ServiceBuilder` is used to compose middleware layers in a declarative stack:
- `tower_http::trace::TraceLayer` for distributed tracing (replacing manual `tracing::instrument` in handlers)
- `tower_http::cors::CorsLayer` for CORS (replacing `warp::cors()`)
- `tower_http::limit::RequestBodyLimitLayer` for body size limits (replacing `warp::body::content_length_limit()`)
- `tower_http::compression::CompressionLayer` for response compression (new capability)

**WebSocket**: The coordinator's WebSocket endpoint for worker connections (`ws.rs`) migrates from `warp::ws()` to `axum::extract::ws::WebSocketUpgrade`. The upgrade mechanism is similar; the primary change is syntactic.

**TLS**: Warp's built-in TLS builder is replaced by `axum-server` with `RustlsConfig` for HTTPS termination. mTLS configuration (client CA verification) is handled at the `rustls::ServerConfig` level, preserving the existing security properties.

**Feature-gated Raft routes**: Warp's `.or(raft_routes)` combinator is replaced by `Router::merge(raft_router)` behind the `#[cfg(feature = "raft")]` gate. The conditional composition is equivalent.

### Dependency reduction

The migration targets elimination of the following duplicate crate families:

| Crate family | Before (Warp) | After (Axum) |
|---|---|---|
| `hyper` | 0.14 + 1.x | 1.x only |
| `http` | 0.2 + 1.x | 1.x only |
| `http-body` | 0.4 + 1.x | 1.x only |
| `h2` | 0.3 + 0.4 | 0.4 only |
| `tokio-rustls` | 0.24 + 0.26 | 0.26 only |
| `rustls` | 0.21 + 0.23 | 0.23 only |

Target: approximately 15 fewer duplicate crate versions in `Cargo.lock`, reducing clean build time and binary size.

## Alternatives Considered

### Stay on Warp

The simplest option: change nothing. Warp 0.3 is stable, the existing code works, and the migration carries risk.

Rejected because:
- The dependency duplication cost is real and growing. Every new crate that depends on hyper 1.x adds another pair of duplicates.
- Warp's maintenance-mode status means security vulnerabilities in Warp's own code (not hyper, which is actively maintained) may not be patched promptly.
- The tower middleware ecosystem is actively expanding (rate limiting, request ID, sensitive headers); staying on Warp means re-implementing these features as custom filters.
- ADR-002 itself noted that the Axum migration was a "known future consideration." The dependency duplication has now made it a practical necessity.

### Poem

Poem is a modern Rust HTTP framework with good ergonomics and built-in OpenAPI support.

Rejected because:
- Poem is not part of the Tokio/tower ecosystem. It has its own middleware model that does not compose with `tower::Layer`, eliminating the benefit of shared middleware with tonic and other tower-based crates.
- Poem's community and adoption are smaller than Axum's, increasing the risk of encountering undocumented edge cases.
- Poem uses its own `http` types in some places, which would not eliminate the dependency duplication problem.

### Raw hyper 1.x

Using `hyper` 1.x directly with `tower` for routing and middleware, without a framework.

Rejected because:
- hyper 1.x deliberately removed the high-level server API. Building an HTTP server on raw hyper 1.x requires manually accepting connections, managing TLS, and routing requests. Axum provides this structure without hiding the underlying tower/hyper primitives.
- The amount of boilerplate for routing, body extraction, and error handling would exceed the current Warp codebase.

### Gradual migration (Warp + Axum coexistence)

Run Warp and Axum servers side-by-side during migration, with a reverse proxy routing traffic.

Rejected as the primary strategy because:
- It doubles the number of HTTP listeners, TLS configurations, and port allocations during the migration window.
- Shared state (tenant manager, coordinator state) would need to be accessible from both framework's handler patterns, complicating the state management.
- The worker and coordinator APIs are independent codebases in separate crates, so they can be migrated one at a time without requiring coexistence within a single server process.

## Consequences

### Positive

- A single hyper version (1.x) across the entire dependency tree eliminates ~15 duplicate crate versions, reducing `cargo build` time for clean builds and shrinking the binary.
- Tower middleware layers (tracing, CORS, compression, body limits) are declarative and composable. Adding a new middleware (e.g., request ID propagation, sensitive header scrubbing) is a one-line addition to the `ServiceBuilder` stack.
- Axum's function-handler model produces shorter and clearer code. Handlers are `async fn` with typed extractors as arguments, rather than chains of `.and()` combinators that must be mentally traced to understand what data reaches the handler.
- Error handling via `IntoResponse` is more natural than Warp's rejection model. Error types carry their HTTP status and body representation directly, making error response behavior visible at the type definition site rather than in a distant `handle_rejection` function.
- Axum's `Router::merge()` and `Router::nest()` compose cleanly with feature gates (`#[cfg(feature = "raft")]`), preserving the conditional Raft endpoint inclusion from the Warp implementation.
- The migration aligns Varpulis with the broader Tokio ecosystem direction, making it easier to adopt future tower-based crates (tower-sessions, tower-governor for rate limiting, etc.) without adapter layers.

### Negative

- The migration requires touching all HTTP handler code in `varpulis-cli/src/api.rs` and `varpulis-cluster/src/api.rs`. While the changes are mechanical (filter chains to function handlers), the volume increases the risk of introducing regressions in request parsing or response formatting.
- Warp's compile-time type safety for filter composition (a route that forgets to extract a required value fails to compile) is partially lost. Axum catches some extractor errors at compile time (wrong number of extractors) but others (extracting from a missing state) surface as runtime panics on the first request.
- TLS configuration moves from Warp's integrated builder to a separate `axum-server` crate or manual `rustls` setup. This is more flexible but also more verbose and requires understanding the rustls configuration API directly.
- The WebSocket API change (`warp::ws::Message` to `axum::extract::ws::Message`) requires updating all WebSocket message handling code, including the coordinator's worker connection management.
- Integration tests that assert on HTTP response behavior need to migrate from `warp::test::request()` to `axum::test` (or use an HTTP client like `reqwest` against a test server), requiring test infrastructure changes.

## References

- [Axum documentation](https://docs.rs/axum)
- [Tower documentation](https://docs.rs/tower)
- [tower-http documentation](https://docs.rs/tower-http)
- [hyper 1.0 announcement](https://hyper.rs/blog/2023/11/15/hyper-v1/)
- [ADR-002](../../docs/adr/002-warp-http.md) -- Original Warp decision (now superseded)
- `crates/varpulis-cli/src/api.rs` -- Worker REST API (migration target)
- `crates/varpulis-cluster/src/api.rs` -- Coordinator REST API (migration target)
- `crates/varpulis-core/src/security.rs` -- Body limit constants shared by both APIs
- [ADR-006](ADR-006-actor-framework.md) -- Actor framework that increases the number of concurrent components requiring HTTP endpoints
