# Varpulis CEP REST API Changelog

This document tracks all REST API endpoint additions, modifications, deprecations, and removals for the Varpulis CEP engine. It is the authoritative reference for API consumers planning upgrades or integrations.

---

## Versioning Policy

### Semantic Versioning

Varpulis follows [Semantic Versioning 2.0.0](https://semver.org/). The REST API version is tied to the product version:

- **Major version** (`X.0.0`): Breaking changes to existing endpoints. Clients must update.
- **Minor version** (`0.X.0`): Backward-compatible additions (new endpoints, new optional fields in responses, new optional query parameters).
- **Patch version** (`0.0.X`): Bug fixes, performance improvements. No API contract changes.

### URL Prefix

All API endpoints are served under the `/api/v1/` prefix. The `v1` segment will only be incremented when a major breaking change requires a parallel API surface (e.g., `/api/v2/`). Both surfaces will be served simultaneously for a minimum of one major version cycle to allow client migration.

### Two API Surfaces

| Surface | Base Path | Authentication | Description |
|---------|-----------|----------------|-------------|
| SaaS API | `/api/v1/` | `x-api-key` (per-tenant) or `x-admin-key` | Multi-tenant pipeline management on a single node or worker. |
| Cluster API | `/api/v1/cluster/` | `x-api-key` (RBAC role resolved) | Coordinator-level operations across a distributed cluster. |

Health and readiness probes (`GET /health`, `GET /ready`) require no authentication and are not versioned.

### Backward Compatibility Commitment

Within a `v1` API surface:

- Existing endpoint paths will not be removed or renamed without a full deprecation cycle (see below).
- Existing required request fields will not be removed.
- Existing response fields will not be removed or have their types changed.
- New optional response fields may be added at any time without a version bump; clients must tolerate unknown fields.
- New optional query parameters may be added at any time.
- HTTP status codes for existing success paths will not change.

---

## Deprecation Policy

### Process

1. **Announcement**: A deprecated endpoint or field is announced in the changelog entry for the minor version in which the deprecation takes effect.
2. **Warning period**: Deprecated items are supported for a minimum of **two minor version releases** after the deprecation announcement before they are eligible for removal.
3. **Response headers**: Deprecated endpoints return a `Deprecation` header (RFC 8594) and a `Sunset` header with the planned removal date.
4. **Removal**: Removal only happens in a major version bump. The removal is documented in the changelog with a migration path.

### Header Format

```
Deprecation: true
Sunset: Sat, 01 Jan 2027 00:00:00 GMT
Link: <https://docs.varpulis.io/api-changelog>; rel="deprecation"
```

### Error Codes

Deprecated endpoints do not return errors; they continue to function normally until the sunset date. After the sunset date, they return `410 Gone` with body:

```json
{
  "error": "This endpoint has been removed. See the migration guide.",
  "code": "ENDPOINT_REMOVED"
}
```

---

## Changelog

---

### v0.3.0 (2026-02-17)

#### Security

- **mTLS support**: The coordinator and worker nodes now support mutual TLS. Clients may optionally present a client certificate; the server validates it against a configured CA bundle. No API contract changes — mTLS is a transport-layer concern.
- **RBAC roles** (`Admin`, `Operator`, `Viewer`): All Cluster API endpoints now enforce role-based access control via the `x-api-key` header. The required minimum role for each endpoint is documented in the OpenAPI spec (`docs/api/openapi.yaml`). Requests with insufficient privileges receive `403 Forbidden`. Previously, all cluster endpoints required only a valid key.
- **Secrets zeroization**: Secret material (API keys, connector credentials) is zeroed from memory on drop. No API contract changes.
- **Resource limits**: JSON request bodies are limited to a configurable maximum size (default 1 MB for standard endpoints, 10 MB for upload endpoints). Requests exceeding the limit receive `413 Request Entity Too Large`.

#### New Endpoints

| Method | Path | Role | Description |
|--------|------|------|-------------|
| `POST` | `/api/v1/cluster/validate` | Viewer | Validate a VPL source string. Returns parse diagnostics and semantic errors without deploying. |

**`POST /api/v1/cluster/validate` — Request:**
```json
{
  "source": "stream S = EventA as a -> EventB as b .within(5m) .emit(a.value)"
}
```

**`POST /api/v1/cluster/validate` — Response (valid):**
```json
{
  "valid": true,
  "diagnostics": []
}
```

**`POST /api/v1/cluster/validate` — Response (invalid):**
```json
{
  "valid": false,
  "diagnostics": [
    {
      "severity": "error",
      "line": 1,
      "column": 42,
      "message": "Unknown stream operator 'foo'",
      "hint": "Did you mean 'forecast'?"
    }
  ]
}
```

#### Changed Endpoints

- **All Cluster API endpoints**: RBAC enforcement added. Previously accepted any valid `x-api-key`; now returns `403 Forbidden` if the key's role is below the endpoint's minimum. This is a behavioral change but not a breaking contract change — the response shape is unchanged.
- **`POST /api/v1/cluster/connectors`**: Connector configuration now supports an optional `tls` block for mTLS settings. Existing configurations without `tls` continue to work.

#### Observability

- **Distributed tracing (OpenTelemetry)**: All inbound requests receive a `traceparent` / `tracestate` header if not already present. Outbound requests from the coordinator to workers propagate these headers. No client-facing API changes.
- **Circuit breaker**: Worker-to-coordinator requests now go through a circuit breaker. When a worker is unhealthy, inject calls to that worker's pipelines return `503 Service Unavailable` instead of timing out. Clients should implement exponential backoff on `503`.
- **Dead letter queue**: Events that fail processing are routed to a dead letter queue. No new endpoints in this release; the DLQ is accessible via connector configuration.
- **Kafka exactly-once delivery**: Kafka sink connectors now support `exactly_once: true` in their configuration. Idempotent producers and transactional delivery are configured automatically.

---

### v0.2.0 (2026-02-07)

#### New Endpoints

**SaaS API — Pipeline management:**

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/api/v1/pipelines` | `x-api-key` | Deploy a new pipeline from VPL source. |
| `GET` | `/api/v1/pipelines` | `x-api-key` | List pipelines for the authenticated tenant. Paginated. |
| `GET` | `/api/v1/pipelines/{id}` | `x-api-key` | Get a pipeline by ID. |
| `DELETE` | `/api/v1/pipelines/{id}` | `x-api-key` | Delete (undeploy) a pipeline. |
| `POST` | `/api/v1/pipelines/{id}/events` | `x-api-key` | Inject a single event into a pipeline. |
| `POST` | `/api/v1/pipelines/{id}/events-batch` | `x-api-key` | Inject a batch of events into a pipeline. |
| `GET` | `/api/v1/pipelines/{id}/metrics` | `x-api-key` | Get event processing metrics for a pipeline. |
| `POST` | `/api/v1/pipelines/{id}/reload` | `x-api-key` | Reload a pipeline with updated VPL source (zero-downtime). |
| `POST` | `/api/v1/pipelines/{id}/checkpoint` | `x-api-key` | Capture a state checkpoint for a pipeline. |
| `POST` | `/api/v1/pipelines/{id}/restore` | `x-api-key` | Restore a pipeline from a checkpoint blob. |
| `GET` | `/api/v1/pipelines/{id}/logs` | `x-api-key` | Stream pipeline logs (Server-Sent Events). |
| `GET` | `/api/v1/usage` | `x-api-key` | Get per-tenant usage metrics. |

**SaaS API — Tenant administration:**

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/api/v1/tenants` | `x-admin-key` | Create a new tenant. |
| `GET` | `/api/v1/tenants` | `x-admin-key` | List all tenants. Paginated. |
| `GET` | `/api/v1/tenants/{id}` | `x-admin-key` | Get a tenant by ID. |
| `DELETE` | `/api/v1/tenants/{id}` | `x-admin-key` | Delete a tenant and all its pipelines. |

**Cluster API — Workers:**

| Method | Path | Min Role | Description |
|--------|------|----------|-------------|
| `POST` | `/api/v1/cluster/workers/register` | Operator | Register a new worker node. Called by workers on startup. |
| `POST` | `/api/v1/cluster/workers/{id}/heartbeat` | Operator | Send a worker heartbeat. |
| `GET` | `/api/v1/cluster/workers` | Viewer | List all registered workers. Paginated. |
| `GET` | `/api/v1/cluster/workers/{id}` | Viewer | Get a worker by ID. |
| `DELETE` | `/api/v1/cluster/workers/{id}` | Admin | Remove a worker registration. |
| `POST` | `/api/v1/cluster/workers/{id}/drain` | Operator | Drain a worker (migrate all pipelines off before shutdown). |

**Cluster API — Pipeline groups:**

| Method | Path | Min Role | Description |
|--------|------|----------|-------------|
| `POST` | `/api/v1/cluster/pipeline-groups` | Operator | Deploy a pipeline group across workers. |
| `GET` | `/api/v1/cluster/pipeline-groups` | Viewer | List all pipeline groups. Paginated. |
| `GET` | `/api/v1/cluster/pipeline-groups/{id}` | Viewer | Get a pipeline group by ID. |
| `DELETE` | `/api/v1/cluster/pipeline-groups/{id}` | Admin | Delete a pipeline group. |
| `POST` | `/api/v1/cluster/pipeline-groups/{id}/inject` | Operator | Inject a single event into all pipelines in the group. |
| `POST` | `/api/v1/cluster/pipeline-groups/{id}/inject-batch` | Operator | Inject a batch of events into the group. |

**Cluster API — Connectors:**

| Method | Path | Min Role | Description |
|--------|------|----------|-------------|
| `GET` | `/api/v1/cluster/connectors` | Viewer | List all cluster connectors. Paginated. |
| `GET` | `/api/v1/cluster/connectors/{name}` | Viewer | Get a connector by name. |
| `POST` | `/api/v1/cluster/connectors` | Operator | Create a new connector. |
| `PUT` | `/api/v1/cluster/connectors/{name}` | Operator | Update a connector configuration. |
| `DELETE` | `/api/v1/cluster/connectors/{name}` | Admin | Delete a connector. |

**Cluster API — Operations:**

| Method | Path | Min Role | Description |
|--------|------|----------|-------------|
| `GET` | `/api/v1/cluster/topology` | Viewer | Get the full cluster topology (workers, groups, routes). |
| `POST` | `/api/v1/cluster/rebalance` | Operator | Trigger a manual pipeline rebalance across workers. |
| `GET` | `/api/v1/cluster/migrations` | Viewer | List active and completed migrations. Paginated. |
| `GET` | `/api/v1/cluster/migrations/{id}` | Viewer | Get a migration by ID. |
| `POST` | `/api/v1/cluster/pipelines/{group_id}/{pipeline_name}/migrate` | Operator | Manually migrate a specific pipeline to a target worker. |
| `GET` | `/api/v1/cluster/metrics` | Viewer | Get aggregate cluster metrics. |
| `GET` | `/api/v1/cluster/prometheus` | (none) | Prometheus scrape endpoint. Returns metrics in Prometheus text format. Unauthenticated — restrict via network policy. |
| `GET` | `/api/v1/cluster/scaling` | Viewer | Get scaling recommendations based on current load. |
| `GET` | `/api/v1/cluster/summary` | Viewer | Get a cluster health and capacity summary. |
| `GET` | `/api/v1/cluster/raft` | (none) | Get the Raft consensus state (leader, term, log index). Unauthenticated for monitoring compatibility. |

**Cluster API — Models and Chat:**

| Method | Path | Min Role | Description |
|--------|------|----------|-------------|
| `GET` | `/api/v1/cluster/models` | Viewer | List uploaded ML models. Paginated. |
| `POST` | `/api/v1/cluster/models` | Operator | Upload an ML model. |
| `DELETE` | `/api/v1/cluster/models/{name}` | Admin | Delete a model. |
| `GET` | `/api/v1/cluster/models/{name}/download` | Viewer | Download a model binary. |
| `POST` | `/api/v1/cluster/chat` | Viewer | Send a chat message to the configured LLM. |
| `GET` | `/api/v1/cluster/chat/config` | Viewer | Get the current chat/LLM configuration. |
| `PUT` | `/api/v1/cluster/chat/config` | Operator | Update the chat/LLM configuration. |

**Cluster API — WebSocket:**

| Protocol | Path | Description |
|----------|------|-------------|
| `WS` | `/api/v1/cluster/ws` | Persistent worker connection for heartbeats and event forwarding. Frame size limited to 64 KB. |

**Probes (SaaS and Cluster, unauthenticated):**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe. Returns `200 OK` if the process is alive. |
| `GET` | `/ready` | Readiness probe. Returns `200 OK` when the engine is ready to process events; `503` during startup or shutdown. |

#### Changed Endpoints

- **`GET /api/v1/pipelines`**: Added pagination support. Response now includes an optional `pagination` object (`total`, `limit`, `offset`, `has_more`) when `limit` or `offset` query parameters are provided. The `total` field was already present and remains required.
- **`GET /api/v1/cluster/workers`**: Added pagination (`limit`, `offset` query parameters).
- **`GET /api/v1/cluster/pipeline-groups`**: Added pagination.
- **`GET /api/v1/cluster/connectors`**: Added pagination.
- **`GET /api/v1/cluster/migrations`**: Added pagination.
- **`GET /api/v1/cluster/models`**: Added pagination.

#### PST Forecasting

The VPL `.forecast()` operator is available in this release. Pipelines that use `.forecast()` emit `ForecastResult` events as output. No new REST API endpoints are required; forecast output flows through the existing event injection and output response paths.

---

### v0.1.0 (initial release)

Initial public API release. Established the `/api/v1/` prefix, SaaS pipeline management endpoints, and the cluster coordinator API.

All endpoints listed in v0.2.0 were present in v0.1.0 with the following exceptions:

- Pagination parameters (`limit`, `offset`) were not supported on any list endpoint.
- `GET /api/v1/cluster/raft` was not present (Raft consensus added in v0.2.0).
- `GET /api/v1/cluster/scaling` was not present.
- `POST /api/v1/cluster/rebalance` was not present.
- `GET /api/v1/cluster/prometheus` was not present (Prometheus scraping added in v0.2.0).
- `GET /api/v1/cluster/summary` was not present.
- Models and Chat endpoints were not present.
- WebSocket (`/api/v1/cluster/ws`) was not present.
- RBAC roles were not enforced; all cluster endpoints required only a non-empty `x-api-key`.

---

## Migration Guide

### Breaking Changes

No breaking changes have been introduced through v0.3.0. All additions have been backward-compatible.

This section will be updated when a major version introduces breaking changes.

### Planned Changes (v0.4.0 and beyond)

The following changes are under consideration and will go through a full deprecation cycle before taking effect:

- **`GET /api/v1/usage`**: The response structure may be extended to include per-pipeline breakdown. The existing fields will remain.
- **`POST /api/v1/pipelines/{id}/checkpoint`** and **`POST /api/v1/pipelines/{id}/restore`**: The checkpoint blob format may be versioned explicitly in a future release. A `version` field will be added to the response; clients should store and return the entire blob opaquely.

### Upgrading from v0.2.0 to v0.3.0

1. **RBAC**: If you are using the Cluster API, ensure your API keys are configured with appropriate roles in the coordinator's RBAC configuration. Keys without a configured role default to `Viewer`. Operations that previously required only a valid key may now return `403 Forbidden` if the key's role is `Viewer` and the endpoint requires `Operator` or `Admin`.
2. **Body size limits**: Requests with bodies larger than 1 MB (standard) or 10 MB (upload) will now be rejected with `413`. Ensure batch injection calls stay within these limits; split larger batches if necessary.
3. **`503` on circuit open**: Inject calls to pipelines on unhealthy workers now return `503 Service Unavailable` rather than timing out. Implement retry logic with exponential backoff on `503` responses.

### Upgrading from v0.1.0 to v0.2.0

1. **Pagination**: List endpoints now accept `limit` and `offset` query parameters. If your client does not pass these parameters, the default behavior (`limit=50, offset=0`) applies. If you were relying on list endpoints returning all results, add explicit pagination loops.
2. **Multi-tenant architecture**: The SaaS API now enforces per-tenant isolation. Each tenant's `x-api-key` can only access that tenant's pipelines. Admin operations require a separate `x-admin-key`.
