# PostgreSQL CDC Architecture

Technical architecture of the Varpulis PostgreSQL Change Data Capture connector.

## Overview

The CDC connector streams database changes from PostgreSQL's Write-Ahead Log (WAL) as Varpulis events. It uses the `pg_logical_slot_get_changes()` polling approach for maximum portability across PostgreSQL versions and hosting environments.

```
PostgreSQL WAL
    │
    ▼
pg_logical_slot_get_changes()  ──  100ms poll interval
    │
    ▼
parse_change_text()  ──  text-format WAL → structured fields
    │
    ▼
cdc_event()  ──  fields → Varpulis Event with _table, _op metadata
    │
    ▼
mpsc::Sender<Event>  ──  into the Varpulis processing pipeline
```

## Polling Architecture

### Why Polling (Not Streaming Replication)

PostgreSQL supports two methods for consuming logical replication:

1. **Streaming replication protocol** (`START_REPLICATION` via `copy_both_simple`)
2. **Polling** (`pg_logical_slot_get_changes()` SQL function)

Varpulis uses polling because:
- Works with standard `tokio-postgres` without custom protocol handling
- Compatible with connection poolers (PgBouncer, Supabase, etc.)
- Works with managed PostgreSQL services that restrict replication connections
- Simpler error recovery — each poll is an independent query

### Poll Loop

```rust
// Simplified poll loop (connector/postgres_cdc.rs)
while running.load(Ordering::SeqCst) {
    match client.query(&poll_query, &[]).await {
        Ok(rows) => {
            for row in &rows {
                let data: Option<&str> = row.try_get(2).ok();
                if let Some(change_data) = data {
                    if let Some(event) = parse_change_text(change_data) {
                        tx.send(event).await?;
                    }
                }
            }
        }
        Err(e) => {
            // Back off on error (5s)
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    // Poll interval: 100ms
    tokio::time::sleep(Duration::from_millis(100)).await;
}
```

The 100ms poll interval provides near-real-time CDC with minimal database load. Each `pg_logical_slot_get_changes()` call atomically consumes all pending changes from the slot.

---

## LSN Tracking and Checkpoints

### Logical Sequence Number (LSN)

Every WAL change has an LSN — a monotonically increasing position in the WAL. The `pg_logical_slot_get_changes()` function returns rows with `(lsn, xid, data)`.

### Automatic LSN Advancement

The `_get_changes` variant (as opposed to `_peek_changes`) automatically advances the slot's confirmed LSN. This means:
- Consumed changes won't be replayed on the next poll
- If Varpulis crashes, only unconsumed changes from the last poll are re-delivered
- WAL segments can be cleaned up by PostgreSQL after the slot advances

### Integration with Varpulis Checkpointing

When Varpulis checkpoints its engine state (via `--state-dir` or `--features persistence`):

1. Engine state (windows, patterns, variables) is serialized
2. The CDC slot's current LSN is NOT stored (the slot tracks it independently in PostgreSQL)
3. On restart, the CDC connector reconnects and the slot resumes from its last confirmed LSN

This means PostgreSQL manages exactly-once delivery semantics for the replication slot, while Varpulis manages its own processing state. There is a small window of potential re-delivery between the last Varpulis checkpoint and the crash point.

---

## Event Format Mapping

### Text-Decoding Format

The `pgoutput` plugin with `pg_logical_slot_get_changes()` returns text-format changes:

```
table public.orders: INSERT: id[integer]:42 amount[numeric]:99.99 customer[text]:'alice'
table public.orders: UPDATE: id[integer]:42 amount[numeric]:199.99 status[text]:'confirmed'
table public.orders: DELETE: id[integer]:42
```

### Parsing

The `parse_change_text()` function extracts:
1. **Table name** — after "table " prefix, stripping schema (e.g., `public.orders` → `orders`)
2. **Operation** — INSERT, UPDATE, or DELETE
3. **Fields** — `name[type]:value` tokens parsed into `(String, Value)` pairs

### PostgreSQL Type → VPL Value Mapping

| PostgreSQL Type | VPL Value | Notes |
|----------------|-----------|-------|
| `integer`, `bigint` | `Value::Int(i64)` | Parsed from text |
| `numeric`, `real`, `double precision` | `Value::Float(f64)` | Parsed from text |
| `boolean` | `Value::Bool` | `t`/`true` → true, `f`/`false` → false |
| `text`, `varchar`, `char` | `Value::Str` | Quotes stripped |
| `null` | `Value::Null` | Literal "null" text |
| Other types | `Value::Str` | Passed as text representation |

### Event Structure

Each CDC event includes:

| Field | Type | Description |
|-------|------|-------------|
| `event_type` | String | `{table}.{INSERT\|UPDATE\|DELETE}` |
| `_table` | Str | Source table name (without schema) |
| `_op` | Str | Operation: `INSERT`, `UPDATE`, or `DELETE` |
| Column fields | Mixed | One field per table column with the parsed value |

---

## Connection Lifecycle

### Startup

1. Connect to PostgreSQL via `tokio-postgres` (standard, non-replication connection)
2. Attempt to create replication slot (`pg_create_logical_replication_slot`)
   - If slot already exists, log and continue
   - If creation fails for other reasons, log warning
3. Spawn async polling task
4. Store `Arc<AtomicBool>` for stop signaling

### Reconnection

The current implementation does not auto-reconnect. If the database connection drops:
- The poll query returns an error
- The loop backs off (5s sleep)
- Subsequent polls retry on the existing client

For production deployments, wrap the connector in Varpulis's circuit breaker or restart the source on connection failure.

### Shutdown

1. `stop()` sets `running` to `false` via `AtomicBool`
2. The poll loop exits on the next iteration
3. The replication slot remains in PostgreSQL (not dropped on stop)

To drop the slot on shutdown, use:
```sql
SELECT pg_drop_replication_slot('varpulis_slot');
```

---

## Error Handling

| Error | Behavior |
|-------|----------|
| Connection failure | `ConnectorError::ConnectionFailed` on `start()` |
| Poll query error | 5-second backoff, retry on next iteration |
| Parse failure | Event skipped (returns `None` from `parse_change_text`) |
| Channel closed (receiver dropped) | Polling loop stops |
| Slot creation failure | Warning logged, continues (may fail on first poll) |

---

## Feature Gating

```toml
# Cargo.toml
[features]
cdc = ["tokio-postgres", "bytes"]
```

When `cdc` is not enabled:
- `PostgresCdcSource::start()` returns `ConnectorError::NotAvailable`
- `PostgresCdcConfig` and `CdcOperation` remain available (not feature-gated)
- `cdc_event()` helper is available in `#[cfg(any(feature = "cdc", test))]`

---

## File Locations

| Component | File |
|-----------|------|
| CDC connector | `crates/varpulis-runtime/src/connector/postgres_cdc.rs` |
| Connector types | `crates/varpulis-runtime/src/connector/types.rs` |
| E2E tests | `crates/varpulis-runtime/tests/cdc_e2e.rs` |
| Benchmark | `crates/varpulis-runtime/benches/cdc_benchmark.rs` |
| Docker test setup | `tests/integration/docker-compose.cdc.yml` |

---

## See Also

- [PostgreSQL CDC Tutorial](../tutorials/postgres-cdc-tutorial.md) -- Step-by-step setup guide
- [Configuration Guide](../guides/configuration.md) -- CDC configuration options
- [System Architecture](system.md) -- Overall Varpulis architecture
