# PostgreSQL CDC Tutorial

This tutorial walks you through setting up Varpulis with PostgreSQL Change Data Capture (CDC) for real-time database change streaming. You'll learn to configure logical replication, create CDC pipelines, filter by operation type, handle updates with old/new fields, and process changes from multiple tables.

## Prerequisites

- PostgreSQL 10+ with `wal_level=logical` (we'll configure this below)
- Varpulis built with the `cdc` feature (`cargo build --release --features cdc`)
- Basic VPL knowledge (see [Getting Started](getting-started.md))

---

## Part 1: Setting Up PostgreSQL for CDC

### Enable Logical Replication

PostgreSQL logical replication requires `wal_level=logical`. Edit `postgresql.conf`:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Or start PostgreSQL with Docker (recommended for testing):

```bash
docker run -d --name varpulis-pg \
    -e POSTGRES_USER=varpulis \
    -e POSTGRES_PASSWORD=secret \
    -e POSTGRES_DB=myapp \
    -p 5432:5432 \
    postgres:16-alpine \
    -c wal_level=logical \
    -c max_replication_slots=4 \
    -c max_wal_senders=4
```

Verify logical replication is enabled:

```bash
docker exec varpulis-pg psql -U varpulis -d myapp -c "SHOW wal_level;"
#  wal_level
# -----------
#  logical
```

### Create Tables and Publication

```sql
-- Create tables
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status TEXT DEFAULT 'pending'
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    stock INTEGER DEFAULT 0
);

-- Create a publication for CDC
CREATE PUBLICATION varpulis_pub FOR TABLE orders, products;
```

The publication tells PostgreSQL which tables to include in the CDC stream.

---

## Part 2: Your First CDC Pipeline

### The Program

Create `cdc_orders.vpl`:

```vpl
# Track all new orders in real-time via PostgreSQL CDC
connector PgCdc = cdc_postgres (
    host: "localhost",
    port: 5432,
    dbname: "myapp",
    user: "varpulis",
    password: "secret",
    publication: "varpulis_pub"
)

stream NewOrders = orders.INSERT
    .from(PgCdc)
    .emit(
        customer: customer,
        amount: amount,
        status: status
    )
```

### How It Works

The CDC connector uses PostgreSQL's logical replication protocol to stream WAL (Write-Ahead Log) changes as events. Each database change becomes a Varpulis event with:

- **`event_type`**: `{table}.{OPERATION}` (e.g., `orders.INSERT`, `orders.UPDATE`)
- **`_table`**: The source table name
- **`_op`**: The operation type (`INSERT`, `UPDATE`, or `DELETE`)
- **Data fields**: Column values from the changed row

### Test It

Insert some rows:

```bash
docker exec varpulis-pg psql -U varpulis -d myapp -c "
    INSERT INTO orders (customer, amount) VALUES ('Alice', 99.99);
    INSERT INTO orders (customer, amount) VALUES ('Bob', 250.00);
"
```

You should see output events for each INSERT.

---

## Part 3: Filtering by Operation

Each CDC event carries an `_op` field that you can filter on:

```vpl
# Only track new inserts
stream Inserts = orders.INSERT
    .from(PgCdc)
    .where(_op == "INSERT")
    .emit(customer: customer, amount: amount)

# Only track updates
stream Updates = orders.UPDATE
    .from(PgCdc)
    .where(_op == "UPDATE")
    .emit(customer: customer, new_amount: amount)

# Only track deletes
stream Deletes = orders.DELETE
    .from(PgCdc)
    .where(_op == "DELETE")
    .emit(deleted_id: id)
```

You can also use the `event_type` which follows the pattern `{table}.{OP}`:

```vpl
# Equivalent — filter by event type
stream Inserts = orders.INSERT
    .from(PgCdc)
    .emit(customer: customer, amount: amount)
```

---

## Part 4: Handling Updates

UPDATE events can include both old and new field values when PostgreSQL is configured with `REPLICA IDENTITY FULL`:

```sql
-- Enable full replica identity for old-value tracking
ALTER TABLE orders REPLICA IDENTITY FULL;
```

With full replica identity, UPDATE events include `old_*` prefixed fields:

```vpl
# Detect price changes
stream PriceChanges = orders.UPDATE
    .from(PgCdc)
    .where(_op == "UPDATE")
    .emit(
        customer: customer,
        old_amount: old_amount,
        new_amount: amount,
        change: amount - old_amount
    )
```

Without `REPLICA IDENTITY FULL`, only the new values and primary key are available in UPDATE events.

---

## Part 5: Multi-Table CDC

A single publication can include multiple tables. Events are automatically routed by their `_table` field:

```vpl
connector PgCdc = cdc_postgres (
    host: "localhost",
    port: 5432,
    dbname: "myapp",
    user: "varpulis",
    password: "secret",
    publication: "varpulis_pub"
)

# Orders pipeline
stream OrderAlerts = orders.INSERT
    .from(PgCdc)
    .where(amount > 1000)
    .emit(alert: "high_value_order", customer: customer, amount: amount)

# Product stock changes
stream LowStock = products.UPDATE
    .from(PgCdc)
    .where(stock < 10)
    .emit(alert: "low_stock", product: name, remaining: stock)
```

---

## Part 6: Complete Example — Order Processing Pipeline

```vpl
connector PgCdc = cdc_postgres (
    host: "localhost",
    port: 5432,
    dbname: "myapp",
    user: "varpulis",
    password: "secret",
    publication: "varpulis_pub"
)

connector AlertWebhook = http (
    url: "http://localhost:8080/alerts",
    method: "POST"
)

# High-value order alerts
stream HighValueOrders = orders.INSERT
    .from(PgCdc)
    .where(amount > 500)
    .emit(
        alert_type: "high_value_order",
        customer: customer,
        amount: amount,
        status: status
    )
    .to(AlertWebhook)

# Order volume per minute
stream OrderVolume = orders.INSERT
    .from(PgCdc)
    .window(1m)
    .aggregate(
        order_count: count(),
        total_amount: sum(amount),
        avg_amount: avg(amount)
    )

# Rapid order pattern: 3+ orders from the same customer within 5 minutes
stream RapidOrders = orders.INSERT as o1
    .from(PgCdc)
    -> orders.INSERT as o2 where o2.customer == o1.customer
    -> orders.INSERT as o3 where o3.customer == o1.customer
    .within(5m)
    .emit(
        alert: "rapid_orders",
        customer: o1.customer,
        order_count: 3,
        total: o1.amount + o2.amount + o3.amount
    )
```

---

## Troubleshooting

### "replication slot already exists"

The connector creates a replication slot on first start. If a previous run didn't clean up:

```sql
-- List existing slots
SELECT slot_name, active FROM pg_replication_slots;

-- Drop a stale slot
SELECT pg_drop_replication_slot('varpulis_slot');
```

### WAL accumulation

Active replication slots prevent WAL cleanup. If the Varpulis connector is down for a long time, WAL will accumulate. Monitor with:

```sql
SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
FROM pg_replication_slots;
```

Drop unused slots to allow WAL cleanup.

### Permission errors

The PostgreSQL user needs `REPLICATION` privilege:

```sql
ALTER USER varpulis WITH REPLICATION;
```

### "wal_level is not logical"

Restart PostgreSQL after changing `wal_level` in `postgresql.conf`. The change requires a server restart.

---

## Quick Reference

| Concept | Syntax / Value |
|---------|---------------|
| Feature flag | `--features cdc` |
| Event type format | `{table}.{INSERT\|UPDATE\|DELETE}` |
| Metadata: table | `_table` field |
| Metadata: operation | `_op` field (`INSERT`, `UPDATE`, `DELETE`) |
| Old values (UPDATE) | `old_*` prefix (requires `REPLICA IDENTITY FULL`) |
| Publication | `CREATE PUBLICATION name FOR TABLE t1, t2;` |
| Replication slot | Auto-created by connector, name configurable |

---

## Next Steps

- [CDC Architecture](../architecture/postgres-cdc.md) -- Polling architecture, LSN tracking, type mapping
- [Configuration Guide](../guides/configuration.md) -- CDC configuration options
- [Checkpointing Tutorial](checkpointing-tutorial.md) -- State persistence across restarts
- [Language Tutorial](language-tutorial.md) -- VPL language reference
