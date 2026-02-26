# Outer Joins Tutorial

This tutorial teaches you to use LEFT, RIGHT, and FULL outer joins in VPL for stream processing. You'll start with an inner join recap, then build progressively through each outer join type, null handling, and a complete reconciliation example.

## Prerequisites

- Varpulis built and on your `PATH` (see [Getting Started](getting-started.md))
- Basic VPL knowledge: streams, `where`, `window`, `join` (see [Language Tutorial](language-tutorial.md))

---

## Part 1: Inner Join Recap

Before learning outer joins, recall how inner joins work. An inner join only produces output when both sides have matching events:

```vpl
stream EnrichedOrders = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent
        on Orders.customer_id == Customers.id
)
.window(5m)
.emit(
    order_id: Orders.id,
    customer_name: Customers.name,
    amount: Orders.amount
)
```

If an order arrives but no matching customer event exists in the window, **no output is produced**. This is fine when both sides are always present, but many real-world scenarios need to handle missing data.

---

## Part 2: LEFT JOIN — Keep All Left-Side Events

A LEFT JOIN keeps all events from the left stream, filling in `null` for unmatched right-side fields:

```vpl
stream OrdersWithPayments = left_join(
    stream Orders = OrderEvent,
    stream Payments = PaymentEvent
        on Orders.order_id == Payments.order_id
)
.window(5m)
.emit(
    order_id: Orders.order_id,
    amount: Orders.amount,
    payment_status: Payments.status,
    paid: Payments.amount
)
```

### Test Data

Create `outer_join_left.evt`:

```
# Orders (left side - all will appear in output)
OrderEvent { order_id: "O1", customer: "alice", amount: 100 }
OrderEvent { order_id: "O2", customer: "bob", amount: 200 }
OrderEvent { order_id: "O3", customer: "charlie", amount: 300 }

# Payments (right side - only O1 and O3 have payments)
PaymentEvent { order_id: "O1", status: "paid", amount: 100 }
PaymentEvent { order_id: "O3", status: "partial", amount: 150 }
```

### Expected Output

| order_id | amount | payment_status | paid |
|----------|--------|----------------|------|
| O1 | 100 | paid | 100 |
| O2 | 200 | *null* | *null* |
| O3 | 300 | partial | 150 |

Order O2 appears in the output even though it has no matching payment. The payment fields are `null`.

**Key takeaway:** LEFT JOIN guarantees every left-side event produces output, with nulls for unmatched right-side fields.

---

## Part 3: RIGHT JOIN — Keep All Right-Side Events

A RIGHT JOIN is the mirror of LEFT JOIN. All right-side events are preserved:

```vpl
stream PaymentReconciliation = right_join(
    stream Orders = OrderEvent,
    stream Payments = PaymentEvent
        on Orders.order_id == Payments.order_id
)
.window(5m)
.emit(
    order_id: Payments.order_id,
    order_amount: Orders.amount,
    payment_amount: Payments.amount,
    status: Payments.status
)
```

If a payment arrives for an order that hasn't been seen yet, the order fields are `null`:

| order_id | order_amount | payment_amount | status |
|----------|-------------|----------------|--------|
| O1 | 100 | 100 | paid |
| O4 | *null* | 50 | refund |

Payment O4 appears even though no matching order exists. Use RIGHT JOIN when the right-side stream is the "primary" data source.

---

## Part 4: FULL JOIN — Keep Everything

A FULL JOIN keeps all events from both sides, with nulls where either side is missing:

```vpl
stream FullReconciliation = full_join(
    stream Orders = OrderEvent,
    stream Payments = PaymentEvent
        on Orders.order_id == Payments.order_id
)
.window(5m)
.emit(
    order_id: Orders.order_id,
    payment_ref: Payments.order_id,
    order_amount: Orders.amount,
    payment_amount: Payments.amount,
    status: Payments.status
)
```

### Expected Output

| order_id | payment_ref | order_amount | payment_amount | status |
|----------|-------------|-------------|----------------|--------|
| O1 | O1 | 100 | 100 | paid |
| O2 | *null* | 200 | *null* | *null* |
| *null* | O4 | *null* | 50 | refund |

FULL JOIN is ideal for reconciliation: identify orders without payments AND payments without orders.

---

## Part 5: Null Handling

When outer joins produce null fields, you can filter and handle them:

### Filtering for Unmatched Events

```vpl
# Find orders that have NO matching payment (null payment)
stream UnpaidOrders = left_join(
    stream Orders = OrderEvent,
    stream Payments = PaymentEvent
        on Orders.order_id == Payments.order_id
)
.window(5m)
.where(Payments.status == null)
.emit(
    order_id: Orders.order_id,
    amount: Orders.amount,
    alert: "unpaid_order"
)
```

### Coalescing Null Values

Use conditional expressions to provide defaults for null fields:

```vpl
stream SafeJoin = left_join(
    stream Orders = OrderEvent,
    stream Payments = PaymentEvent
        on Orders.order_id == Payments.order_id
)
.window(5m)
.emit(
    order_id: Orders.order_id,
    amount: Orders.amount,
    status: if Payments.status != null then Payments.status else "awaiting_payment"
)
```

---

## Part 6: Complete Example — Order-Payment Reconciliation

```vpl
connector KafkaBroker = kafka (
    brokers: ["broker:9092"],
    group_id: "reconciliation"
)

# Full reconciliation across orders and payments
stream Reconciliation = full_join(
    stream Orders = OrderEvent.from(KafkaBroker, topic: "orders"),
    stream Payments = PaymentEvent.from(KafkaBroker, topic: "payments")
        on Orders.order_id == Payments.order_id
)
.window(10m)
.emit(
    order_id: Orders.order_id,
    payment_ref: Payments.order_id,
    order_amount: Orders.amount,
    paid_amount: Payments.amount,
    match_status: if Orders.order_id != null and Payments.order_id != null
        then "matched"
        else if Orders.order_id != null
            then "order_no_payment"
            else "payment_no_order"
)

# Alert on mismatches
stream Mismatches = Reconciliation
    .where(match_status != "matched")
    .emit(
        alert: "reconciliation_mismatch",
        type: match_status,
        order_id: order_id,
        payment_ref: payment_ref
    )

# Reconciliation summary per window
stream Summary = Reconciliation
    .window(10m)
    .aggregate(
        matched: count() where match_status == "matched",
        unmatched_orders: count() where match_status == "order_no_payment",
        orphan_payments: count() where match_status == "payment_no_order"
    )
```

---

## Quick Reference

| Join Type | Syntax | Behavior |
|-----------|--------|----------|
| Inner | `join(...)` | Only matching events |
| Left | `left_join(...)` | All left + matching right (nulls for unmatched) |
| Right | `right_join(...)` | Matching left + all right (nulls for unmatched) |
| Full | `full_join(...)` | All from both sides (nulls for unmatched) |

### Null Patterns

| Pattern | Purpose |
|---------|---------|
| `.where(field == null)` | Find unmatched (null-filled) events |
| `.where(field != null)` | Find matched events |
| `if field != null then field else default` | Coalesce nulls |

---

## Next Steps

- [Joins Reference](../reference/joins.md) -- Complete join semantics and performance notes
- [Language Tutorial Part 5: Joins](language-tutorial.md#part-5-joins) -- Inner join patterns
- [Windows & Aggregations Reference](../reference/windows-aggregations.md) -- Window types for joins
