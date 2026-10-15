# Joins Reference

Complete reference for stream join types in VPL.

A join runs in the engine as it ships (`varpulis simulate`, a Vejas detect
unit) and its buffers are in the program's snapshot. Each clause reads a
source under a name of its own — `stream Orders = OrderEvent` — and that name
is what the `on` condition, the emitted fields (`Orders.order_id`) and the
buffer use. The condition is written either way the examples show: `on
Orders.k == Payments.k` in the clause, or `.on(Orders.k == Payments.k)` after
the join. An outer join emits a pair when the match arrives, and an event of
a preserved side that found no match once, alone, when its window closes:
when the event clock of the join's sources passes the window's end, or at the
end of a bounded input (see the
[outer joins tutorial](../tutorials/outer-joins-tutorial.md)).

## Join Types

| Type | Syntax | Left Preserved | Right Preserved | Null Fill |
|------|--------|----------------|-----------------|-----------|
| Inner | `join(...)` | Only matched | Only matched | None |
| Left | `left_join(...)` | All | Only matched | Right-side nulls |
| Right | `right_join(...)` | Only matched | All | Left-side nulls |
| Full | `full_join(...)` | All | All | Both sides |

---

## Inner Join

Produces output only when both sides have matching events within the window.

**Syntax:**
```vpl
stream Result = join(
    stream Left = LeftEvent,
    stream Right = RightEvent.on(Left.key == Right.key)
)
.window(5m)
.emit(...)
```

**Behavior:**
- Events are buffered per side within the window
- When a new event arrives, it is matched against buffered events from the other side
- Only matching pairs produce output
- Unmatched events expire silently when the window closes

---

## Left Join

All left-side events produce output. Right-side fields are `null` when no match exists.

**Syntax:**
```vpl
stream Result = left_join(
    stream Left = LeftEvent,
    stream Right = RightEvent.on(Left.key == Right.key)
)
.window(5m)
.emit(...)
```

**Behavior:**
- Every left event produces exactly one output event
- If a matching right event arrives within the window, the pair is emitted then
- If none does, the left event is emitted alone when its window closes, with
  the right side's join key `null` and its other fields absent
- A right event alone produces nothing

**Use cases:**
- Find orders without payments
- Enrich events with optional reference data
- Detect missing acknowledgments

---

## Right Join

All right-side events produce output. Left-side fields are `null` when no match exists.

**Syntax:**
```vpl
stream Result = right_join(
    stream Left = LeftEvent,
    stream Right = RightEvent.on(Left.key == Right.key)
)
.window(5m)
.emit(...)
```

**Behavior:**
- Mirror of left join: every right event produces exactly one output event,
  the pair when its match arrives or itself alone when its window closes
- Left-side fields are `null` when unmatched

**Note:** `right_join(A, B)` is semantically equivalent to `left_join(B, A)` with swapped field references.

---

## Full Join

All events from both sides produce output.

**Syntax:**
```vpl
stream Result = full_join(
    stream Left = LeftEvent,
    stream Right = RightEvent.on(Left.key == Right.key)
)
.window(5m)
.emit(...)
```

**Behavior:**
- Matched pairs produce output with all fields populated, when the match arrives
- An unmatched event of either side produces output when its window closes,
  with the other side's fields null

**Use cases:**
- Reconciliation (detect mismatches from either side)
- Data quality monitoring
- Complete audit trails

---

## Join Conditions

### Simple Equality

```vpl
.on(Left.key == Right.key)
```

### Composite Keys

```vpl
.on(Left.customer_id == Right.customer_id
    and Left.region == Right.region)
```

### Multi-Stream Joins

```vpl
stream Result = join(
    stream A = EventA,
    stream B = EventB.on(A.id == B.a_id),
    stream C = EventC.on(A.id == C.a_id)
)
.window(5m)
.emit(...)
```

Multi-stream joins evaluate pairwise: A matches B, then the result matches C.

---

## Window Interaction

Joins require a window to bound the match space:

| Window Type | Behavior |
|-------------|----------|
| Tumbling | Events match only within the same window boundary |
| Sliding | Events match within the sliding window extent |
| Count | Match against the last N events from each side |

### Late Events

A join's window runs on event time. An event that arrives after its window
has closed matches nothing; to hold windows open for a bounded disorder,
declare `.watermark(out_of_order: 30s)` on the stream (see
[watermarks](../language/operators.md)). `.window(5m, policy: ...)` is
refused: it was parsed and ignored.

---

## Null-Fill Behavior

For outer joins, unmatched fields are filled with VPL `null`:

```vpl
# Detect null-filled fields
.where(Right.field == null)      # Unmatched right
.where(Left.field != null)       # Matched left

# Provide defaults
.emit(
    value: if Right.amount != null then Right.amount else 0
)
```

Null comparisons follow SQL semantics:
- `null == null` evaluates to `false`
- `null != value` evaluates to `true`
- Use `== null` / `!= null` for null checks

---

## Performance Notes

- **Buffer size**: Each side buffers events within the window. Memory is `O(events_per_window × 2)`
- **Match complexity**: each side's buffer is keyed by the join key's value, so a new event is compared only with the buffered events that share its key
- **Window size**: Smaller windows reduce memory and improve match latency

### No Partitioning Needed

A join clause takes nothing but `.on(...)`, so `.partition_by()` cannot be put
inside `join(...)`, and it would not help: the buffers are already keyed by the
join key, as the match complexity above says.

```vpl
stream Result = join(
    stream A = EventA,
    stream B = EventB.on(A.key == B.key)
)
.window(5m)
.emit(key: A.key, a_value: A.value, b_value: B.value)
```

---

## Implementation

| Component | Location |
|-----------|----------|
| JoinType enum | `varpulis-core/src/ast.rs` |
| Join execution | `varpulis-runtime/src/engine/dispatch.rs` |
| Null emission | `varpulis-runtime/src/join.rs` |

---

## See Also

- [Outer Joins Tutorial](../tutorials/outer-joins-tutorial.md) -- Step-by-step outer join examples
- [Language Tutorial Part 5: Joins](../tutorials/language-tutorial.md#part-5-joins) -- Inner join patterns
- [Windows & Aggregations Reference](windows-aggregations.md) -- Window types for joins
