# Joins Reference

Complete reference for stream join types in VPL.

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
    stream Right = RightEvent
        on Left.key == Right.key
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
    stream Right = RightEvent
        on Left.key == Right.key
)
.window(5m)
.emit(...)
```

**Behavior:**
- Every left event is guaranteed to produce at least one output event
- If a matching right event exists, fields are joined normally
- If no matching right event exists within the window, right-side fields are `null`
- Unmatched left events emit at window close (or immediately if configured)

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
    stream Right = RightEvent
        on Left.key == Right.key
)
.window(5m)
.emit(...)
```

**Behavior:**
- Mirror of left join: every right event produces output
- Left-side fields are `null` when unmatched

**Note:** `right_join(A, B)` is semantically equivalent to `left_join(B, A)` with swapped field references.

---

## Full Join

All events from both sides produce output.

**Syntax:**
```vpl
stream Result = full_join(
    stream Left = LeftEvent,
    stream Right = RightEvent
        on Left.key == Right.key
)
.window(5m)
.emit(...)
```

**Behavior:**
- Matched pairs produce output with all fields populated
- Unmatched left events produce output with null right-side fields
- Unmatched right events produce output with null left-side fields

**Use cases:**
- Reconciliation (detect mismatches from either side)
- Data quality monitoring
- Complete audit trails

---

## Join Conditions

### Simple Equality

```vpl
on Left.key == Right.key
```

### Composite Keys

```vpl
on Left.customer_id == Right.customer_id
    and Left.region == Right.region
```

### Multi-Stream Joins

```vpl
stream Result = join(
    stream A = EventA,
    stream B = EventB
        on A.id == B.a_id,
    stream C = EventC
        on A.id == C.a_id
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

### Window Policy

```vpl
.window(5m, policy: "watermark")
```

| Policy | Description |
|--------|-------------|
| `"time"` (default) | Window based on wall clock |
| `"watermark"` | Window advances with watermark (for out-of-order events) |

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
- **Match complexity**: Per-event match is `O(buffered_events)` for equality joins
- **Partitioning**: Use `.partition_by()` before the join to reduce match space
- **Window size**: Smaller windows reduce memory and improve match latency

### Optimization: Partition Before Join

```vpl
# Without partitioning: scans all buffered events
stream Result = join(
    stream A = EventA,
    stream B = EventB on A.key == B.key
).window(5m)

# With partitioning: matches only within the same partition
stream Result = join(
    stream A = EventA.partition_by(key),
    stream B = EventB.partition_by(key)
        on A.key == B.key
).window(5m)
```

---

## Implementation

| Component | Location |
|-----------|----------|
| JoinType enum | `varpulis-core/src/ast.rs` |
| Join execution | `varpulis-runtime/src/engine.rs` |
| Null emission | `varpulis-runtime/src/join.rs` |

---

## See Also

- [Outer Joins Tutorial](../tutorials/outer-joins-tutorial.md) -- Step-by-step outer join examples
- [Language Tutorial Part 5: Joins](../tutorials/language-tutorial.md#part-5-joins) -- Inner join patterns
- [Windows & Aggregations Reference](windows-aggregations.md) -- Window types for joins
