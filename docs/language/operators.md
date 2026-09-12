# Operators

## Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `a + b` |
| `-` | Subtraction | `a - b` |
| `*` | Multiplication | `a * b` |
| `/` | Division | `a / b` |
| `%` | Modulo | `a % b` |
| `**` | Power | `a ** b` |
| `-` (unary) | Negation | `-a` |

## Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equality | `a == b` |
| `!=` | Inequality | `a != b` |
| `<` | Less than | `a < b` |
| `<=` | Less than or equal | `a <= b` |
| `>` | Greater than | `a > b` |
| `>=` | Greater than or equal | `a >= b` |

All six widen `int` to `float` when the two sides differ in numeric type, so
`DestinationPort == 445` holds whether the port arrived as `445` or `445.0`, and
`.having(total == 100)` can be satisfied by an aggregate — every numeric
aggregate returns a `float`. The same rule applies inside a `->` sequence step
as in `.where()`. Comparisons between values that are not both numeric (a string
and an int, say) are neither true nor false: the predicate does not match.

## Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `and` | Logical AND | `a and b` |
| `or` | Logical OR | `a or b` |
| `not` | Logical NOT | `not a` |

## Bitwise Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `&` | Bitwise AND | `a & b` |
| `\|` | Bitwise OR | `a \| b` |
| `^` | Bitwise XOR | `a ^ b` |
| `~` | Bitwise NOT | `~a` |
| `<<` | Left shift | `a << 2` |
| `>>` | Right shift | `a >> 2` |

## Assignment Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `:=` | Assignment | `x := 5` |

## Chaining Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `.` | Member access / chaining | `stream.where(...).select(...)` |
| `?.` | Optional access | `user?.name` |
| `??` | Null coalesce | `value ?? default` |

## Collection Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `[]` | Indexing | `array[0]` |
| `[:]` | Slice | `array[1:3]` |
| `in` | Membership | `x in list` |
| `not in` | Non-membership | `x not in list` |

## Range Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `..` | Exclusive range | `0..10` (0 to 9) |
| `..=` | Inclusive range | `0..=10` (0 to 10) |

## Lambda Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=>` | Lambda expression | `x => x * 2` |
| `->` | Return type | `fn add(a: int, b: int) -> int` |

## SASE+ Pattern Mode Operators

Stream-level operators that control how SASE+ patterns produce matches.
See [SASE+ Patterns Guide](../guides/sase-patterns.md#selection-and-emission-modes) for details.

### Selection strategy (how runs are spawned)

| Operator | Description | Example |
|----------|-------------|---------|
| `.strict()` | Strict contiguity — events must be adjacent | `stream X = A -> B.strict().emit(...)` |
| `.stnm()` | Skip-till-next-match — non-overlapping maximal matches | `stream X = A -> B.stnm().emit(...)` |
| `.stam()` | Skip-till-any-match — overlapping runs (default) | `stream X = A -> B.stam().emit(...)` |

### Emission mode (how matches are produced)

| Operator | Description | Example |
|----------|-------------|---------|
| `.each()` | Emit one match per Kleene event extension (**default**) | `stream X = all B as b .each().emit(...)` |
| `.longest()` | Emit one consolidated match at terminator/break | `stream X = all B as b .longest().emit(...)` |
| `.subsets()` | Emit 2^N − 1 matches (paper-correct STAM verbose, expert mode) | `stream X = all B as b .subsets().emit(...)` |

### The Kleene event cap

A Kleene closure (`all X`, `X+`, `X*`) accumulates **at most 20 events per
match**. With n accumulated events the engine's ZDD enumerates up to 2^n − 1
combinations, so the bound is what keeps an unbounded closure from exhausting
memory.

Events past the cap are dropped from that run, which means any count derived
from the closure — `count(alias)`, `_count_{alias}` — stops rising. The engine
does not do this quietly:

- `varpulis check` reports **W003** on every unbounded closure, naming the cap.
- The first drop in a run is logged at `WARN`.
- Under `.longest()` and `.subsets()`, the match itself carries
  **`_kleene_truncated`**: the number of events dropped. It is absent when
  nothing was dropped, so you can emit it directly. Under `.each()` (the
  default) the run simply stops producing matches once the cap is reached —
  there is no later match to carry the mark — so the warning and the log are
  the signal there.

```varpulis
stream BruteForce = AuthEvent where status == "failed" as first
    -> all AuthEvent where status == "failed" as fails
    -> AuthEvent where status == "success" as success
    .within(30m)
    .partition_by(source_ip)
    .longest()
    .emit(failed_count: count(fails) + 1, dropped: _kleene_truncated)
```

If you need an exact count of an unbounded run, count it in a windowed
aggregation rather than a Kleene closure.

### The other cap: how many matches one closure emits

Separate from the 20-event cap, an enumeration emits **at most 10 000 matches**
from a single closure. The two are different losses. The first drops events
from a closure; this one drops whole matches from the result set, so what you
receive is the first 10 000 of an unknown number rather than all of them.

A match from a truncated enumeration carries **`_enumeration_truncated`**, set
to `true`. It is absent when the enumeration saw every combination, including
when it reached the cap on the very last one — a complete answer is not marked
incomplete.

There is deliberately no count. Knowing how many matches were dropped would
mean enumerating them, which is the work the cap exists to avoid.

```varpulis
stream Combinations = Login as start
    -> all Action as steps
    .within(1h)
    .subsets()
    .emit(steps: count(steps), partial: _enumeration_truncated)
```

### Absence: `-> NOT B`

A negated step matches when its event does **not** arrive before the pattern's
`within` deadline. "A happened, and B did not follow."

It lives in a `pattern` declaration, and takes `where` and `within` without a
leading dot:

```varpulis
event Order:
    id: str
event Ack:
    id: str

pattern Unacked =
    Order as o
    -> NOT Ack where id == o.id
    within 4h
    partition by id

stream Alerts = Unacked
    .emit(order: o.id, violation: "no acknowledgement within 4h")
```

This is not `.not()`. `.not()` is a stream operator that **cancels** a run in
flight when the forbidden event arrives — "A then C, unless B came in between".
It cannot fire on an absence, because with no event to trigger it there is
nothing for the engine to emit. `-> NOT B` is the opposite: the deadline
passing is what produces the match.

**The alert arrives with the next event, not on a wall clock.** The deadline is
checked against the watermark, and the watermark moves when an event arrives —
any event of a type the pattern references. On a stream that keeps flowing this
is a few seconds of lag; on a stream that goes completely silent after the
trigger, the alert does not fire at all, because nothing tells the engine that
time has passed. If you need the alert on a dead stream, emit a periodic
heartbeat event of a type the pattern references.

### Monotonic pattern shortcuts

| Operator | Description | Example |
|----------|-------------|---------|
| `.increasing(field)` | Sugar for `where field > self.field` Kleene+; auto-implies `.longest()` | `all TempReading.increasing(temperature) as rising` |
| `.decreasing(field)` | Sugar for `where field < self.field` Kleene+; auto-implies `.longest()` | `all Sensor.decreasing(pressure) as falling` |


## Duration Operators

```varpulis
# Duration suffixes
5s      # 5 seconds
10m     # 10 minutes
2h      # 2 hours
1d      # 1 day
500ms   # 500 milliseconds
100us   # 100 microseconds
50ns    # 50 nanoseconds
```

## Operator Precedence (highest to lowest)

1. `()`, `[]`, `.`, `?.`
2. `**`
3. `-` (unary), `not`, `~`
4. `*`, `/`, `%`
5. `+`, `-`
6. `<<`, `>>`
7. `&`
8. `^`
9. `|`
10. `<`, `<=`, `>`, `>=`, `==`, `!=`, `in`, `not in`
11. `and`
12. `or`
13. `??`
14. `:=`
