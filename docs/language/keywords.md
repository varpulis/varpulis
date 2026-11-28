# Keywords

The words VPL gives a meaning to. Having a meaning does not mean being run. The engine that ships (the
`varpulis-engine` library, the CLI and the WASM build) refuses at load the
constructs it does not execute, with the code given in the tables, rather than
loading them and skipping them. The last sections list the words that are
set aside and not implemented at all.

## Declarations

| Keyword | Description |
|---------|-------------|
| `stream` | Declares a stream |
| `event` | Declares an event type |
| `type` | Declares a type alias |
| `let` | Declares an immutable variable |
| `var` | Declares a mutable variable |
| `const` | Declares a global constant |
| `fn` | Declares a function |
| `context` | Declares an execution context. Refused by the shipped engine (E101): one program runs on one thread |
| `config` | Configuration block |

## Control Flow

| Keyword | Description |
|---------|-------------|
| `if` | Condition |
| `else` | Alternative branch |
| `elif` | Alternative condition (else if) |
| `for` | Loop over iterable |
| `while` | Conditional loop |
| `break` | Exit loop |
| `continue` | Skip to next iteration |
| `return` | Return a value |

## Stream Operators

| Keyword | Description |
|---------|-------------|
| `from` | Connector source binding (`.from()`) |
| `where` | Filtering |
| `select` | Projection |
| `join` | Inner stream join. Not run by the shipped engine: the first event a join receives is an error |
| `left_join` | Left outer join. Not run by the shipped engine: the first event a join receives is an error |
| `right_join` | Right outer join. Not run by the shipped engine: the first event a join receives is an error |
| `full_join` | Full outer join. Not run by the shipped engine: the first event a join receives is an error |
| `merge` | Stream merge |
| `window` | Time window |
| `aggregate` | Aggregation |
| `partition_by` | Partitioning |
| `order_by` | Sorting |
| `limit` | Limitation |
| `distinct` | Deduplication |
| `emit` | Emit output events |
| `forecast` | PST-based pattern forecasting |

### Forecast Built-in Variables

| Variable | Description |
|----------|-------------|
| `forecast_probability` | Pattern completion probability (0.0–1.0) |
| `forecast_time` | Expected time to completion (nanoseconds) |
| `forecast_state` | Current NFA state label |
| `forecast_context_depth` | PST context depth used for prediction |

## Patterns

| Keyword | Description |
|---------|-------------|
| `pattern` | Defines a detection pattern |

## Types and Values

| Keyword | Description |
|---------|-------------|
| `true` | Boolean true |
| `false` | Boolean false |
| `null` | Absence of value |
| `int` | Integer type |
| `float` | Floating point type |
| `bool` | Boolean type |
| `str` | String type |
| `timestamp` | Timestamp type |
| `duration` | Duration type |

## Logical Operators

| Keyword | Description |
|---------|-------------|
| `and` | Logical AND, in an expression; in a `pattern`, both events in any order |
| `or` | Logical OR, in an expression; in a `pattern`, either event |
| `not` | Logical NOT, in an expression |

`and` and `or` combine conditions inside a `.where()`. In a `pattern`
declaration they also combine events: `A and B` matches both, in any order,
within the pattern's `within`; `A or B` matches either. They bind tighter than
`->`, and `and` tighter than `or`: `X -> A or B and C` is
`X -> (A or (B and C))`. A sequence is written with `->`, an absence with
`-> NOT B` (uppercase, a step of its own). See
[SASE+ patterns](../guides/sase-patterns.md#both-in-any-order-and).
| `in` | Membership |
| `is` | Type/identity test |

## Other

| Keyword | Description |
|---------|-------------|
| `as` | Alias/cast |
| `extends` | Event inheritance |
| `import` | Module import. Refused by the shipped engine (E100): imports are not resolved, so a program is one file. `import fn ... from "f.wasm"` loads a WASM function only in builds with the `wasm-udf` feature |

## Stream operators refused by the shipped engine

Not keywords, but parsed and then refused at load, for the same reason:

| Operator | Code | Why |
|----------|------|-----|
| `.enrich(...)` | E103 | The lookup is not performed. The host enriches the event before feeding it |
| `.alert(webhook: ...)` | E104 | The webhook is not called. Route the stream with `.to()`; `.alert(message: ...)` without a webhook only logs |

`timer(5s)` as a stream source is run, on a clock the host drives: the
library's `Program::next_deadline` says when a timer is next due and
`Program::fire_due(now)` fires it, or the events fed move the timers when
the host chooses event time, as `varpulis simulate` does. A clock that jumps
over several periods fires a timer once, for the latest of them, and counts
the others as skipped. The period and the `initial_delay` are duration
literals:

```vpl
stream Beat = timer(1m, initial_delay: 10s)
    .emit(kind: "beat")
```

A period or delay that is not a duration literal, or a period of zero, is
refused (E102): an expression used to become one second, and a zero period
would fire without end.

<!-- engine-refuses: E102 -->
```vpl
stream Beat = timer(0s)
    .emit(kind: "beat")
```

The WASM build has no clock to fire a timer on and refuses every `timer()`
with E102.

## Words set aside

These words have no meaning in VPL today; there is no `match` expression and
no `try`/`catch`. The editor tooling may highlight some of them, the engine
gives them none:

- `match`
- `try`, `catch`, `finally`, `raise`
- `export`
- `async`, `await`
- `class`, `trait`, `impl`
- `pub`, `priv`
- `mod`, `use`
- `yield`
- `defer`
- `go`, `spawn`
