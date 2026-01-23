# Event Listeners and Temporal Sequences

## Context

### Problem Statement
Current VarpulisQL is **stream-centric**: everything is a stream transformation. This works well for:
- Temporal aggregations (`window`, `aggregate`)
- Filtering and projection (`where`, `select`)
- Joins (`join`, `merge`)
- Pattern matching within windows (`pattern`)

However, it does not easily support:
- Reacting to **correlated event sequences**
- Waiting for an event **after** another with timeout
- Creating **nested listeners** dynamically

### Reference: Apama EPL

```epl
monitor NewsCorrelation {
   action onload() {
      on all NewsItem() as news {
         on StockTick(symbol=news.subject) as tick {
            on StockTick(symbol=news.subject, price >= tick.price * 1.05)
               within(300.0) alertUser;
         }
      }
   }
}
```

Key Apama concepts:
| Concept | Description |
|---------|-------------|
| `on EventType()` | Listen for ONE event then terminate |
| `on all EventType()` | Listen for ALL events (continues) |
| `->` (followed-by) | Sequence: A followed by B |
| `within(seconds)` | Temporal constraint |
| `:alias` | Capture the matched event |
| Nesting | Dynamically created listeners |

---

## Approach Analysis

### Approach 1: Apama Style (Imperative)

```varpulis
on all NewsItem as news {
    on StockTick where symbol == news.subject as tick {
        on StockTick where symbol == news.subject 
                      and price >= tick.price * 1.05
           .within(5m)
        => emit(alert_type: "NEWS_CORRELATION", symbol: news.subject)
    }
}
```

**Pros:**
- Familiar syntax for Apama users
- Maximum expressivity for complex sequences
- Natural nesting

**Cons:**
- Imperative paradigm (vs declarative streams)
- Implicit state management (where are `news`, `tick` stored?)
- Complex parallelism (each listener = isolated context?)
- Two "languages" within VarpulisQL

### Approach 2: `->` Operator (Declarative)

```varpulis
stream NewsCorrelation = NewsItem
    -> StockTick where symbol == $.subject as tick
    -> StockTick where symbol == $.subject and price >= tick.price * 1.05
       .within(5m)
    .emit(alert_type: "NEWS_CORRELATION", symbol: $.subject)
```

**Pros:**
- Stays within stream paradigm
- Concise syntax
- `$` references the previous event in sequence

**Cons:**
- Less flexible for very complex sequences
- `all` vs `one` less explicit

### Approach 3: `sequence()` (Named Declarative)

```varpulis
stream NewsCorrelation = sequence(
    news: NewsItem,
    tick: StockTick where symbol == news.subject,
    pump: StockTick where symbol == news.subject 
                     and price >= tick.price * 1.05
          .within(5m)
)
.emit(
    alert_type: "NEWS_CORRELATION",
    symbol: news.subject,
    initial_price: tick.price,
    final_price: pump.price
)
```

**Pros:**
- All variables explicitly named
- Excellent readability
- Integrates with stream paradigm

**Cons:**
- Linear sequence only (no branches)
- `all` implicit on first element?

### Approach 4: Unified Hybrid (Recommended)

```varpulis
# Simple sequences via -> operator
stream SimpleCorrelation = NewsItem
    -> StockTick(symbol: $.subject).within(5m)
    .emit(...)

# Complex sequences via sequence()
stream ComplexCorrelation = sequence(
    mode: "all",  # all = continue, one = single match
    steps: [
        news: NewsItem,
        tick: StockTick where symbol == news.subject,
        pump: StockTick where symbol == news.subject 
                         and price >= tick.price * 1.05
              .within(5m)
    ]
)
.emit(...)

# Complex cases with branches
stream BranchedCorrelation = NewsItem as news
    .fork(
        path_a: -> StockTick(symbol: news.subject, price > news.threshold).within(1m),
        path_b: -> AnalystRating(symbol: news.subject, rating: "buy").within(1h)
    )
    .any()  # or .all() to wait for both
    .emit(...)
```

---

## Proposed Unified Syntax

### 1. `->` Operator (Followed-By)

```ebnf
stream_expr     ::= stream_source (stream_op | followed_by)*

followed_by     ::= '->' event_matcher

event_matcher   ::= IDENTIFIER                           # Event type
                  | IDENTIFIER '(' filter_args ')'       # With positional filters
                  | IDENTIFIER 'where' expr              # With condition
                  | IDENTIFIER 'where' expr 'as' IDENT   # With alias
```

Examples:
```varpulis
# Simple sequence
stream S = A -> B -> C

# With filters
stream S = OrderCreated -> PaymentReceived(order_id: $.id).within(30s)

# With alias
stream S = NewsItem as news 
    -> StockTick where symbol == news.subject as tick
    -> StockTick where price >= tick.price * 1.05
       .within(5m)
```

### 2. Quantifiers `one` / `all`

```varpulis
# Default: one (first match, then terminates this branch)
stream S = A -> B

# Explicit: all (continues matching)
stream S = all A -> B           # All A, first B after each A
stream S = A -> all B           # First A, all B after
stream S = all A -> all B       # All A, all B
```

### 3. `sequence()` Construct

For more complex named sequences:

```varpulis
stream Correlation = sequence(
    # Configuration
    mode: "all",              # "one" | "all"
    timeout: 5m,              # Optional global timeout
    
    # Steps (order matters)
    steps:
        news: NewsItem,
        tick: StockTick where symbol == news.subject,
        confirmation: StockTick 
            where symbol == news.subject 
              and price >= tick.price * 1.05
            .within(5m)
)
.emit(
    event_type: "correlation_detected",
    symbol: news.subject,
    price_change: (confirmation.price - tick.price) / tick.price
)
```

### 4. Branches with `fork()` / `any()` / `all()`

```varpulis
stream MultiPath = OrderCreated as order
    .fork(
        payment: -> PaymentReceived(order_id: order.id).within(30m),
        shipping: -> ShippingConfirmed(order_id: order.id).within(2h),
        cancel: -> OrderCancelled(order_id: order.id)
    )
    .first()    # First path that matches
    # or .all()  # All paths must match
    # or .any(2) # At least 2 paths
    .emit(...)
```

### 5. Negation with `.not()`

```varpulis
# A occurred but B did not occur within 30 seconds
stream Timeout = OrderCreated as order
    -> .not(PaymentReceived where order_id == order.id).within(30s)
    .emit(alert_type: "PAYMENT_TIMEOUT", order_id: order.id)

# No activity detected for 10 seconds
stream Inactivity = HeartbeatExpected
    -> .not(Heartbeat where device_id == $.device_id).within(10s)
    .emit(alert_type: "DEVICE_OFFLINE", device_id: $.device_id)
```

---

## Execution Model

### State and Parallelism

```
                    ┌─────────────────────────────────────┐
                    │         Sequence Manager            │
                    │                                     │
                    │  ┌─────────────────────────────┐    │
    NewsItem ──────►│  │  Active Correlations        │    │
                    │  │                             │    │
                    │  │  ┌───────────────────────┐  │    │
                    │  │  │ Correlation #1        │  │    │
                    │  │  │ news: {ACME, ...}     │  │    │
                    │  │  │ state: waiting_tick   │  │    │
   StockTick ──────►│  │  │ timeout: 5m           │  │    │
                    │  │  └───────────────────────┘  │    │
                    │  │                             │    │
                    │  │  ┌───────────────────────┐  │    │
                    │  │  │ Correlation #2        │  │    │
                    │  │  │ news: {MSFT, ...}     │  │    │
                    │  │  │ tick: {MSFT, 150}     │  │    │
                    │  │  │ state: waiting_pump   │  │    │
                    │  │  └───────────────────────┘  │    │
                    │  └─────────────────────────────┘    │
                    └─────────────────────────────────────┘
```

### Parallelism Rules

1. **Isolation per correlation**: Each sequence instance has its own context
2. **Natural partitioning**: Parallelization possible by correlation key (e.g., `symbol`)
3. **Timeout management**: Timers managed by runtime, no impact on main thread
4. **Memory bounds**: Configurable limit on active correlations

```varpulis
stream Correlation = sequence(
    mode: "all",
    # Resource constraints
    max_active: 10000,           # Max simultaneous correlations
    eviction: "oldest",          # oldest | lru | error
    partition_by: news.symbol,   # Automatic parallelization
    
    steps:
        news: NewsItem,
        tick: StockTick where symbol == news.subject
              .within(5m)
)
```

---

## Comparison with Apama

| Feature | Apama | VarpulisQL Proposed |
|---------|-------|---------------------|
| Single listener | `on A()` | `A` or `one A` |
| Continuous listener | `on all A()` | `all A` |
| Sequence | `A() -> B()` | `A -> B` |
| Timeout | `within(30.0)` | `.within(30s)` |
| Capture | `A():a` | `A as a` |
| Filter | `A(field=value)` | `A where field == value` |
| Nested | Explicit nesting | `sequence()` or chained `->` |
| And | `A() and B()` | `.fork().all()` |
| Or | `A() or B()` | `.fork().any()` or `.first()` |
| Not | `and not A()` | `.not(A)` |

### VarpulisQL Advantages

1. **Unified syntax**: Streams and sequences use the same operators
2. **Declarative**: No manual state management
3. **Explicit parallelism**: `partition_by` instead of implicit
4. **Type-safe**: Capture variables are statically typed
5. **Composable**: Sequences are streams, can be chained

---

## Implementation Plan

### Phase 1: Simple `->` Operator
- [ ] Parser: add `followed_by` to `stream_op`
- [ ] AST: `StreamOp::FollowedBy { event_type, filter, alias, within_ms }`
- [ ] Runtime: `SequenceTracker` to manage active correlations

### Phase 2: `one` / `all` Quantifiers
- [ ] Parser: `all` prefix on event matcher
- [ ] Runtime: listener duplication mode

### Phase 3: `sequence()` Construct
- [ ] Parser: `sequence()` as `stream_source`
- [ ] AST: `StreamSource::Sequence { mode, steps, constraints }`
- [ ] Runtime: complete `SequenceManager`

### Phase 4: `fork()` and Branches
- [ ] Parser: `.fork()`, `.any()`, `.all()`, `.first()`
- [ ] Runtime: parallel branch management

---

## Design Decisions

| Question | Decision | Justification |
|----------|----------|---------------|
| Timeout syntax | `.within(5m)` | Method style consistent with the rest |
| Previous reference | `$` allowed | Concise, well documented |
| Negation | `.not(EventType)` | Clearer than `.unless()` |
| Persistent state | TBD | Depends on backend (RocksDB vs in-memory) |

---

## Complete Examples

### Pump & Dump Detection

```varpulis
event NewsItem:
    subject: str
    headline: str
    timestamp: timestamp

event StockTick:
    symbol: str
    price: float
    volume: int
    timestamp: timestamp

stream PumpAndDump = all NewsItem as news
    -> StockTick where symbol == news.subject as baseline
    -> StockTick where symbol == news.subject 
                   and price >= baseline.price * 1.05
                   and volume >= baseline.volume * 3
       .within(5m)
    -> StockTick where symbol == news.subject
                   and price <= baseline.price * 0.95
       .within(30m)
    .emit(
        alert_type: "PUMP_AND_DUMP",
        symbol: news.subject,
        news_headline: news.headline,
        peak_price: $[2].price,
        dump_price: $[3].price
    )
```

### Multi-Source Correlation

```varpulis
stream FraudAlert = sequence(
    mode: "all",
    partition_by: card.number,
    max_active: 100000,
    
    steps:
        card: CardTransaction where amount > 1000,
        atm: ATMWithdrawal where card_number == card.number
             .within(10m),
        online: OnlinePurchase where card_number == card.number
                .within(30m)
)
.where(atm.location.country != online.ip_country)
.emit(
    alert_type: "POSSIBLE_FRAUD",
    card_number: card.number,
    atm_location: atm.location,
    online_ip: online.ip_address,
    risk_score: calculate_risk(card, atm, online)
)
```

### Order Timeout Detection

```varpulis
stream OrderTimeouts = all OrderCreated as order
    -> .not(PaymentReceived where order_id == order.id).within(30m)
    .emit(
        alert_type: "PAYMENT_TIMEOUT",
        order_id: order.id,
        customer_id: order.customer_id,
        amount: order.total
    )
```
