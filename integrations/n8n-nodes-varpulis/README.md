# n8n-nodes-varpulis

**Add temporal pattern detection to any n8n workflow.**

This community node brings the [Varpulis CEP engine](https://www.varpulis-cep.com/) into n8n. It runs entirely inside your n8n instance via WebAssembly -- no sidecar process, no external service, no network calls. Define patterns in Varpulis Pattern Language (VPL), feed events from any n8n trigger, and route detected matches to downstream nodes.

## What it does

The Varpulis node watches a stream of incoming n8n items and detects **temporal patterns** across them -- sequences of events that occur in a specific order within a time window, with conditions on their fields. When a pattern matches, it emits a new item on the **Matches** output. Every input item is also forwarded unchanged on the **Passthrough** output.

Use cases:

- **Payment fraud**: detect rapid sequences of failed-then-succeeded transactions
- **IoT anomaly**: catch temperature spikes followed by pressure drops within a window
- **User behavior**: identify sign-up-then-churn patterns for engagement workflows
- **Monitoring**: correlate error bursts across services in real time

## How it works

```
Trigger --> [Varpulis CEP] --Matches--> Slack / DB / HTTP / ...
                |
                +--Passthrough--> next node
```

1. You write a VPL pattern that declares event types and stream definitions.
2. For each execution, a fresh WASM engine loads the pattern and processes every input item as an event.
3. Items that trigger a pattern match appear on the **Matches** output with emitted fields.
4. All original items pass through unchanged on the **Passthrough** output.

The WASM engine is ~2 MB and initializes in milliseconds. There is no state between executions.

## Installation

### In n8n (community nodes)

1. Go to **Settings > Community Nodes**
2. Enter `n8n-nodes-varpulis`
3. Click **Install**

### Manual (self-hosted)

```bash
cd ~/.n8n/nodes
npm install n8n-nodes-varpulis
# Restart n8n
```

### From source (development)

```bash
cd integrations/n8n-nodes-varpulis
npm install
npm run build
# Then link into your n8n instance:
npm link
cd ~/.n8n/nodes
npm link n8n-nodes-varpulis
```

## Node properties

| Property | Description |
|----------|-------------|
| **VPL Pattern** | The full VPL program -- event declarations and stream definitions |
| **Event Type** | The event type name to assign to each incoming item (must match a declared event) |

## Example: Stripe payment fraud detection

Detect a failed payment followed by a successful payment from the same customer within 5 minutes:

**VPL Pattern:**
```
event Payment:
    customer_id: string
    status: string
    amount: float

stream SuspiciousRetry =
    Payment as failed -> Payment as succeeded
    .where(failed.status == "failed"
       AND succeeded.status == "succeeded"
       AND succeeded.customer_id == failed.customer_id
       AND succeeded.amount >= failed.amount)
    .within(5m)
    .emit(
        customer: failed.customer_id,
        failed_amount: failed.amount,
        succeeded_amount: succeeded.amount
    )
```

**Event Type:** `Payment`

Wire a Stripe webhook trigger into the Varpulis node, and connect the Matches output to a Slack notification or database insert.

## Example: IoT temperature anomaly

Detect a high-temperature reading followed by a pressure drop within 2 minutes:

**VPL Pattern:**
```
event Sensor:
    zone: string
    temperature: float
    pressure: float

stream TempPressureAnomaly =
    Sensor as hot -> Sensor as drop
    .where(hot.temperature > 80
       AND drop.pressure < 30
       AND drop.zone == hot.zone)
    .within(2m)
    .emit(
        zone: hot.zone,
        peak_temp: hot.temperature,
        low_pressure: drop.pressure
    )
```

**Event Type:** `Sensor`

## VPL syntax basics

```
# Declare an event type with typed fields
event <Name>:
    <field>: <type>    # string, int, float, bool

# Define a pattern stream
stream <Name> =
    <EventType> as <alias> -> <EventType> as <alias>  # sequence
    .where(<conditions>)                                # filter
    .within(<duration>)                                 # time window (s/m/h)
    .emit(<field>: <expr>, ...)                         # output fields
```

Supported types: `string`, `int`, `float`, `bool`

Duration units: `s` (seconds), `m` (minutes), `h` (hours)

Conditions support: `AND`, `OR`, `NOT`, comparisons (`==`, `!=`, `>`, `<`, `>=`, `<=`), arithmetic (`+`, `-`, `*`, `/`).

For full syntax including Kleene patterns, negation, aggregations, and forecasting, see the [VPL documentation](https://www.varpulis-cep.com/docs/syntax).

## Outputs

| Output | Description |
|--------|-------------|
| **Matches** | Items emitted when a pattern matches. Contains the `.emit()` fields plus `_stream` (stream name), `_timestamp`, and `_sourceIndex` (which input item triggered the match). |
| **Passthrough** | Every input item, unchanged. Use this to continue processing regardless of matches. |

## Links

- [Varpulis documentation](https://www.varpulis-cep.com/docs/)
- [VPL syntax reference](https://www.varpulis-cep.com/docs/syntax)
- [GitHub repository](https://github.com/varpulis/varpulis)
- [Example workflows](./examples/)

## License

MIT
