# Alert Notifications Tutorial

The `.alert()` operator sends webhook notifications when events match your pipeline conditions. It is a side-effect operator: it fires the webhook and then lets events continue downstream to subsequent operators like `.emit()`.

## How `.alert()` Works

`.alert()` accepts two named parameters:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `webhook` | No | URL to POST the alert payload to (HTTP) |
| `message` | No | Message template with `{field}` interpolation |

When an event reaches `.alert()`:

1. The `message` template is rendered by replacing `{field_name}` placeholders with actual event field values.
2. If a `webhook` URL is provided, an HTTP POST is sent asynchronously (fire-and-forget) with a JSON body containing the rendered message.
3. If no `webhook` is set, the alert message is logged at INFO level.
4. The event continues to the next operator in the pipeline -- `.alert()` never consumes or modifies events.

## Basic Webhook Alert

```vpl
event TemperatureReading:
    sensor_id: str
    temperature: float
    zone: str

stream HighTempAlert = TemperatureReading
    .where(temperature > 100)
    .alert(webhook: "https://example.com/hooks/alerts", message: "High temp: {temperature}F in zone {zone}")
    .emit(sensor: sensor_id, temp: temperature)
```

When a `TemperatureReading` event arrives with `temperature > 100`:

1. The `.alert()` operator POSTs the following JSON to the webhook URL:
   ```json
   {
     "text": "High temp: 105.2F in zone A",
     "event_type": "alert",
     "stream": "HighTempAlert"
   }
   ```
2. The event then continues to `.emit()`, which produces an output event with the `sensor` and `temp` fields.

## Template Interpolation

The `message` parameter supports `{field_name}` placeholders. Each placeholder is replaced with the string representation of the corresponding field from the current event.

```vpl
stream FraudAlert = Transaction
    .where(amount > 10000 && country != "US")
    .alert(
        webhook: "https://example.com/fraud-webhook",
        message: "Suspicious transaction: {user_id} spent {amount} at {merchant} in {country}"
    )
    .emit(user: user_id, amount: amount, country: country)
```

For an event `Transaction { user_id: "u42", amount: 15000, merchant: "Watches", country: "CH" }`, the rendered message would be:

```
Suspicious transaction: u42 spent 15000 at Watches in CH
```

If a placeholder references a field that does not exist in the event, the literal `{field_name}` text is left in place.

## Chaining Alert with Emit

Since `.alert()` is non-consuming, it works naturally in operator chains. The most common pattern is `.where()` then `.alert()` then `.emit()`:

```vpl
stream SecurityAlerts = LoginEvent
    .where(failed_attempts > 5)
    .alert(webhook: "https://example.com/security", message: "Brute force: {username} from {ip_address}")
    .emit(
        alert_type: "brute_force",
        username: username,
        ip: ip_address,
        attempts: failed_attempts
    )
```

The `.alert()` fires the webhook, and the `.emit()` produces a structured output event that downstream streams or consumers can process further.

## Slack Integration

Slack incoming webhooks accept the same `{"text": "..."}` JSON format that `.alert()` produces. Point the `webhook` parameter at your Slack webhook URL:

```vpl
stream SlackAlerts = ServerMetric
    .where(cpu_percent > 90)
    .alert(
        webhook: "https://example.com/slack-webhook",
        message: "CPU alert: {hostname} at {cpu_percent}% (threshold: 90%)"
    )
    .emit(host: hostname, cpu: cpu_percent)
```

The Slack channel configured for that webhook URL will receive messages like:

```
CPU alert: web-server-03 at 95.2% (threshold: 90%)
```

Any service that accepts HTTP POST with a JSON body containing a `text` field will work the same way (Microsoft Teams, Discord, PagerDuty generic webhooks, etc.).

## Alert Without Webhook (Logging Only)

If you omit the `webhook` parameter, the alert is logged to the Varpulis log output at INFO level instead of being sent over HTTP:

```vpl
stream LoggedAlerts = ErrorEvent
    .where(severity == "critical")
    .alert(message: "Critical error in {service}: {error_message}")
    .emit(service: service, error: error_message)
```

This writes to the log:

```
INFO varpulis::engine: Alert: Critical error in payment-service: connection timeout
```

This is useful during development or when you want alert semantics (a visible side-effect) without external HTTP calls.

## Fire-and-Forget Behavior

The `.alert()` webhook call is asynchronous and non-blocking:

- The HTTP POST is spawned as a background task. The pipeline does not wait for a response.
- If the webhook endpoint is unreachable or returns an error, a warning is logged but the event continues downstream.
- Failed webhooks never cause event loss or pipeline stalls.

This design ensures that alert delivery issues do not affect pipeline throughput or correctness.

## Complete Example: Fraud Detection with Alerts

Here is a full pipeline that detects suspicious transactions, alerts a Slack channel, and emits structured output:

Create `fraud_alerts.vpl`:

```vpl
// Declare event types
event Transaction:
    user_id: str
    amount: float
    merchant: str
    country: str
    card_type: str

// High-value foreign transactions
stream ForeignHighValue = Transaction
    .where(amount > 5000 && country != "US")
    .alert(
        webhook: "https://example.com/slack-webhook",
        message: "Foreign high-value: {user_id} spent {amount} at {merchant} in {country}"
    )
    .emit(
        alert_type: "foreign_high_value",
        user: user_id,
        amount: amount,
        country: country
    )

// Any single transaction over $10,000
stream VeryLargeTransaction = Transaction
    .where(amount > 10000)
    .alert(
        webhook: "https://example.com/slack-webhook",
        message: "Very large transaction: {user_id} charged {amount} on {card_type}"
    )
    .emit(
        alert_type: "very_large",
        user: user_id,
        amount: amount
    )
```

Create test events in `fraud_events.evt`:

```
Transaction { user_id: "u001", amount: 250.0, merchant: "Coffee", country: "US", card_type: "visa" }
Transaction { user_id: "u002", amount: 8500.0, merchant: "Jewelry", country: "CH", card_type: "amex" }
Transaction { user_id: "u003", amount: 15000.0, merchant: "Cars", country: "US", card_type: "visa" }
Transaction { user_id: "u004", amount: 12000.0, merchant: "Electronics", country: "RU", card_type: "mastercard" }
```

Run the simulation:

```bash
varpulis simulate -p fraud_alerts.vpl -e fraud_events.evt -v -w 1
```

Expected behavior:

- Event 1 (`u001`, $250, US): No alerts, no output (below thresholds)
- Event 2 (`u002`, $8500, CH): Triggers `ForeignHighValue` (foreign + over $5000)
- Event 3 (`u003`, $15000, US): Triggers `VeryLargeTransaction` (over $10000, but domestic so not `ForeignHighValue`)
- Event 4 (`u004`, $12000, RU): Triggers both `ForeignHighValue` and `VeryLargeTransaction`

In simulation mode, webhook HTTP calls are attempted if the URLs are reachable. In production with `varpulis run`, alerts fire continuously as events stream in.

## See Also

- [VPL Language Tutorial](language-tutorial.md) -- Comprehensive language guide including `.where()` and `.emit()`
- [Getting Started](getting-started.md) -- Installation and first pipeline
- [Debugging Pipelines](../guides/debugging-pipelines.md) -- Use trace mode to verify alert operator behavior
- [Language Operators Reference](../language/operators.md) -- Full operator reference
- [Configuration Guide](../guides/configuration.md) -- Production deployment with connectors
