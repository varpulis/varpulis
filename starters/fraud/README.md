# Fraud Detection Starter

Account takeover detection with predictive pattern forecasting.

## The Pattern

Detects **Login -> PasswordChange -> Transaction** sequences within 30 minutes.

Uses `.forecast()` to predict when a pattern is *about to complete* — alerting before the fraudulent transaction happens, not after.

## Quick Start

### 1. Run it

```bash
docker compose up
```

That's it. Varpulis loads `events.jsonl` in preload mode and processes all events.

### 2. What to expect

The event file contains 5 attack sequences (alice, bob, dave, eve, charlie) mixed with normal user activity. You should see alerts like:

```
ACCOUNT_TAKEOVER | user: alice | amount: 2500 | probability: 0.85 | severity: HIGH
ACCOUNT_TAKEOVER | user: bob   | amount: 8000 | probability: 0.92 | severity: CRITICAL
```

## How it works

1. **Pattern matching**: SASE+ engine tracks Login -> PasswordChange -> Transaction per user
2. **Forecasting**: After warmup (50 events), the PST model learns transition patterns
3. **Early alert**: When `forecast_probability > 0.6`, fires before the final event

## Files

| File | Description |
|------|-------------|
| `pipeline.vpl` | Detection pipeline with `.forecast()` |
| `events.jsonl` | 82 events: 5 attacks + normal activity |
| `docker-compose.yml` | Varpulis in simulate mode |

## Next steps

- Add more event types (e.g., `GeoLocation` for travel anomaly)
- Lower `forecast_probability` threshold to catch more (with more false positives)
- Add a `.to()` sink to forward alerts to Kafka/webhook
- See [full fraud example](../../examples/forecast_fraud.vpl) for 6 patterns
- Read the [forecasting docs](../../docs/forecasting.md)
