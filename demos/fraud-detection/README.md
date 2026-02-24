# Fraud Detection Demo

Real-time fraud detection using Varpulis pattern matching.

## Quick Start

```bash
docker-compose up
```

Then open:
- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Varpulis API**: http://localhost:9000/health

## Patterns Detected

| Pattern | Description | VPL |
|---------|-------------|-----|
| Suspicious Transfer | Login followed by >$5K transfer within 5 min | Sequence + within |
| Rapid Transfers | 3+ transfers totaling >$2K in a window | Window + aggregate |
| High-Value Payment | Single card payment >$10K | Filter |

## Architecture

```
[Event Generator] --events--> [Varpulis Engine] --alerts--> [Grafana]
  (fraud schema)                (fraud_pipeline.vpl)         (dashboard)
```

## Event Types

- `login` — user authentication events
- `transfer` — bank transfers
- `card_payment` — card transactions

## Customization

Edit `fraud_pipeline.vpl` to modify detection patterns, thresholds, or add new patterns.

```bash
# Adjust event rate
docker-compose run generator generate --schema fraud --rate 500 --duration 60s
```
