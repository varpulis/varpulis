# Parallelization and Supervision

## Parallelization Model

```
                    ┌─────────────┐
                    │   Scheduler │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐       ┌─────▼────┐      ┌─────▼────┐
   │ Worker 1│       │ Worker 2 │      │ Worker N │
   │         │       │          │      │          │
   │Partition│       │Partition │      │Partition │
   │    1    │       │    2     │      │    N     │
   └─────────┘       └──────────┘      └──────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Collector  │
                    └─────────────┘
```

## Declarative Parallelization

```varpulis
stream OrderProcessing = Orders
    .partition_by(customer_id)  # Automatic isolation
    .concurrent(
        max_workers: 8,
        strategy: "consistent_hash",  # or "round_robin"
        supervision:
            restart: "always"
            max_restarts: 3
            backoff: "exponential"
    )
    .process(order =>
        validate_order(order)?
        check_inventory(order)?
        calculate_shipping(order)?
        Ok(order)
    )
    .on_error((error, order) =>
        log_error(error)
        emit_to_dlq(order)
    )
    .collect()  # Remerge les résultats
```

## Partitioning Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **consistent_hash** | Stable key hashing | Order guarantee per key |
| **round_robin** | Equal distribution | Load balancing |
| **broadcast** | Send to all workers | Enrichment, reference data |

## Automatic Supervision

### Configuration

```varpulis
supervision:
    # Restart strategy
    restart_policy: "always"  # or "on_failure", "never"
    
    # Exponential backoff
    backoff:
        initial: 1s
        max: 1m
        multiplier: 2.0
    
    # Circuit breaker
    circuit_breaker:
        enabled: true
        failure_threshold: 5
        timeout: 30s
        half_open_requests: 3
    
    # Health checks
    health_check:
        interval: 5s
        timeout: 1s
```

### Restart Policies

| Policy | Behavior |
|--------|----------|
| **always** | Always restart after failure |
| **on_failure** | Restart only on error |
| **never** | Never restart |

### Circuit Breaker

States:
1. **Closed**: Normal operation
2. **Open**: Circuit open after N failures, rejects requests
3. **Half-Open**: Test a few requests to verify recovery

## Order Guarantees

- **Per partition**: Strict order guaranteed
- **Global**: Approximate order via watermarks
- **Exactly once**: Checkpointing + idempotence combination
