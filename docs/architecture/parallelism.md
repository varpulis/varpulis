# Parallélisation et supervision

## Modèle de parallélisation

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

## Parallélisation déclarative

```varpulis
stream OrderProcessing = Orders
    .partition_by(customer_id)  # Isolation automatique
    .concurrent(
        max_workers: 8,
        strategy: "consistent_hash",  # ou "round_robin"
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

## Stratégies de partitionnement

| Stratégie | Description | Usage |
|-----------|-------------|-------|
| **consistent_hash** | Hash stable de la clé | Garantie d'ordre par clé |
| **round_robin** | Distribution égale | Load balancing |
| **broadcast** | Envoi à tous les workers | Enrichissement, référentiels |

## Supervision automatique

### Configuration

```varpulis
supervision:
    # Stratégie de restart
    restart_policy: "always"  # ou "on_failure", "never"
    
    # Backoff exponentiel
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

### Politiques de restart

| Politique | Comportement |
|-----------|-------------|
| **always** | Redémarre toujours après échec |
| **on_failure** | Redémarre uniquement sur erreur |
| **never** | Ne redémarre jamais |

### Circuit Breaker

États:
1. **Closed**: Fonctionnement normal
2. **Open**: Circuit ouvert après N échecs, rejette les requêtes
3. **Half-Open**: Test de quelques requêtes pour vérifier la recovery

## Garanties d'ordre

- **Par partition**: Ordre strict garanti
- **Global**: Ordre approximatif via watermarks
- **Exactement une fois**: Combinaison checkpointing + idempotence
