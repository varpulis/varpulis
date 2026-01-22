# Objectifs de performance

## Latence (mode low_latency)

| Percentile | Objectif |
|------------|----------|
| p50 | < 500 µs |
| p95 | < 2 ms |
| p99 | < 5 ms |

## Throughput (mode throughput)

| Scénario | Objectif |
|----------|----------|
| Events simples | > 1M events/sec (single node) |
| Avec attention | > 100K events/sec |
| Avec agrégations complexes | > 500K events/sec |

## Comparaison vs Apama (indicatif)

| Métrique | Apama | Varpulis (cible) |
|----------|-------|------------------|
| Latence | Référence | Similaire en mode low_latency |
| Throughput | Référence | 2-3x supérieur grâce à Rust |
| Memory footprint | Référence | -40% |

## Modes de performance

### Mode `low_latency`
- Priorité à la latence minimale
- Batch size = 1
- Pas de buffering
- Idéal pour: trading, alerting temps réel

### Mode `throughput`
- Priorité au débit maximal
- Batching agressif
- Buffering optimisé
- Idéal pour: analytics, agrégations massives

### Mode `balanced`
- Compromis latence/throughput
- Configuration par défaut
- Idéal pour: cas d'usage généraux

## Benchmarks à implémenter

1. **Latency benchmark**: Mesure p50/p95/p99 sur événements simples
2. **Throughput benchmark**: Events/sec maximum soutenu
3. **Memory benchmark**: Consommation mémoire sous charge
4. **Pattern matching benchmark**: Complexité des patterns vs performance
5. **Attention benchmark**: Impact du nombre de heads sur la latence
