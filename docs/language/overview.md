# VarpulisQL - Vue d'ensemble

## Philosophie de design

- **Tout est un Stream** : Pas de distinction artificielle listeners/streams
- **Déclaratif** : L'utilisateur décrit QUOI, pas COMMENT
- **Composable** : Les opérations s'enchaînent naturellement
- **Type-safe** : Détection d'erreurs à la compilation
- **Observable** : Métriques et traces intégrées
- **Pythonique** : Syntaxe inspirée de Python (indentation optionnelle, clarté)

## Exemple rapide

```varpulis
# Définition d'un stream simple
stream Trades from TradeEvent

# Filtrage et agrégation
stream HighValueTrades = Trades
    .where(price > 10000)
    .window(5m)
    .aggregate(
        total: sum(price),
        count: count()
    )

# Pattern detection avec attention
stream FraudAlert = Trades
    .attention_window(30s, heads: 4)
    .pattern(
        suspicious: events =>
            events.filter(e => e.amount > 10000).count() > 3
            and attention_score(events[0], events[-1]) > 0.85
    )
    .emit(
        alert_type: "fraud",
        severity: "high"
    )
```

## Différences avec Python

| Aspect | Python | VarpulisQL |
|--------|--------|------------|
| **Paradigme** | Impératif/OO | Déclaratif/Stream |
| **Typage** | Dynamique | Statique avec inférence |
| **Indentation** | Obligatoire | Optionnelle (préférée) |
| **Async** | `async/await` | Implicite (tout est async) |
| **Null** | `None` | `null` avec types optionnels |

## Concepts clés

### Streams
Un stream est une séquence potentiellement infinie d'événements typés.

### Events
Un événement est un enregistrement immuable avec un timestamp.

### Windows
Fenêtres temporelles pour borner les calculs sur les streams.

### Patterns
Règles de détection sur des séquences d'événements.

### Aggregations
Fonctions de réduction sur les fenêtres (sum, avg, count, etc.).

## Voir aussi

- [Syntaxe complète](syntax.md)
- [Système de types](types.md)
- [Mots-clés réservés](keywords.md)
- [Opérateurs](operators.md)
- [Fonctions built-in](builtins.md)
- [Grammaire formelle](grammar.md)
