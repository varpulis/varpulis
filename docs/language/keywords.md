# Mots-clés réservés

## Déclarations

| Mot-clé | Description |
|---------|-------------|
| `stream` | Déclare un stream |
| `event` | Déclare un type d'événement |
| `type` | Déclare un alias de type |
| `let` | Déclare une variable immutable |
| `var` | Déclare une variable mutable |
| `const` | Déclare une constante globale |
| `fn` | Déclare une fonction |
| `config` | Bloc de configuration |

## Contrôle de flux

| Mot-clé | Description |
|---------|-------------|
| `if` | Condition |
| `else` | Branche alternative |
| `elif` | Condition alternative (else if) |
| `match` | Pattern matching |
| `for` | Boucle sur itérable |
| `while` | Boucle conditionnelle |
| `break` | Sort de la boucle |
| `continue` | Passe à l'itération suivante |
| `return` | Retourne une valeur |

## Opérateurs de stream

| Mot-clé | Description |
|---------|-------------|
| `from` | Source d'un stream |
| `where` | Filtrage |
| `select` | Projection |
| `join` | Jointure de streams |
| `merge` | Fusion de streams |
| `window` | Fenêtre temporelle |
| `aggregate` | Agrégation |
| `partition_by` | Partitionnement |
| `order_by` | Tri |
| `limit` | Limitation |
| `distinct` | Dédoublonnage |
| `emit` | Émission vers sink |

## Patterns et attention

| Mot-clé | Description |
|---------|-------------|
| `pattern` | Définit un pattern de détection |
| `attention_window` | Fenêtre avec attention mechanism |
| `attention_score` | Score de corrélation entre événements |

## Types et valeurs

| Mot-clé | Description |
|---------|-------------|
| `true` | Booléen vrai |
| `false` | Booléen faux |
| `null` | Absence de valeur |
| `int` | Type entier |
| `float` | Type flottant |
| `bool` | Type booléen |
| `str` | Type chaîne |
| `timestamp` | Type timestamp |
| `duration` | Type durée |

## Opérateurs logiques

| Mot-clé | Description |
|---------|-------------|
| `and` | ET logique |
| `or` | OU logique |
| `not` | NON logique |
| `in` | Appartenance |
| `is` | Test de type/identité |

## Gestion d'erreurs

| Mot-clé | Description |
|---------|-------------|
| `try` | Bloc try |
| `catch` | Capture d'exception |
| `finally` | Bloc final |
| `raise` | Lève une exception |

## Autres

| Mot-clé | Description |
|---------|-------------|
| `as` | Alias/cast |
| `extends` | Héritage d'événement |
| `import` | Import de module |
| `export` | Export de symbole |

## Mots-clés réservés pour usage futur

Les mots suivants sont réservés pour de potentielles extensions :

- `async`, `await`
- `class`, `trait`, `impl`
- `pub`, `priv`
- `mod`, `use`
- `yield`
- `defer`
- `go`, `spawn`
