# Fonctions built-in

## Fonctions d'agrégation

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `count()` | Nombre d'éléments | `count()` |
| `sum(expr)` | Somme | `sum(price)` |
| `avg(expr)` | Moyenne | `avg(temperature)` |
| `min(expr)` | Minimum | `min(latency)` |
| `max(expr)` | Maximum | `max(latency)` |
| `first(expr)` | Premier élément | `first(timestamp)` |
| `last(expr)` | Dernier élément | `last(value)` |
| `stddev(expr)` | Écart-type | `stddev(values)` |
| `variance(expr)` | Variance | `variance(values)` |
| `median(expr)` | Médiane | `median(prices)` |
| `percentile(expr, p)` | Percentile | `percentile(latency, 0.99)` |
| `collect()` | Liste de tous les éléments | `collect()` |
| `distinct(expr)` | Valeurs distinctes | `distinct(user_id)` |

## Fonctions de fenêtre

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `window(duration)` | Fenêtre tumbling | `window(5m)` |
| `window(duration, sliding)` | Fenêtre sliding | `window(5m, sliding: 1m)` |
| `session_window(gap)` | Fenêtre session | `session_window(gap: 30m)` |
| `row_number()` | Numéro de ligne | `row_number()` |
| `rank()` | Rang | `rank()` |
| `lag(expr, n)` | Valeur n positions avant | `lag(price, 1)` |
| `lead(expr, n)` | Valeur n positions après | `lead(price, 1)` |

## Fonctions de string

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `len(s)` | Longueur | `len(name)` |
| `upper(s)` | Majuscules | `upper(name)` |
| `lower(s)` | Minuscules | `lower(name)` |
| `trim(s)` | Supprime espaces | `trim(input)` |
| `split(s, sep)` | Découpe | `split(path, "/")` |
| `join(arr, sep)` | Joint | `join(parts, ",")` |
| `contains(s, sub)` | Contient | `contains(text, "error")` |
| `starts_with(s, pre)` | Commence par | `starts_with(url, "https")` |
| `ends_with(s, suf)` | Finit par | `ends_with(file, ".json")` |
| `replace(s, old, new)` | Remplace | `replace(text, "foo", "bar")` |
| `regex_match(s, pat)` | Regex match | `regex_match(log, r"\d+")` |
| `regex_extract(s, pat)` | Regex extract | `regex_extract(log, r"(\d+)")` |

## Fonctions mathématiques

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `abs(x)` | Valeur absolue | `abs(-5)` |
| `round(x, n)` | Arrondi | `round(3.14159, 2)` |
| `floor(x)` | Arrondi inférieur | `floor(3.9)` |
| `ceil(x)` | Arrondi supérieur | `ceil(3.1)` |
| `sqrt(x)` | Racine carrée | `sqrt(16)` |
| `pow(x, y)` | Puissance | `pow(2, 10)` |
| `log(x)` | Logarithme naturel | `log(10)` |
| `log10(x)` | Logarithme base 10 | `log10(100)` |
| `exp(x)` | Exponentielle | `exp(1)` |
| `sin(x)`, `cos(x)`, `tan(x)` | Trigonométrie | `sin(0.5)` |

## Fonctions de timestamp

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `now()` | Timestamp actuel | `now()` |
| `timestamp(s)` | Parse timestamp | `timestamp("2026-01-23")` |
| `year(ts)` | Année | `year(timestamp)` |
| `month(ts)` | Mois | `month(timestamp)` |
| `day(ts)` | Jour | `day(timestamp)` |
| `hour(ts)` | Heure | `hour(timestamp)` |
| `minute(ts)` | Minute | `minute(timestamp)` |
| `second(ts)` | Seconde | `second(timestamp)` |
| `day_of_week(ts)` | Jour de la semaine | `day_of_week(timestamp)` |
| `format_ts(ts, fmt)` | Formatage | `format_ts(ts, "%Y-%m-%d")` |
| `duration_between(t1, t2)` | Durée entre | `duration_between(start, end)` |

## Fonctions de collection

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `len(arr)` | Longueur | `len(items)` |
| `first(arr)` | Premier | `first(items)` |
| `last(arr)` | Dernier | `last(items)` |
| `head(arr, n)` | N premiers | `head(items, 5)` |
| `tail(arr, n)` | N derniers | `tail(items, 5)` |
| `reverse(arr)` | Inverse | `reverse(items)` |
| `sort(arr)` | Tri | `sort(items)` |
| `flatten(arr)` | Aplatit | `flatten(nested)` |
| `zip(a, b)` | Combine | `zip(keys, values)` |
| `enumerate(arr)` | Avec index | `enumerate(items)` |

## Fonctions d'attention

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `attention_score(e1, e2)` | Score de corrélation | `attention_score(evt1, evt2)` |
| `top_attention(e, n)` | Top N corrélés | `top_attention(event, 5)` |
| `cluster_by_attention(th)` | Clustering | `cluster_by_attention(0.8)` |

## Fonctions utilitaires

| Fonction | Description | Exemple |
|----------|-------------|---------|
| `coalesce(a, b, ...)` | Première non-null | `coalesce(x, y, 0)` |
| `if_null(val, default)` | Valeur par défaut | `if_null(name, "unknown")` |
| `typeof(x)` | Type de la valeur | `typeof(value)` |
| `hash(x)` | Hash de la valeur | `hash(user_id)` |
| `uuid()` | Génère un UUID | `uuid()` |
| `random()` | Nombre aléatoire [0,1) | `random()` |
| `random_int(min, max)` | Entier aléatoire | `random_int(1, 100)` |
