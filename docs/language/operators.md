# Opérateurs

## Opérateurs arithmétiques

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `+` | Addition | `a + b` |
| `-` | Soustraction | `a - b` |
| `*` | Multiplication | `a * b` |
| `/` | Division | `a / b` |
| `%` | Modulo | `a % b` |
| `**` | Puissance | `a ** b` |
| `-` (unaire) | Négation | `-a` |

## Opérateurs de comparaison

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `==` | Égalité | `a == b` |
| `!=` | Inégalité | `a != b` |
| `<` | Inférieur | `a < b` |
| `<=` | Inférieur ou égal | `a <= b` |
| `>` | Supérieur | `a > b` |
| `>=` | Supérieur ou égal | `a >= b` |

## Opérateurs logiques

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `and` | ET logique | `a and b` |
| `or` | OU logique | `a or b` |
| `not` | NON logique | `not a` |

## Opérateurs bit à bit

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `&` | AND bit à bit | `a & b` |
| `\|` | OR bit à bit | `a \| b` |
| `^` | XOR bit à bit | `a ^ b` |
| `~` | NOT bit à bit | `~a` |
| `<<` | Décalage gauche | `a << 2` |
| `>>` | Décalage droite | `a >> 2` |

## Opérateurs d'affectation

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `=` | Affectation | `x = 5` |
| `+=` | Addition et affectation | `x += 1` |
| `-=` | Soustraction et affectation | `x -= 1` |
| `*=` | Multiplication et affectation | `x *= 2` |
| `/=` | Division et affectation | `x /= 2` |
| `%=` | Modulo et affectation | `x %= 3` |

## Opérateurs de chaînage

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `.` | Accès membre / chaînage | `stream.where(...).map(...)` |
| `?.` | Accès optionnel | `user?.name` |
| `??` | Coalescence null | `value ?? default` |

## Opérateurs de collection

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `[]` | Indexation | `array[0]` |
| `[:]` | Slice | `array[1:3]` |
| `in` | Appartenance | `x in list` |
| `not in` | Non-appartenance | `x not in list` |

## Opérateurs de range

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `..` | Range exclusif | `0..10` (0 à 9) |
| `..=` | Range inclusif | `0..=10` (0 à 10) |

## Opérateurs lambda

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `=>` | Lambda expression | `x => x * 2` |
| `->` | Type de retour | `fn add(a: int, b: int) -> int` |

## Opérateurs de durée

```varpulis
# Suffixes de durée
5s      # 5 secondes
10m     # 10 minutes  
2h      # 2 heures
1d      # 1 jour
500ms   # 500 millisecondes
100us   # 100 microsecondes
50ns    # 50 nanosecondes
```

## Priorité des opérateurs (du plus élevé au plus bas)

1. `()`, `[]`, `.`, `?.`
2. `**`
3. `-` (unaire), `not`, `~`
4. `*`, `/`, `%`
5. `+`, `-`
6. `<<`, `>>`
7. `&`
8. `^`
9. `|`
10. `<`, `<=`, `>`, `>=`, `==`, `!=`, `in`, `not in`
11. `and`
12. `or`
13. `??`
14. `=`, `+=`, `-=`, etc.
