# Operators

## Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `a + b` |
| `-` | Subtraction | `a - b` |
| `*` | Multiplication | `a * b` |
| `/` | Division | `a / b` |
| `%` | Modulo | `a % b` |
| `**` | Power | `a ** b` |
| `-` (unary) | Negation | `-a` |

## Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equality | `a == b` |
| `!=` | Inequality | `a != b` |
| `<` | Less than | `a < b` |
| `<=` | Less than or equal | `a <= b` |
| `>` | Greater than | `a > b` |
| `>=` | Greater than or equal | `a >= b` |

## Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `and` | Logical AND | `a and b` |
| `or` | Logical OR | `a or b` |
| `not` | Logical NOT | `not a` |

## Bitwise Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `&` | Bitwise AND | `a & b` |
| `\|` | Bitwise OR | `a \| b` |
| `^` | Bitwise XOR | `a ^ b` |
| `~` | Bitwise NOT | `~a` |
| `<<` | Left shift | `a << 2` |
| `>>` | Right shift | `a >> 2` |

## Assignment Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `:=` | Assignment | `x := 5` |

## Chaining Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `.` | Member access / chaining | `stream.where(...).select(...)` |
| `?.` | Optional access | `user?.name` |
| `??` | Null coalesce | `value ?? default` |

## Collection Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `[]` | Indexing | `array[0]` |
| `[:]` | Slice | `array[1:3]` |
| `in` | Membership | `x in list` |
| `not in` | Non-membership | `x not in list` |

## Range Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `..` | Exclusive range | `0..10` (0 to 9) |
| `..=` | Inclusive range | `0..=10` (0 to 10) |

## Lambda Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=>` | Lambda expression | `x => x * 2` |
| `->` | Return type | `fn add(a: int, b: int) -> int` |

## Duration Operators

```varpulis
# Duration suffixes
5s      # 5 seconds
10m     # 10 minutes
2h      # 2 hours
1d      # 1 day
500ms   # 500 milliseconds
100us   # 100 microseconds
50ns    # 50 nanoseconds
```

## Operator Precedence (highest to lowest)

1. `()`, `[]`, `.`, `?.`
2. `**`
3. `-` (unary), `not`, `~`
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
14. `:=`
