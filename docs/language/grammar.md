# VarpulisQL Formal Grammar

## Notation

This grammar uses simplified EBNF notation:
- `|` : alternative
- `*` : zero or more
- `+` : one or more
- `?` : optional
- `()` : grouping
- `"..."` : literal terminal

## Lexemes

### Keywords

```
STREAM EVENT TYPE LET VAR CONST FN CONFIG
IF ELSE ELIF MATCH FOR WHILE BREAK CONTINUE RETURN
FROM WHERE SELECT JOIN MERGE WINDOW AGGREGATE PARTITION_BY ORDER_BY LIMIT DISTINCT EMIT TO
PATTERN ATTENTION_WINDOW ATTENTION_SCORE
TRUE FALSE NULL
AND OR NOT IN IS
TRY CATCH FINALLY RAISE
AS EXTENDS IMPORT EXPORT
INT FLOAT BOOL STR TIMESTAMP DURATION
```

### Operators

```
+ - * / % **
== != < <= > >=
& | ^ ~ << >>
= += -= *= /= %=
. ?. ?? =>  ->
.. ..=
```

### Literals

```
INTEGER     : [0-9]+
FLOAT       : [0-9]+ '.' [0-9]+ ([eE] [+-]? [0-9]+)?
STRING      : '"' [^"]* '"' | "'" [^']* "'"
DURATION    : [0-9]+ ('ns' | 'us' | 'ms' | 's' | 'm' | 'h' | 'd')
TIMESTAMP   : '@' ISO8601_DATE
IDENTIFIER  : [a-zA-Z_][a-zA-Z0-9_]*
```

## Productions

### Program

```ebnf
program         ::= statement*

statement       ::= stream_decl
                  | event_decl
                  | type_decl
                  | var_decl
                  | fn_decl
                  | config_block
                  | import_stmt
```

### Declarations

```ebnf
stream_decl     ::= 'stream' IDENTIFIER (':' type)? '=' stream_expr
                  | 'stream' IDENTIFIER 'from' IDENTIFIER

event_decl      ::= 'event' IDENTIFIER ('extends' IDENTIFIER)? ':' NEWLINE INDENT field_decl+ DEDENT

type_decl       ::= 'type' IDENTIFIER '=' type

var_decl        ::= ('let' | 'var' | 'const') IDENTIFIER (':' type)? '=' expr

fn_decl         ::= 'fn' IDENTIFIER '(' param_list? ')' ('->' type)? ':' block

field_decl      ::= IDENTIFIER ':' type NEWLINE

param_list      ::= param (',' param)*
param           ::= IDENTIFIER ':' type
```

### Types

```ebnf
type            ::= primitive_type
                  | array_type
                  | map_type
                  | tuple_type
                  | optional_type
                  | stream_type
                  | IDENTIFIER

primitive_type  ::= 'int' | 'float' | 'bool' | 'str' | 'timestamp' | 'duration'

array_type      ::= '[' type ']'

map_type        ::= '{' type ':' type '}'

tuple_type      ::= '(' type (',' type)+ ')'

optional_type   ::= type '?'

stream_type     ::= 'Stream' '<' type '>'
```

### Stream Expressions

```ebnf
stream_expr     ::= stream_source stream_op*

stream_source   ::= IDENTIFIER
                  | 'merge' '(' stream_list ')'
                  | 'join' '(' join_clause (',' join_clause)* ')'

stream_list     ::= stream_decl (',' stream_decl)*

join_clause     ::= stream_decl ('on' expr)?

stream_op       ::= '.where' '(' expr ')'
                  | '.select' '(' select_list ')'
                  | '.window' '(' window_args ')'
                  | '.aggregate' '(' agg_list ')'
                  | '.partition_by' '(' expr ')'
                  | '.order_by' '(' order_list ')'
                  | '.limit' '(' expr ')'
                  | '.distinct' '(' expr? ')'
                  | '.map' '(' lambda ')'
                  | '.filter' '(' lambda ')'
                  | '.tap' '(' tap_args ')'
                  | '.emit' '(' emit_args? ')'
                  | '.to' '(' sink_target ')'
                  | '.pattern' '(' pattern_def ')'
                  | '.attention_window' '(' attention_args ')'
                  | '.concurrent' '(' concurrent_args ')'
                  | '.process' '(' lambda ')'
                  | '.on_error' '(' lambda ')'
                  | '.collect' '(' ')'

window_args     ::= expr (',' named_arg)*
attention_args  ::= named_arg (',' named_arg)*
concurrent_args ::= named_arg (',' named_arg)*
tap_args        ::= named_arg (',' named_arg)*
emit_args       ::= named_arg (',' named_arg)*

select_list     ::= select_item (',' select_item)*
select_item     ::= IDENTIFIER
                  | IDENTIFIER ':' expr

agg_list        ::= agg_item (',' agg_item)*
agg_item        ::= IDENTIFIER ':' agg_call
                  | agg_call

agg_call        ::= IDENTIFIER '(' expr? ')'

order_list      ::= order_item (',' order_item)*
order_item      ::= expr ('asc' | 'desc')?
```

### Expressions

```ebnf
expr            ::= or_expr

or_expr         ::= and_expr ('or' and_expr)*

and_expr        ::= not_expr ('and' not_expr)*

not_expr        ::= 'not' not_expr
                  | comparison

comparison      ::= bitwise_or (comp_op bitwise_or)*

comp_op         ::= '==' | '!=' | '<' | '<=' | '>' | '>=' | 'in' | 'not' 'in' | 'is'

bitwise_or      ::= bitwise_xor ('|' bitwise_xor)*

bitwise_xor     ::= bitwise_and ('^' bitwise_and)*

bitwise_and     ::= shift ('&' shift)*

shift           ::= additive (('<<' | '>>') additive)*

additive        ::= multiplicative (('+' | '-') multiplicative)*

multiplicative  ::= power (('*' | '/' | '%') power)*

power           ::= unary ('**' power)?

unary           ::= ('-' | '~') unary
                  | postfix

postfix         ::= primary (postfix_op)*

postfix_op      ::= '.' IDENTIFIER
                  | '?.' IDENTIFIER
                  | '[' expr ']'
                  | '[' expr? ':' expr? ']'
                  | '(' arg_list? ')'

primary         ::= literal
                  | IDENTIFIER
                  | '(' expr ')'
                  | '[' expr_list? ']'
                  | '{' map_entries? '}'
                  | lambda
                  | if_expr
                  | match_expr

literal         ::= INTEGER | FLOAT | STRING | DURATION | TIMESTAMP
                  | 'true' | 'false' | 'null'

lambda          ::= IDENTIFIER '=>' expr
                  | '(' param_list? ')' '=>' expr
                  | '(' param_list? ')' '=>' block

if_expr         ::= 'if' expr 'then' expr 'else' expr

match_expr      ::= 'match' expr ':' NEWLINE INDENT match_arm+ DEDENT

match_arm       ::= pattern '=>' expr NEWLINE

pattern         ::= literal | IDENTIFIER | '_'

arg_list        ::= arg (',' arg)*
arg             ::= expr | named_arg

named_arg       ::= IDENTIFIER ':' expr

expr_list       ::= expr (',' expr)*

map_entries     ::= map_entry (',' map_entry)*
map_entry       ::= (STRING | IDENTIFIER) ':' expr
```

### Blocks and Control Flow

```ebnf
block           ::= NEWLINE INDENT statement+ DEDENT
                  | statement

statement       ::= var_decl
                  | if_stmt
                  | match_stmt
                  | for_stmt
                  | while_stmt
                  | return_stmt
                  | break_stmt
                  | continue_stmt
                  | expr_stmt

if_stmt         ::= 'if' expr ':' block ('elif' expr ':' block)* ('else' ':' block)?

for_stmt        ::= 'for' IDENTIFIER 'in' expr ':' block

while_stmt      ::= 'while' expr ':' block

return_stmt     ::= 'return' expr?

break_stmt      ::= 'break'

continue_stmt   ::= 'continue'

expr_stmt       ::= expr
```

### Configuration

```ebnf
config_block    ::= 'config' ':' NEWLINE INDENT config_item+ DEDENT

config_item     ::= IDENTIFIER ':' config_value NEWLINE
                  | IDENTIFIER ':' NEWLINE INDENT config_item+ DEDENT

config_value    ::= literal | IDENTIFIER | '[' config_list ']' | '{' config_map '}'

config_list     ::= config_value (',' config_value)*

config_map      ::= config_entry (',' config_entry)*

config_entry    ::= (STRING | IDENTIFIER) ':' config_value
```

## See Also

- [LALRPOP Implementation](../../grammar/varpulis.lalrpop)
