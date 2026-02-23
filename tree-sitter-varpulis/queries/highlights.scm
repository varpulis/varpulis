; VPL Syntax Highlighting for Tree-sitter

; Keywords
[
  "stream"
  "event"
  "connector"
  "context"
  "type"
  "let"
  "var"
  "const"
  "fn"
  "if"
  "else"
  "elif"
  "then"
  "for"
  "while"
  "break"
  "continue"
  "return"
  "from"
  "where"
  "select"
  "join"
  "merge"
  "window"
  "aggregate"
  "partition_by"
  "order_by"
  "limit"
  "distinct"
  "emit"
  "to"
  "pattern"
  "and"
  "or"
  "not"
  "in"
  "is"
  "as"
  "extends"
  "import"
  "on"
  "all"
  "within"
  "sequence"
] @keyword

; Stream operation keywords
[
  "map"
  "filter"
  "tap"
  "print"
  "log"
  "process"
  "on_error"
  "collect"
  "concurrent"
  "fork"
  "any"
  "first"
  "sliding"
  "policy"
  "forecast"
  "trend_aggregate"
  "enrich"
] @function.method

; Connector types
(connector_type) @type.builtin

; Types
(primitive_type) @type.builtin
(named_type) @type
(stream_type "Stream" @type.builtin)

; Type keywords
[
  "int"
  "float"
  "bool"
  "str"
  "timestamp"
  "duration"
  "Stream"
] @type.builtin

; Literals
(integer) @number
(float) @number.float
(string) @string
(duration) @number
(timestamp) @string.special
(boolean) @constant.builtin
(null) @constant.builtin

; Identifiers
(identifier) @variable

; Function definitions
(function_declaration
  name: (identifier) @function)

; Function calls
(call_expression
  function: (identifier) @function.call)

; Parameters
(parameter
  name: (identifier) @variable.parameter)

; Fields
(field_declaration
  name: (identifier) @property)

; Named arguments
(named_argument
  name: (identifier) @property)

; Connector declarations
(connector_declaration
  name: (identifier) @variable)

; Connector params
(connector_param
  name: (identifier) @property)

; Context declarations
(context_declaration
  name: (identifier) @variable)

; Event declarations
(event_declaration
  name: (identifier) @type)

; Stream declarations
(stream_declaration
  name: (identifier) @variable)

; Variable declarations
(variable_declaration
  name: (identifier) @variable)

(constant_declaration
  name: (identifier) @constant)

; Member access
(member_expression
  member: (identifier) @property)

; Operators
[
  "+"
  "-"
  "*"
  "/"
  "%"
  "**"
  "=="
  "!="
  "<"
  "<="
  ">"
  ">="
  "&"
  "|"
  "^"
  "~"
  "<<"
  ">>"
  "="
  "->"
  "=>"
  "."
  "?."
  "??"
] @operator

; Punctuation
[
  "("
  ")"
  "["
  "]"
  "{"
  "}"
] @punctuation.bracket

[
  ","
  ":"
  "?"
] @punctuation.delimiter

; Comments
(comment) @comment
