; VPL Syntax Highlighting for Tree-sitter

; Keywords
[
  "stream"
  "event"
  "type"
  "let"
  "var"
  "const"
  "fn"
  "config"
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
  "export"
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
  "enrich"
] @function.method

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
