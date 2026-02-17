# ADR-001: Pest PEG Parser for VPL

**Status:** Accepted
**Date:** 2026-02-17
**Authors:** Varpulis Team

## Context

Varpulis requires a parser for its query language, VPL (Varpulis Pattern Language). VPL is a whitespace-sensitive, expression-rich language that supports:

- Stream declarations with operator chains (`.where()`, `.window()`, `.aggregate()`, `.emit()`, etc.)
- Sequence/pattern syntax using `->` (followed-by) with optional `all` keyword for Kleene closure
- Temporal operators: `.within(duration)`, `.forecast(...)`, `.trend_aggregate(...)`
- Control flow: `if`/`for`/`while` statements, `fn` declarations, `return`/`break`/`continue`
- Declaration-time metaprogramming: `for` loops at the top level with `{var}` interpolation
- Indentation-sensitive structure requiring a pre-processing step to insert INDENT/DEDENT tokens

The parser must produce useful error messages (line, column, expected tokens), be maintainable as the language evolves, and have no ambiguity in its grammar rules.

Two classes of parser technology were evaluated: parser combinator libraries and parser generator libraries.

## Decision

VPL is parsed using **Pest** (`pest` crate, `pest_derive` for code generation), a PEG (Parsing Expression Grammar) parser generator.

The grammar lives in `crates/varpulis-parser/src/varpulis.pest`. A preprocessor (`indent.rs`) converts indentation into explicit INDENT/DEDENT marker tokens before the grammar runs, which allows Pest's purely top-down, no-backtracking PEG model to handle whitespace sensitivity without dedicated grammar machinery.

The derive macro `#[derive(Parser)] #[grammar = "varpulis.pest"]` generates the `VarpulisParser` struct at compile time. The hand-written `pest_parser.rs` walks the resulting parse tree to produce the `Program` AST defined in `varpulis-core`.

An optimizer pass (`optimize.rs`) folds constant expressions after parsing, and an expander (`expand.rs`) handles declaration loops before the grammar runs.

### Why PEG specifically

PEG grammars use ordered choice (`/`) rather than union (`|`). This makes ambiguity impossible by definition: the first matching alternative always wins. VPL has several constructs where ordered choice is essential:

- `stream_source` must try `from_connector_source`, `merge_source`, etc. before falling back to a bare `identifier`, because all of them begin with an identifier.
- `filter_expr` (used inside `->` followed-by chains) must terminate before keywords like `.emit`, `.where` are consumed as part of the predicate. Pest's negative lookahead (`!`) makes this precise: `("." ~ identifier ~ !("("))` matches `.field` but not `.method()`.
- The `dot_op` rule lists every possible stream operation in order; the ordered choice guarantees that new operators can be added without introducing ambiguity.

## Alternatives Considered

### nom (parser combinator)

`nom` is a widely-used zero-copy parser combinator library in Rust. It would give full control over the parsing logic and avoid a separate grammar file.

Rejected because:
- Combinators duplicate knowledge across code and tests; the grammar is implicit in Rust function names.
- Error messages from `nom` parsers require significant effort to produce good line/column diagnostics. Pest provides these for free via `pest::error::Error<Rule>`, including "Expected one of: ..." messages derived directly from the grammar.
- A `.pest` file is a self-contained, reviewable specification of the language syntax. A `nom` implementation is opaque to contributors not fluent in combinator idioms.
- Extending the grammar (adding `forecast_op`, `trend_aggregate_op`, `enrich_op`) is one line in the `dot_op` rule in Pest vs. adding and wiring new combinator functions in `nom`.

### lalrpop (LALR parser generator)

`lalrpop` generates bottom-up LALR(1) parsers from a BNF-like grammar. It handles left-recursive grammars natively, whereas PEG requires left-recursion elimination.

Rejected because:
- LALR parsers produce shift/reduce and reduce/reduce conflicts when the grammar is ambiguous in ways that are difficult to diagnose. VPL has several ambiguous-looking constructs (expression precedence, the `as` keyword used in both aliases and type annotations) that are handled naturally by PEG's ordered choice but would require careful precedence declarations in LALR.
- The indentation-sensitive preprocessor approach used by Varpulis is simpler to reason about with a top-down PEG parser that sees INDENT/DEDENT tokens in the stream.
- `lalrpop` grammar actions are written inline in the grammar file, mixing Rust code and BNF syntax. Pest keeps grammar purely declarative and the AST construction in separate Rust code, which is cleaner for a language this size.

### tree-sitter

`tree-sitter` is an incremental parsing framework designed primarily for editors. It supports error recovery and incremental re-parsing.

Rejected for the runtime parser because:
- tree-sitter is designed for editors; its Rust API surface is not idiomatic for embedding in a CLI or server binary.
- Error recovery produces partial trees on invalid input, but the runtime parser needs to reject invalid VPL with a clear error, not produce a partial AST.
- The grammar definition is in JavaScript (for the parser generator) while the parsing API is C. This is an impedance mismatch for a pure-Rust project.

Note: Varpulis does use tree-sitter for the VS Code extension's syntax highlighting (`web-ui/`), where incremental parsing and error recovery are precisely what is needed. This is a complementary use, not a replacement for the Pest runtime parser.

## Consequences

### Positive

- Grammar is the single source of truth for VPL syntax. Contributors can read `varpulis.pest` to understand the language without reading parser code.
- Pest generates precise error messages with line/column information and a list of expected tokens at the failure point. The `convert_pest_error` function in `pest_parser.rs` translates these into structured `ParseError::Located` values with human-readable hints.
- Adding new VPL operators requires only: (a) adding a rule to `varpulis.pest`, (b) referencing it in `dot_op`, and (c) implementing the AST constructor in `pest_parser.rs`. The grammar and parsing separation makes each step independent.
- Compile-time grammar validation: the `#[grammar = "..."]` macro fails the build if the grammar file contains syntax errors, catching mistakes before they reach CI.
- The indentation preprocessor is isolated in `indent.rs` and completely independent of the grammar; the grammar sees only a flat token stream.

### Negative

- Pest PEG parsers do not support left-recursive rules directly. VPL expression grammar (`expr`, `or_expr`, `and_expr`, ...) must be written using right-recursive rules with explicit precedence levels. This adds boilerplate but is standard practice for PEG expression grammars.
- The two-phase approach (preprocessor then Pest) means parse errors from the preprocessor do not carry Pest rule names and use a simpler error format.
- Pest parse trees must be walked manually to construct the AST; there is no equivalent to `lalrpop`'s inline grammar actions. The `pest_parser.rs` file is correspondingly verbose (several thousand lines), though each `parse_*` function is straightforward.
- Pest does not support incremental or error-recovering parsing, so a single syntax error in a large VPL file aborts parsing entirely. This is acceptable for the runtime compiler but less ideal for language server use; the LSP uses a best-effort fallback when Pest fails.

## References

- [Pest documentation](https://pest.rs)
- [Parsing Expression Grammars: A Recognition-Based Syntactic Foundation](https://dl.acm.org/doi/10.1145/982962.964011) — Ford (POPL 2004), foundational PEG paper
- `crates/varpulis-parser/src/varpulis.pest` — VPL grammar
- `crates/varpulis-parser/src/pest_parser.rs` — AST construction from parse tree
- `crates/varpulis-parser/src/indent.rs` — indentation preprocessor
- `crates/varpulis-lsp/` — LSP server with fallback validation when full parse fails
