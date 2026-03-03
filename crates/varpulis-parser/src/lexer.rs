//! Lexer for VPL using Logos

use std::fmt;

use logos::Logos;

/// Token type for VPL
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r\n\f]+")]
#[logos(skip(r"#[^\n]*", allow_greedy = true))]
#[logos(skip r"/\*([^*]|\*[^/])*\*/")]
pub enum Token {
    // === Keywords ===
    /// `stream` keyword.
    #[token("stream")]
    Stream,
    /// `event` keyword.
    #[token("event")]
    Event,
    /// `type` keyword.
    #[token("type")]
    Type,
    /// `let` keyword.
    #[token("let")]
    Let,
    /// `var` keyword.
    #[token("var")]
    Var,
    /// `const` keyword.
    #[token("const")]
    Const,
    /// `fn` keyword.
    #[token("fn")]
    Fn,
    /// `config` keyword.
    #[token("config")]
    Config,

    /// `if` keyword.
    #[token("if")]
    If,
    /// `else` keyword.
    #[token("else")]
    Else,
    /// `elif` keyword.
    #[token("elif")]
    Elif,
    /// `then` keyword.
    #[token("then")]
    Then,
    /// `match` keyword.
    #[token("match")]
    Match,
    /// `for` keyword.
    #[token("for")]
    For,
    /// `while` keyword.
    #[token("while")]
    While,
    /// `break` keyword.
    #[token("break")]
    Break,
    /// `continue` keyword.
    #[token("continue")]
    Continue,
    /// `return` keyword.
    #[token("return")]
    Return,

    /// `from` keyword.
    #[token("from")]
    From,
    /// `where` keyword.
    #[token("where")]
    Where,
    /// `select` keyword.
    #[token("select")]
    Select,
    /// `join` keyword.
    #[token("join")]
    Join,
    /// `merge` keyword.
    #[token("merge")]
    Merge,
    /// `window` keyword.
    #[token("window")]
    Window,
    /// `aggregate` keyword.
    #[token("aggregate")]
    Aggregate,
    /// `partition_by` keyword.
    #[token("partition_by")]
    PartitionBy,
    /// `order_by` keyword.
    #[token("order_by")]
    OrderBy,
    /// `limit` keyword.
    #[token("limit")]
    Limit,
    /// `distinct` keyword.
    #[token("distinct")]
    Distinct,
    /// `emit` keyword.
    #[token("emit")]
    Emit,
    /// `to` keyword.
    #[token("to")]
    To,
    /// `on` keyword.
    #[token("on")]
    On,
    /// `all` keyword.
    #[token("all")]
    All,
    /// `within` keyword.
    #[token("within")]
    Within,

    /// `pattern` keyword.
    #[token("pattern")]
    Pattern,
    // Note: Stream operation names (map, filter, etc.) are NOT keywords
    // They are parsed contextually after '.' and can be used as identifiers
    /// Boolean literal `true`.
    #[token("true")]
    True,
    /// Boolean literal `false`.
    #[token("false")]
    False,
    /// Null literal.
    #[token("null")]
    Null,

    /// Logical `and` operator.
    #[token("and")]
    And,
    /// Logical `or` operator.
    #[token("or")]
    Or,
    /// Logical `xor` operator.
    #[token("xor")]
    Xor,
    /// Logical `not` operator.
    #[token("not")]
    Not,
    /// `in` keyword (membership test / for loops).
    #[token("in")]
    In,
    /// `is` keyword (type check).
    #[token("is")]
    Is,

    /// `as` keyword (alias / cast).
    #[token("as")]
    As,
    /// `extends` keyword (event inheritance).
    #[token("extends")]
    Extends,
    /// `import` keyword.
    #[token("import")]
    Import,
    /// `export` keyword.
    #[token("export")]
    Export,

    // Type keywords
    /// `int` type keyword.
    #[token("int")]
    IntType,
    /// `float` type keyword.
    #[token("float")]
    FloatType,
    /// `bool` type keyword.
    #[token("bool")]
    BoolType,
    /// `str` type keyword.
    #[token("str")]
    StrType,
    /// `timestamp` type keyword.
    #[token("timestamp")]
    TimestampType,
    /// `duration` type keyword.
    #[token("duration")]
    DurationType,
    /// `Stream` type keyword.
    #[token("Stream")]
    StreamType,

    // === Operators ===
    /// `+` operator.
    #[token("+")]
    Plus,
    /// `-` operator.
    #[token("-")]
    Minus,
    /// `*` operator.
    #[token("*")]
    Star,
    /// `/` operator.
    #[token("/")]
    Slash,
    /// `%` operator (modulo).
    #[token("%")]
    Percent,
    /// `**` operator (exponentiation).
    #[token("**")]
    DoubleStar,

    /// `==` equality comparison.
    #[token("==")]
    EqEq,
    /// `!=` inequality comparison.
    #[token("!=")]
    NotEq,
    /// `<` less-than comparison.
    #[token("<")]
    Lt,
    /// `<=` less-than-or-equal comparison.
    #[token("<=")]
    Le,
    /// `>` greater-than comparison.
    #[token(">")]
    Gt,
    /// `>=` greater-than-or-equal comparison.
    #[token(">=")]
    Ge,

    /// `&` bitwise AND.
    #[token("&")]
    Amp,
    /// `|` bitwise OR.
    #[token("|")]
    Pipe,
    /// `^` bitwise XOR.
    #[token("^")]
    Caret,
    /// `~` bitwise NOT.
    #[token("~")]
    Tilde,
    /// `<<` left shift.
    #[token("<<")]
    Shl,
    /// `>>` right shift.
    #[token(">>")]
    Shr,

    /// `=` assignment.
    #[token("=")]
    Eq,
    /// `+=` add-assign.
    #[token("+=")]
    PlusEq,
    /// `-=` subtract-assign.
    #[token("-=")]
    MinusEq,
    /// `*=` multiply-assign.
    #[token("*=")]
    StarEq,
    /// `/=` divide-assign.
    #[token("/=")]
    SlashEq,
    /// `%=` modulo-assign.
    #[token("%=")]
    PercentEq,

    /// `.` member access.
    #[token(".")]
    Dot,
    /// `?.` optional chaining.
    #[token("?.")]
    QuestionDot,
    /// `??` null coalescing.
    #[token("??")]
    QuestionQuestion,
    /// `=>` fat arrow (lambdas / match arms).
    #[token("=>")]
    FatArrow,
    /// `->` thin arrow (return type annotation).
    #[token("->")]
    Arrow,
    /// `..` exclusive range.
    #[token("..")]
    DotDot,
    /// `..=` inclusive range.
    #[token("..=")]
    DotDotEq,
    /// `$` dollar sign (special variable prefix).
    #[token("$")]
    Dollar,

    // === Delimiters ===
    /// `(` left parenthesis.
    #[token("(")]
    LParen,
    /// `)` right parenthesis.
    #[token(")")]
    RParen,
    /// `[` left bracket.
    #[token("[")]
    LBracket,
    /// `]` right bracket.
    #[token("]")]
    RBracket,
    /// `{` left brace.
    #[token("{")]
    LBrace,
    /// `}` right brace.
    #[token("}")]
    RBrace,
    /// `,` comma separator.
    #[token(",")]
    Comma,
    /// `:` colon (type annotations, block starts).
    #[token(":")]
    Colon,
    /// `?` question mark (ternary / optional).
    #[token("?")]
    Question,
    /// `@` at sign (timestamp literal prefix / decorator).
    #[token("@")]
    At,

    // === Literals ===
    /// Integer literal (e.g., `42`).
    #[regex(r"[0-9]+", |lex| lex.slice().parse::<i64>().ok())]
    Integer(i64),

    /// Floating-point literal (e.g., `3.14`, `1.0e10`).
    #[regex(r"[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?", |lex| lex.slice().parse::<f64>().ok())]
    Float(f64),

    /// String literal (double- or single-quoted).
    #[regex(r#""([^"\\]|\\.)*""#, |lex| {
        let s = lex.slice();
        Some(s[1..s.len()-1].to_string())
    })]
    #[regex(r#"'([^'\\]|\\.)*'"#, |lex| {
        let s = lex.slice();
        Some(s[1..s.len()-1].to_string())
    })]
    String(String),

    /// Duration literal (e.g., `5s`, `100ms`, `2h`).
    #[regex(r"[0-9]+(ns|us|ms|s|m|h|d)", |lex| Some(lex.slice().to_string()))]
    Duration(String),

    /// Timestamp literal (e.g., `@2024-01-15T10:30:00Z`).
    #[regex(r"@[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})?)?", |lex| Some(lex.slice().to_string()))]
    Timestamp(String),

    // === Identifier ===
    /// Identifier (e.g., variable name, event type).
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| Some(lex.slice().to_string()))]
    Ident(String),

    // === Special ===
    /// End-of-file sentinel token.
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream => write!(f, "stream"),
            Self::Event => write!(f, "event"),
            Self::Type => write!(f, "type"),
            Self::Let => write!(f, "let"),
            Self::Var => write!(f, "var"),
            Self::Const => write!(f, "const"),
            Self::Fn => write!(f, "fn"),
            Self::Config => write!(f, "config"),
            Self::If => write!(f, "if"),
            Self::Else => write!(f, "else"),
            Self::Elif => write!(f, "elif"),
            Self::Then => write!(f, "then"),
            Self::Match => write!(f, "match"),
            Self::For => write!(f, "for"),
            Self::While => write!(f, "while"),
            Self::Break => write!(f, "break"),
            Self::Continue => write!(f, "continue"),
            Self::Return => write!(f, "return"),
            Self::From => write!(f, "from"),
            Self::Where => write!(f, "where"),
            Self::Select => write!(f, "select"),
            Self::Join => write!(f, "join"),
            Self::Merge => write!(f, "merge"),
            Self::Window => write!(f, "window"),
            Self::Aggregate => write!(f, "aggregate"),
            Self::PartitionBy => write!(f, "partition_by"),
            Self::OrderBy => write!(f, "order_by"),
            Self::Limit => write!(f, "limit"),
            Self::Distinct => write!(f, "distinct"),
            Self::Emit => write!(f, "emit"),
            Self::To => write!(f, "to"),
            Self::On => write!(f, "on"),
            Self::All => write!(f, "all"),
            Self::Within => write!(f, "within"),
            Self::Pattern => write!(f, "pattern"),
            Self::True => write!(f, "true"),
            Self::False => write!(f, "false"),
            Self::Null => write!(f, "null"),
            Self::And => write!(f, "and"),
            Self::Or => write!(f, "or"),
            Self::Xor => write!(f, "xor"),
            Self::Not => write!(f, "not"),
            Self::In => write!(f, "in"),
            Self::Is => write!(f, "is"),
            Self::As => write!(f, "as"),
            Self::Extends => write!(f, "extends"),
            Self::Import => write!(f, "import"),
            Self::Export => write!(f, "export"),
            Self::IntType => write!(f, "int"),
            Self::FloatType => write!(f, "float"),
            Self::BoolType => write!(f, "bool"),
            Self::StrType => write!(f, "str"),
            Self::TimestampType => write!(f, "timestamp"),
            Self::DurationType => write!(f, "duration"),
            Self::StreamType => write!(f, "Stream"),
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Star => write!(f, "*"),
            Self::Slash => write!(f, "/"),
            Self::Percent => write!(f, "%"),
            Self::DoubleStar => write!(f, "**"),
            Self::EqEq => write!(f, "=="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::Le => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Ge => write!(f, ">="),
            Self::Amp => write!(f, "&"),
            Self::Pipe => write!(f, "|"),
            Self::Caret => write!(f, "^"),
            Self::Tilde => write!(f, "~"),
            Self::Shl => write!(f, "<<"),
            Self::Shr => write!(f, ">>"),
            Self::Eq => write!(f, "="),
            Self::PlusEq => write!(f, "+="),
            Self::MinusEq => write!(f, "-="),
            Self::StarEq => write!(f, "*="),
            Self::SlashEq => write!(f, "/="),
            Self::PercentEq => write!(f, "%="),
            Self::Dot => write!(f, "."),
            Self::QuestionDot => write!(f, "?."),
            Self::QuestionQuestion => write!(f, "??"),
            Self::FatArrow => write!(f, "=>"),
            Self::Arrow => write!(f, "->"),
            Self::DotDot => write!(f, ".."),
            Self::DotDotEq => write!(f, "..="),
            Self::Dollar => write!(f, "$"),
            Self::LParen => write!(f, "("),
            Self::RParen => write!(f, ")"),
            Self::LBracket => write!(f, "["),
            Self::RBracket => write!(f, "]"),
            Self::LBrace => write!(f, "{{"),
            Self::RBrace => write!(f, "}}"),
            Self::Comma => write!(f, ","),
            Self::Colon => write!(f, ":"),
            Self::Question => write!(f, "?"),
            Self::At => write!(f, "@"),
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Duration(d) => write!(f, "{d}"),
            Self::Timestamp(t) => write!(f, "{t}"),
            Self::Ident(s) => write!(f, "{s}"),
            Self::Eof => write!(f, "EOF"),
        }
    }
}

/// Spanned token with position information
#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    /// The token value.
    pub token: Token,
    /// Byte offset of the first character of this token.
    pub start: usize,
    /// Byte offset past the last character of this token.
    pub end: usize,
}

/// Lexer wrapper that produces spanned tokens
pub struct Lexer<'source> {
    inner: logos::Lexer<'source, Token>,
    peeked: Option<SpannedToken>,
    eof_emitted: bool,
}

impl std::fmt::Debug for Lexer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lexer").finish_non_exhaustive()
    }
}

impl<'source> Lexer<'source> {
    /// Create a new lexer over the given source string.
    pub fn new(source: &'source str) -> Self {
        Self {
            inner: Token::lexer(source),
            peeked: None,
            eof_emitted: false,
        }
    }

    /// Peek at the next token without consuming it.
    pub fn peek(&mut self) -> Option<&SpannedToken> {
        if self.peeked.is_none() {
            self.peeked = self.next_token();
        }
        self.peeked.as_ref()
    }

    fn next_token(&mut self) -> Option<SpannedToken> {
        match self.inner.next() {
            Some(Ok(token)) => {
                let span = self.inner.span();
                Some(SpannedToken {
                    token,
                    start: span.start,
                    end: span.end,
                })
            }
            Some(Err(())) => {
                let span = self.inner.span();
                Some(SpannedToken {
                    token: Token::Ident(self.inner.slice().to_string()),
                    start: span.start,
                    end: span.end,
                })
            }
            None if !self.eof_emitted => {
                self.eof_emitted = true;
                let pos = self.inner.span().end;
                Some(SpannedToken {
                    token: Token::Eof,
                    start: pos,
                    end: pos,
                })
            }
            None => None,
        }
    }
}

impl Iterator for Lexer<'_> {
    type Item = SpannedToken;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(peeked) = self.peeked.take() {
            return Some(peeked);
        }
        self.next_token()
    }
}

/// Tokenize a source string into a vector of spanned tokens
pub fn tokenize(source: &str) -> Vec<SpannedToken> {
    Lexer::new(source).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords() {
        let tokens: Vec<_> = tokenize("stream event let var const fn")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Stream,
                Token::Event,
                Token::Let,
                Token::Var,
                Token::Const,
                Token::Fn,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_literals() {
        let tokens: Vec<_> = tokenize("42 2.5 \"hello\" 5s true null")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Integer(42),
                Token::Float(2.5),
                Token::String("hello".to_string()),
                Token::Duration("5s".to_string()),
                Token::True,
                Token::Null,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_operators() {
        let tokens: Vec<_> = tokenize("+ - * / == != <= >=")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::EqEq,
                Token::NotEq,
                Token::Le,
                Token::Ge,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_stream_decl() {
        let tokens: Vec<_> = tokenize("stream Trades = TradeEvent")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Stream,
                Token::Ident("Trades".to_string()),
                Token::Eq,
                Token::Ident("TradeEvent".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_comments() {
        let tokens: Vec<_> = tokenize("# comment\nstream /* inline */ Trades")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Stream,
                Token::Ident("Trades".to_string()),
                Token::Eof,
            ]
        );
    }

    // ==========================================================================
    // Additional Coverage Tests
    // ==========================================================================

    #[test]
    fn test_more_keywords() {
        let tokens: Vec<_> = tokenize("if else elif then match for while break continue return")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(tokens.contains(&Token::If));
        assert!(tokens.contains(&Token::Else));
        assert!(tokens.contains(&Token::Match));
        assert!(tokens.contains(&Token::For));
        assert!(tokens.contains(&Token::While));
        assert!(tokens.contains(&Token::Break));
        assert!(tokens.contains(&Token::Return));
    }

    #[test]
    fn test_stream_keywords() {
        let tokens: Vec<_> = tokenize("where select join merge window aggregate emit")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(tokens.contains(&Token::Where));
        assert!(tokens.contains(&Token::Select));
        assert!(tokens.contains(&Token::Join));
        assert!(tokens.contains(&Token::Merge));
        assert!(tokens.contains(&Token::Window));
        assert!(tokens.contains(&Token::Aggregate));
        assert!(tokens.contains(&Token::Emit));
    }

    #[test]
    fn test_more_operators() {
        let tokens: Vec<_> = tokenize("% ** < > & | ^ ~ << >> = += -= *= /= %=")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(tokens.contains(&Token::Percent));
        assert!(tokens.contains(&Token::DoubleStar));
        assert!(tokens.contains(&Token::Lt));
        assert!(tokens.contains(&Token::Gt));
        assert!(tokens.contains(&Token::Amp));
        assert!(tokens.contains(&Token::Pipe));
        assert!(tokens.contains(&Token::Caret));
        assert!(tokens.contains(&Token::Tilde));
        assert!(tokens.contains(&Token::Eq));
        assert!(tokens.contains(&Token::PlusEq));
    }

    #[test]
    fn test_delimiters() {
        let tokens: Vec<_> = tokenize("( ) [ ] { } , : ? @")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(tokens.contains(&Token::LParen));
        assert!(tokens.contains(&Token::RParen));
        assert!(tokens.contains(&Token::LBracket));
        assert!(tokens.contains(&Token::RBracket));
        assert!(tokens.contains(&Token::LBrace));
        assert!(tokens.contains(&Token::RBrace));
        assert!(tokens.contains(&Token::Comma));
        assert!(tokens.contains(&Token::Colon));
        assert!(tokens.contains(&Token::Question));
        assert!(tokens.contains(&Token::At));
    }

    #[test]
    fn test_special_operators() {
        let tokens: Vec<_> = tokenize(". ?. ?? => -> .. ..= $")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(tokens.contains(&Token::Dot));
        assert!(tokens.contains(&Token::QuestionDot));
        assert!(tokens.contains(&Token::QuestionQuestion));
        assert!(tokens.contains(&Token::FatArrow));
        assert!(tokens.contains(&Token::Arrow));
        assert!(tokens.contains(&Token::DotDot));
        assert!(tokens.contains(&Token::DotDotEq));
        assert!(tokens.contains(&Token::Dollar));
    }

    #[test]
    fn test_type_keywords() {
        let tokens: Vec<_> = tokenize("int float bool str timestamp duration Stream")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(tokens.contains(&Token::IntType));
        assert!(tokens.contains(&Token::FloatType));
        assert!(tokens.contains(&Token::BoolType));
        assert!(tokens.contains(&Token::StrType));
        assert!(tokens.contains(&Token::TimestampType));
        assert!(tokens.contains(&Token::DurationType));
        assert!(tokens.contains(&Token::StreamType));
    }

    #[test]
    fn test_logical_keywords() {
        let tokens: Vec<_> = tokenize("and or not in is as")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(tokens.contains(&Token::And));
        assert!(tokens.contains(&Token::Or));
        assert!(tokens.contains(&Token::Not));
        assert!(tokens.contains(&Token::In));
        assert!(tokens.contains(&Token::Is));
        assert!(tokens.contains(&Token::As));
    }

    #[test]
    fn test_duration_variants() {
        let tokens: Vec<_> = tokenize("100ms 5s 10m 2h 1d")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(matches!(&tokens[0], Token::Duration(s) if s == "100ms"));
        assert!(matches!(&tokens[1], Token::Duration(s) if s == "5s"));
        assert!(matches!(&tokens[2], Token::Duration(s) if s == "10m"));
        assert!(matches!(&tokens[3], Token::Duration(s) if s == "2h"));
        assert!(matches!(&tokens[4], Token::Duration(s) if s == "1d"));
    }

    #[test]
    fn test_string_escapes() {
        let tokens: Vec<_> = tokenize(r#""hello\nworld" "tab\there""#)
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert!(matches!(&tokens[0], Token::String(_)));
        assert!(matches!(&tokens[1], Token::String(_)));
    }

    #[test]
    fn test_lexer_peek() {
        let mut lexer = Lexer::new("a b c");
        assert_eq!(lexer.peek().unwrap().token, Token::Ident("a".to_string()));
        assert_eq!(lexer.peek().unwrap().token, Token::Ident("a".to_string())); // Still 'a'
        assert_eq!(lexer.next().unwrap().token, Token::Ident("a".to_string()));
        assert_eq!(lexer.peek().unwrap().token, Token::Ident("b".to_string()));
    }

    #[test]
    fn test_lexer_empty() {
        let tokens: Vec<_> = tokenize("").into_iter().map(|t| t.token).collect();
        assert_eq!(tokens, vec![Token::Eof]);
    }

    #[test]
    fn test_token_display() {
        assert_eq!(format!("{}", Token::Stream), "stream");
        assert_eq!(format!("{}", Token::Plus), "+");
        assert_eq!(format!("{}", Token::Integer(42)), "42");
        assert_eq!(format!("{}", Token::Float(2.5)), "2.5");
        assert_eq!(format!("{}", Token::String("test".to_string())), "\"test\"");
        assert_eq!(format!("{}", Token::Ident("foo".to_string())), "foo");
        assert_eq!(format!("{}", Token::Eof), "EOF");
    }

    #[test]
    fn test_more_token_display() {
        assert_eq!(format!("{}", Token::Event), "event");
        assert_eq!(format!("{}", Token::Type), "type");
        assert_eq!(format!("{}", Token::Config), "config");
        assert_eq!(format!("{}", Token::Elif), "elif");
        assert_eq!(format!("{}", Token::Then), "then");
        assert_eq!(format!("{}", Token::Continue), "continue");
        assert_eq!(format!("{}", Token::PartitionBy), "partition_by");
        assert_eq!(format!("{}", Token::OrderBy), "order_by");
        assert_eq!(format!("{}", Token::Limit), "limit");
        assert_eq!(format!("{}", Token::Distinct), "distinct");
        assert_eq!(format!("{}", Token::To), "to");
        assert_eq!(format!("{}", Token::On), "on");
        assert_eq!(format!("{}", Token::All), "all");
        assert_eq!(format!("{}", Token::Within), "within");
        assert_eq!(format!("{}", Token::Pattern), "pattern");
        assert_eq!(format!("{}", Token::False), "false");
        assert_eq!(format!("{}", Token::Extends), "extends");
        assert_eq!(format!("{}", Token::Import), "import");
        assert_eq!(format!("{}", Token::Export), "export");
    }

    #[test]
    fn test_remaining_token_display() {
        assert_eq!(format!("{}", Token::Shl), "<<");
        assert_eq!(format!("{}", Token::Shr), ">>");
        assert_eq!(format!("{}", Token::MinusEq), "-=");
        assert_eq!(format!("{}", Token::StarEq), "*=");
        assert_eq!(format!("{}", Token::SlashEq), "/=");
        assert_eq!(format!("{}", Token::PercentEq), "%=");
        assert_eq!(format!("{}", Token::LBrace), "{");
        assert_eq!(format!("{}", Token::RBrace), "}");
        assert_eq!(format!("{}", Token::Duration("5m".to_string())), "5m");
        assert_eq!(
            format!("{}", Token::Timestamp("2024-01-01".to_string())),
            "2024-01-01"
        );
    }

    #[test]
    fn test_spanned_token_positions() {
        let tokens: Vec<_> = tokenize("ab cd").into_iter().collect();
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 2);
        assert_eq!(tokens[1].start, 3);
        assert_eq!(tokens[1].end, 5);
    }

    #[test]
    fn test_special_chars_in_code() {
        let tokens: Vec<_> = tokenize("a.b.c[0]").into_iter().map(|t| t.token).collect();
        assert!(tokens.contains(&Token::Dot));
        assert!(tokens.contains(&Token::LBracket));
        assert!(tokens.contains(&Token::RBracket));
    }

    #[test]
    fn test_negative_number() {
        let tokens: Vec<_> = tokenize("-42 -2.5").into_iter().map(|t| t.token).collect();
        assert!(tokens.contains(&Token::Minus));
        assert!(tokens.contains(&Token::Integer(42)));
        assert!(tokens.contains(&Token::Float(2.5)));
    }
}
