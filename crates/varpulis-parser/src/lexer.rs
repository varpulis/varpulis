//! Lexer for VarpulisQL using Logos

use logos::Logos;
use std::fmt;

/// Token type for VarpulisQL
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r\n\f]+")]
#[logos(skip r"#[^\n]*")]
#[logos(skip r"/\*([^*]|\*[^/])*\*/")]
pub enum Token {
    // === Keywords ===
    #[token("stream")]
    Stream,
    #[token("event")]
    Event,
    #[token("type")]
    Type,
    #[token("let")]
    Let,
    #[token("var")]
    Var,
    #[token("const")]
    Const,
    #[token("fn")]
    Fn,
    #[token("config")]
    Config,

    #[token("if")]
    If,
    #[token("else")]
    Else,
    #[token("elif")]
    Elif,
    #[token("then")]
    Then,
    #[token("match")]
    Match,
    #[token("for")]
    For,
    #[token("while")]
    While,
    #[token("break")]
    Break,
    #[token("continue")]
    Continue,
    #[token("return")]
    Return,

    #[token("from")]
    From,
    #[token("where")]
    Where,
    #[token("select")]
    Select,
    #[token("join")]
    Join,
    #[token("merge")]
    Merge,
    #[token("window")]
    Window,
    #[token("aggregate")]
    Aggregate,
    #[token("partition_by")]
    PartitionBy,
    #[token("order_by")]
    OrderBy,
    #[token("limit")]
    Limit,
    #[token("distinct")]
    Distinct,
    #[token("emit")]
    Emit,
    #[token("to")]
    To,
    #[token("on")]
    On,
    #[token("all")]
    All,
    #[token("within")]
    Within,

    #[token("pattern")]
    Pattern,
    #[token("attention_window")]
    AttentionWindow,
    #[token("attention_score")]
    AttentionScore,

    // Note: Stream operation names (map, filter, etc.) are NOT keywords
    // They are parsed contextually after '.' and can be used as identifiers
    #[token("true")]
    True,
    #[token("false")]
    False,
    #[token("null")]
    Null,

    #[token("and")]
    And,
    #[token("or")]
    Or,
    #[token("xor")]
    Xor,
    #[token("not")]
    Not,
    #[token("in")]
    In,
    #[token("is")]
    Is,

    #[token("as")]
    As,
    #[token("extends")]
    Extends,
    #[token("import")]
    Import,
    #[token("export")]
    Export,

    // Type keywords
    #[token("int")]
    IntType,
    #[token("float")]
    FloatType,
    #[token("bool")]
    BoolType,
    #[token("str")]
    StrType,
    #[token("timestamp")]
    TimestampType,
    #[token("duration")]
    DurationType,
    #[token("Stream")]
    StreamType,

    // === Operators ===
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,
    #[token("%")]
    Percent,
    #[token("**")]
    DoubleStar,

    #[token("==")]
    EqEq,
    #[token("!=")]
    NotEq,
    #[token("<")]
    Lt,
    #[token("<=")]
    Le,
    #[token(">")]
    Gt,
    #[token(">=")]
    Ge,

    #[token("&")]
    Amp,
    #[token("|")]
    Pipe,
    #[token("^")]
    Caret,
    #[token("~")]
    Tilde,
    #[token("<<")]
    Shl,
    #[token(">>")]
    Shr,

    #[token("=")]
    Eq,
    #[token("+=")]
    PlusEq,
    #[token("-=")]
    MinusEq,
    #[token("*=")]
    StarEq,
    #[token("/=")]
    SlashEq,
    #[token("%=")]
    PercentEq,

    #[token(".")]
    Dot,
    #[token("?.")]
    QuestionDot,
    #[token("??")]
    QuestionQuestion,
    #[token("=>")]
    FatArrow,
    #[token("->")]
    Arrow,
    #[token("..")]
    DotDot,
    #[token("..=")]
    DotDotEq,
    #[token("$")]
    Dollar,

    // === Delimiters ===
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token(",")]
    Comma,
    #[token(":")]
    Colon,
    #[token("?")]
    Question,
    #[token("@")]
    At,

    // === Literals ===
    #[regex(r"[0-9]+", |lex| lex.slice().parse::<i64>().ok())]
    Integer(i64),

    #[regex(r"[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?", |lex| lex.slice().parse::<f64>().ok())]
    Float(f64),

    #[regex(r#""([^"\\]|\\.)*""#, |lex| {
        let s = lex.slice();
        Some(s[1..s.len()-1].to_string())
    })]
    #[regex(r#"'([^'\\]|\\.)*'"#, |lex| {
        let s = lex.slice();
        Some(s[1..s.len()-1].to_string())
    })]
    String(String),

    #[regex(r"[0-9]+(ns|us|ms|s|m|h|d)", |lex| Some(lex.slice().to_string()))]
    Duration(String),

    #[regex(r"@[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})?)?", |lex| Some(lex.slice().to_string()))]
    Timestamp(String),

    // === Identifier ===
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| Some(lex.slice().to_string()))]
    Ident(String),

    // === Special ===
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Stream => write!(f, "stream"),
            Token::Event => write!(f, "event"),
            Token::Type => write!(f, "type"),
            Token::Let => write!(f, "let"),
            Token::Var => write!(f, "var"),
            Token::Const => write!(f, "const"),
            Token::Fn => write!(f, "fn"),
            Token::Config => write!(f, "config"),
            Token::If => write!(f, "if"),
            Token::Else => write!(f, "else"),
            Token::Elif => write!(f, "elif"),
            Token::Then => write!(f, "then"),
            Token::Match => write!(f, "match"),
            Token::For => write!(f, "for"),
            Token::While => write!(f, "while"),
            Token::Break => write!(f, "break"),
            Token::Continue => write!(f, "continue"),
            Token::Return => write!(f, "return"),
            Token::From => write!(f, "from"),
            Token::Where => write!(f, "where"),
            Token::Select => write!(f, "select"),
            Token::Join => write!(f, "join"),
            Token::Merge => write!(f, "merge"),
            Token::Window => write!(f, "window"),
            Token::Aggregate => write!(f, "aggregate"),
            Token::PartitionBy => write!(f, "partition_by"),
            Token::OrderBy => write!(f, "order_by"),
            Token::Limit => write!(f, "limit"),
            Token::Distinct => write!(f, "distinct"),
            Token::Emit => write!(f, "emit"),
            Token::To => write!(f, "to"),
            Token::On => write!(f, "on"),
            Token::All => write!(f, "all"),
            Token::Within => write!(f, "within"),
            Token::Pattern => write!(f, "pattern"),
            Token::AttentionWindow => write!(f, "attention_window"),
            Token::AttentionScore => write!(f, "attention_score"),
            Token::True => write!(f, "true"),
            Token::False => write!(f, "false"),
            Token::Null => write!(f, "null"),
            Token::And => write!(f, "and"),
            Token::Or => write!(f, "or"),
            Token::Xor => write!(f, "xor"),
            Token::Not => write!(f, "not"),
            Token::In => write!(f, "in"),
            Token::Is => write!(f, "is"),
            Token::As => write!(f, "as"),
            Token::Extends => write!(f, "extends"),
            Token::Import => write!(f, "import"),
            Token::Export => write!(f, "export"),
            Token::IntType => write!(f, "int"),
            Token::FloatType => write!(f, "float"),
            Token::BoolType => write!(f, "bool"),
            Token::StrType => write!(f, "str"),
            Token::TimestampType => write!(f, "timestamp"),
            Token::DurationType => write!(f, "duration"),
            Token::StreamType => write!(f, "Stream"),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
            Token::DoubleStar => write!(f, "**"),
            Token::EqEq => write!(f, "=="),
            Token::NotEq => write!(f, "!="),
            Token::Lt => write!(f, "<"),
            Token::Le => write!(f, "<="),
            Token::Gt => write!(f, ">"),
            Token::Ge => write!(f, ">="),
            Token::Amp => write!(f, "&"),
            Token::Pipe => write!(f, "|"),
            Token::Caret => write!(f, "^"),
            Token::Tilde => write!(f, "~"),
            Token::Shl => write!(f, "<<"),
            Token::Shr => write!(f, ">>"),
            Token::Eq => write!(f, "="),
            Token::PlusEq => write!(f, "+="),
            Token::MinusEq => write!(f, "-="),
            Token::StarEq => write!(f, "*="),
            Token::SlashEq => write!(f, "/="),
            Token::PercentEq => write!(f, "%="),
            Token::Dot => write!(f, "."),
            Token::QuestionDot => write!(f, "?."),
            Token::QuestionQuestion => write!(f, "??"),
            Token::FatArrow => write!(f, "=>"),
            Token::Arrow => write!(f, "->"),
            Token::DotDot => write!(f, ".."),
            Token::DotDotEq => write!(f, "..="),
            Token::Dollar => write!(f, "$"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::Comma => write!(f, ","),
            Token::Colon => write!(f, ":"),
            Token::Question => write!(f, "?"),
            Token::At => write!(f, "@"),
            Token::Integer(n) => write!(f, "{}", n),
            Token::Float(n) => write!(f, "{}", n),
            Token::String(s) => write!(f, "\"{}\"", s),
            Token::Duration(d) => write!(f, "{}", d),
            Token::Timestamp(t) => write!(f, "{}", t),
            Token::Ident(s) => write!(f, "{}", s),
            Token::Eof => write!(f, "EOF"),
        }
    }
}

/// Spanned token with position information
#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub start: usize,
    pub end: usize,
}

/// Lexer wrapper that produces spanned tokens
pub struct Lexer<'source> {
    inner: logos::Lexer<'source, Token>,
    peeked: Option<SpannedToken>,
    eof_emitted: bool,
}

impl<'source> Lexer<'source> {
    pub fn new(source: &'source str) -> Self {
        Self {
            inner: Token::lexer(source),
            peeked: None,
            eof_emitted: false,
        }
    }

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
            Some(Err(_)) => {
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

impl<'source> Iterator for Lexer<'source> {
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
        let tokens: Vec<_> = tokenize("42 3.14 \"hello\" 5s true null")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Integer(42),
                Token::Float(3.14),
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
        let tokens: Vec<_> = tokenize("stream Trades from TradeEvent")
            .into_iter()
            .map(|t| t.token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Stream,
                Token::Ident("Trades".to_string()),
                Token::From,
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
        assert_eq!(format!("{}", Token::Float(3.14)), "3.14");
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
        assert_eq!(format!("{}", Token::AttentionWindow), "attention_window");
        assert_eq!(format!("{}", Token::AttentionScore), "attention_score");
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
        let tokens: Vec<_> = tokenize("-42 -3.14").into_iter().map(|t| t.token).collect();
        assert!(tokens.contains(&Token::Minus));
        assert!(tokens.contains(&Token::Integer(42)));
        assert!(tokens.contains(&Token::Float(3.14)));
    }
}
