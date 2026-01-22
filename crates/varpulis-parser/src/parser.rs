//! Recursive descent parser for VarpulisQL
//!
//! For the MVP, we use a hand-written recursive descent parser instead of LALRPOP
//! for faster iteration and simpler debugging.

use crate::error::{ParseError, ParseResult};
use crate::lexer::{Lexer, SpannedToken, Token};
use varpulis_core::ast::*;
use varpulis_core::span::{Span, Spanned};
use varpulis_core::types::Type;

/// Parse a VarpulisQL source string into a Program AST
pub fn parse(source: &str) -> ParseResult<Program> {
    let mut parser = Parser::new(source);
    parser.parse_program()
}

/// Parser state
pub struct Parser<'source> {
    lexer: Lexer<'source>,
    current: SpannedToken,
    previous: SpannedToken,
}

impl<'source> Parser<'source> {
    pub fn new(source: &'source str) -> Self {
        let mut lexer = Lexer::new(source);
        let current = lexer.next().unwrap_or(SpannedToken {
            token: Token::Eof,
            start: 0,
            end: 0,
        });
        Self {
            lexer,
            current: current.clone(),
            previous: current,
        }
    }

    fn span(&self) -> Span {
        Span::new(self.current.start, self.current.end)
    }

    fn prev_span(&self) -> Span {
        Span::new(self.previous.start, self.previous.end)
    }

    fn advance(&mut self) {
        self.previous = self.current.clone();
        self.current = self.lexer.next().unwrap_or(SpannedToken {
            token: Token::Eof,
            start: self.previous.end,
            end: self.previous.end,
        });
    }

    fn check(&self, token: &Token) -> bool {
        std::mem::discriminant(&self.current.token) == std::mem::discriminant(token)
    }

    fn is_at_end(&self) -> bool {
        matches!(self.current.token, Token::Eof)
    }

    fn consume(&mut self, expected: &Token, msg: &str) -> ParseResult<SpannedToken> {
        if self.check(expected) {
            let tok = self.current.clone();
            self.advance();
            Ok(tok)
        } else {
            Err(ParseError::UnexpectedToken {
                position: self.current.start,
                expected: msg.to_string(),
                found: format!("{}", self.current.token),
            })
        }
    }

    fn match_token(&mut self, token: &Token) -> bool {
        if self.check(token) {
            self.advance();
            true
        } else {
            false
        }
    }

    // ========================================================================
    // Program
    // ========================================================================

    fn parse_program(&mut self) -> ParseResult<Program> {
        let mut statements = Vec::new();
        while !self.is_at_end() {
            statements.push(self.parse_statement()?);
        }
        Ok(Program { statements })
    }

    // ========================================================================
    // Statements
    // ========================================================================

    fn parse_statement(&mut self) -> ParseResult<Spanned<Stmt>> {
        let start = self.span();
        let stmt = match &self.current.token {
            Token::Stream => self.parse_stream_decl()?,
            Token::Event => self.parse_event_decl()?,
            Token::Type => self.parse_type_decl()?,
            Token::Let => self.parse_var_decl(false)?,
            Token::Var => self.parse_var_decl(true)?,
            Token::Const => self.parse_const_decl()?,
            Token::Fn => self.parse_fn_decl()?,
            Token::Config => self.parse_config()?,
            Token::Import => self.parse_import()?,
            Token::If => self.parse_if_stmt()?,
            Token::For => self.parse_for_stmt()?,
            Token::While => self.parse_while_stmt()?,
            Token::Return => self.parse_return_stmt()?,
            Token::Break => {
                self.advance();
                Stmt::Break
            }
            Token::Continue => {
                self.advance();
                Stmt::Continue
            }
            _ => Stmt::Expr(self.parse_expr()?),
        };
        let end = self.prev_span();
        Ok(Spanned::new(stmt, start.merge(end)))
    }

    // ========================================================================
    // Stream Declaration
    // ========================================================================

    fn parse_stream_decl(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Stream, "stream")?;
        let name = self.parse_identifier()?;

        // Check for type annotation
        let type_annotation = if self.match_token(&Token::Colon) {
            Some(self.parse_type()?)
        } else {
            None
        };

        // stream X from Y
        if self.match_token(&Token::From) {
            let source_name = self.parse_identifier()?;
            return Ok(Stmt::StreamDecl {
                name,
                type_annotation,
                source: StreamSource::From(source_name),
                ops: Vec::new(),
            });
        }

        // stream X = expr
        self.consume(&Token::Eq, "= or from")?;
        let (source, ops) = self.parse_stream_expr()?;

        Ok(Stmt::StreamDecl {
            name,
            type_annotation,
            source,
            ops,
        })
    }

    fn parse_stream_expr(&mut self) -> ParseResult<(StreamSource, Vec<StreamOp>)> {
        let source = self.parse_stream_source()?;
        let mut ops = Vec::new();

        while self.match_token(&Token::Dot) {
            ops.push(self.parse_stream_op()?);
        }

        Ok((source, ops))
    }

    fn parse_stream_source(&mut self) -> ParseResult<StreamSource> {
        if self.match_token(&Token::Merge) {
            self.consume(&Token::LParen, "(")?;
            let streams = self.parse_inline_stream_list()?;
            self.consume(&Token::RParen, ")")?;
            return Ok(StreamSource::Merge(streams));
        }

        if self.match_token(&Token::Join) {
            self.consume(&Token::LParen, "(")?;
            let clauses = self.parse_join_clause_list()?;
            self.consume(&Token::RParen, ")")?;
            return Ok(StreamSource::Join(clauses));
        }

        let name = self.parse_identifier()?;
        Ok(StreamSource::Ident(name))
    }

    fn parse_inline_stream_list(&mut self) -> ParseResult<Vec<InlineStreamDecl>> {
        let mut streams = vec![self.parse_inline_stream()?];
        while self.match_token(&Token::Comma) {
            streams.push(self.parse_inline_stream()?);
        }
        Ok(streams)
    }

    fn parse_inline_stream(&mut self) -> ParseResult<InlineStreamDecl> {
        self.consume(&Token::Stream, "stream")?;
        let name = self.parse_identifier()?;
        self.consume(&Token::From, "from")?;
        let source = self.parse_identifier()?;

        let filter = if self.match_token(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(InlineStreamDecl { name, source, filter })
    }

    fn parse_join_clause_list(&mut self) -> ParseResult<Vec<JoinClause>> {
        let mut clauses = vec![self.parse_join_clause()?];
        while self.match_token(&Token::Comma) {
            clauses.push(self.parse_join_clause()?);
        }
        Ok(clauses)
    }

    fn parse_join_clause(&mut self) -> ParseResult<JoinClause> {
        self.consume(&Token::Stream, "stream")?;
        let name = self.parse_identifier()?;
        self.consume(&Token::From, "from")?;
        let source = self.parse_identifier()?;

        let on = if self.match_token(&Token::On) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(JoinClause { name, source, on })
    }

    fn parse_stream_op(&mut self) -> ParseResult<StreamOp> {
        // Stream operations can be keywords or identifiers
        let ident = self.parse_stream_op_name()?;
        
        match ident.as_str() {
            "where" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Where(expr))
            }
            "select" => {
                self.consume(&Token::LParen, "(")?;
                let items = self.parse_select_list()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Select(items))
            }
            "window" => {
                self.consume(&Token::LParen, "(")?;
                let args = self.parse_window_args()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Window(args))
            }
            "aggregate" => {
                self.consume(&Token::LParen, "(")?;
                let items = self.parse_agg_list()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Aggregate(items))
            }
            "partition_by" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::PartitionBy(expr))
            }
            "order_by" => {
                self.consume(&Token::LParen, "(")?;
                let items = self.parse_order_list()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::OrderBy(items))
            }
            "limit" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Limit(expr))
            }
            "distinct" => {
                self.consume(&Token::LParen, "(")?;
                let expr = if !self.check(&Token::RParen) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Distinct(expr))
            }
            "map" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Map(expr))
            }
            "filter" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Filter(expr))
            }
            "tap" => {
                self.consume(&Token::LParen, "(")?;
                let args = self.parse_named_arg_list()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Tap(args))
            }
            "emit" => {
                self.consume(&Token::LParen, "(")?;
                let args = if !self.check(&Token::RParen) {
                    self.parse_named_arg_list()?
                } else {
                    Vec::new()
                };
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Emit(args))
            }
            "to" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::To(expr))
            }
            "pattern" => {
                self.consume(&Token::LParen, "(")?;
                let def = self.parse_pattern_def()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Pattern(def))
            }
            "attention_window" => {
                self.consume(&Token::LParen, "(")?;
                let args = self.parse_named_arg_list()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::AttentionWindow(args))
            }
            "concurrent" => {
                self.consume(&Token::LParen, "(")?;
                let args = self.parse_named_arg_list()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Concurrent(args))
            }
            "process" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Process(expr))
            }
            "on_error" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::OnError(expr))
            }
            "collect" => {
                self.consume(&Token::LParen, "(")?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Collect)
            }
            _ => Err(ParseError::Custom {
                span: self.prev_span(),
                message: format!("Unknown stream operation: {}", ident),
            }),
        }
    }

    fn parse_select_list(&mut self) -> ParseResult<Vec<SelectItem>> {
        let mut items = vec![self.parse_select_item()?];
        while self.match_token(&Token::Comma) {
            items.push(self.parse_select_item()?);
        }
        Ok(items)
    }

    fn parse_select_item(&mut self) -> ParseResult<SelectItem> {
        let name = self.parse_identifier()?;
        if self.match_token(&Token::Colon) {
            let expr = self.parse_expr()?;
            Ok(SelectItem::Alias(name, expr))
        } else {
            Ok(SelectItem::Field(name))
        }
    }

    fn parse_window_args(&mut self) -> ParseResult<WindowArgs> {
        let duration = self.parse_expr()?;
        let mut sliding = None;
        let mut policy = None;

        while self.match_token(&Token::Comma) {
            let key = self.parse_identifier()?;
            self.consume(&Token::Colon, ":")?;
            let value = self.parse_expr()?;
            match key.as_str() {
                "sliding" => sliding = Some(value),
                "policy" => policy = Some(value),
                _ => {}
            }
        }

        Ok(WindowArgs { duration, sliding, policy })
    }

    fn parse_agg_list(&mut self) -> ParseResult<Vec<AggItem>> {
        let mut items = vec![self.parse_agg_item()?];
        while self.match_token(&Token::Comma) {
            items.push(self.parse_agg_item()?);
        }
        Ok(items)
    }

    fn parse_agg_item(&mut self) -> ParseResult<AggItem> {
        let alias = self.parse_identifier()?;
        self.consume(&Token::Colon, ":")?;
        let func = self.parse_identifier()?;
        self.consume(&Token::LParen, "(")?;
        let arg = if !self.check(&Token::RParen) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        self.consume(&Token::RParen, ")")?;
        Ok(AggItem { alias, func, arg })
    }

    fn parse_order_list(&mut self) -> ParseResult<Vec<OrderItem>> {
        let mut items = vec![self.parse_order_item()?];
        while self.match_token(&Token::Comma) {
            items.push(self.parse_order_item()?);
        }
        Ok(items)
    }

    fn parse_order_item(&mut self) -> ParseResult<OrderItem> {
        let expr = self.parse_expr()?;
        let descending = if let Token::Ident(s) = &self.current.token {
            if s == "desc" {
                self.advance();
                true
            } else if s == "asc" {
                self.advance();
                false
            } else {
                false
            }
        } else {
            false
        };
        Ok(OrderItem { expr, descending })
    }

    fn parse_named_arg_list(&mut self) -> ParseResult<Vec<NamedArg>> {
        let mut args = vec![self.parse_named_arg()?];
        while self.match_token(&Token::Comma) {
            args.push(self.parse_named_arg()?);
        }
        Ok(args)
    }

    fn parse_named_arg(&mut self) -> ParseResult<NamedArg> {
        let name = self.parse_identifier()?;
        self.consume(&Token::Colon, ":")?;
        let value = self.parse_expr()?;
        Ok(NamedArg { name, value })
    }

    fn parse_pattern_def(&mut self) -> ParseResult<PatternDef> {
        let name = self.parse_identifier()?;
        self.consume(&Token::Colon, ":")?;
        let matcher = self.parse_expr()?;
        Ok(PatternDef { name, matcher })
    }

    // ========================================================================
    // Event Declaration
    // ========================================================================

    fn parse_event_decl(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Event, "event")?;
        let name = self.parse_identifier()?;

        let extends = if self.match_token(&Token::Extends) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        self.consume(&Token::Colon, ":")?;
        let fields = self.parse_field_list()?;

        Ok(Stmt::EventDecl { name, extends, fields })
    }

    fn parse_field_list(&mut self) -> ParseResult<Vec<Field>> {
        let mut fields = Vec::new();
        while let Token::Ident(_) = &self.current.token {
            fields.push(self.parse_field()?);
        }
        Ok(fields)
    }

    fn parse_field(&mut self) -> ParseResult<Field> {
        let name = self.parse_identifier()?;
        self.consume(&Token::Colon, ":")?;
        let ty = self.parse_type()?;
        let optional = self.match_token(&Token::Question);
        Ok(Field { name, ty, optional })
    }

    // ========================================================================
    // Type Declaration
    // ========================================================================

    fn parse_type_decl(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Type, "type")?;
        let name = self.parse_identifier()?;
        self.consume(&Token::Eq, "=")?;
        let ty = self.parse_type()?;
        Ok(Stmt::TypeDecl { name, ty })
    }

    // ========================================================================
    // Variable Declaration
    // ========================================================================

    fn parse_var_decl(&mut self, mutable: bool) -> ParseResult<Stmt> {
        self.advance(); // consume let/var
        let name = self.parse_identifier()?;

        let ty = if self.match_token(&Token::Colon) {
            Some(self.parse_type()?)
        } else {
            None
        };

        self.consume(&Token::Eq, "=")?;
        let value = self.parse_expr()?;

        Ok(Stmt::VarDecl { mutable, name, ty, value })
    }

    fn parse_const_decl(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Const, "const")?;
        let name = self.parse_identifier()?;

        let ty = if self.match_token(&Token::Colon) {
            Some(self.parse_type()?)
        } else {
            None
        };

        self.consume(&Token::Eq, "=")?;
        let value = self.parse_expr()?;

        Ok(Stmt::ConstDecl { name, ty, value })
    }

    // ========================================================================
    // Function Declaration
    // ========================================================================

    fn parse_fn_decl(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Fn, "fn")?;
        let name = self.parse_identifier()?;
        self.consume(&Token::LParen, "(")?;

        let params = if !self.check(&Token::RParen) {
            self.parse_param_list()?
        } else {
            Vec::new()
        };

        self.consume(&Token::RParen, ")")?;

        let ret = if self.match_token(&Token::Arrow) {
            Some(self.parse_type()?)
        } else {
            None
        };

        self.consume(&Token::Colon, ":")?;
        let body = self.parse_block()?;

        Ok(Stmt::FnDecl { name, params, ret, body })
    }

    fn parse_param_list(&mut self) -> ParseResult<Vec<Param>> {
        let mut params = vec![self.parse_param()?];
        while self.match_token(&Token::Comma) {
            params.push(self.parse_param()?);
        }
        Ok(params)
    }

    fn parse_param(&mut self) -> ParseResult<Param> {
        let name = self.parse_identifier()?;
        self.consume(&Token::Colon, ":")?;
        let ty = self.parse_type()?;
        Ok(Param { name, ty })
    }

    fn parse_block(&mut self) -> ParseResult<Vec<Spanned<Stmt>>> {
        // For MVP, we just parse a single expression or statement
        // A full implementation would handle indentation-based blocks
        let stmt = self.parse_statement()?;
        Ok(vec![stmt])
    }

    // ========================================================================
    // Control Flow
    // ========================================================================

    fn parse_if_stmt(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::If, "if")?;
        let cond = self.parse_expr()?;
        self.consume(&Token::Colon, ":")?;
        let then_branch = self.parse_block()?;

        let mut elif_branches = Vec::new();
        while self.match_token(&Token::Elif) {
            let elif_cond = self.parse_expr()?;
            self.consume(&Token::Colon, ":")?;
            let elif_body = self.parse_block()?;
            elif_branches.push((elif_cond, elif_body));
        }

        let else_branch = if self.match_token(&Token::Else) {
            self.consume(&Token::Colon, ":")?;
            Some(self.parse_block()?)
        } else {
            None
        };

        Ok(Stmt::If {
            cond,
            then_branch,
            elif_branches,
            else_branch,
        })
    }

    fn parse_for_stmt(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::For, "for")?;
        let var = self.parse_identifier()?;
        self.consume(&Token::In, "in")?;
        let iter = self.parse_expr()?;
        self.consume(&Token::Colon, ":")?;
        let body = self.parse_block()?;

        Ok(Stmt::For { var, iter, body })
    }

    fn parse_while_stmt(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::While, "while")?;
        let cond = self.parse_expr()?;
        self.consume(&Token::Colon, ":")?;
        let body = self.parse_block()?;

        Ok(Stmt::While { cond, body })
    }

    fn parse_return_stmt(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Return, "return")?;
        let value = if !self.is_at_end() && !self.check(&Token::Eof) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Stmt::Return(value))
    }

    fn parse_config(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Config, "config")?;
        self.consume(&Token::Colon, ":")?;
        let items = self.parse_config_items()?;
        Ok(Stmt::Config(items))
    }

    fn parse_config_items(&mut self) -> ParseResult<Vec<ConfigItem>> {
        let mut items = Vec::new();
        while let Token::Ident(_) = &self.current.token {
            items.push(self.parse_config_item()?);
        }
        Ok(items)
    }

    fn parse_config_item(&mut self) -> ParseResult<ConfigItem> {
        let name = self.parse_identifier()?;
        self.consume(&Token::Colon, ":")?;

        // Check if nested or value
        if let Token::Ident(_) = &self.current.token {
            // Could be nested or identifier value
            // For simplicity, treat as value for MVP
            let value = self.parse_config_value()?;
            Ok(ConfigItem::Value(name, value))
        } else {
            let value = self.parse_config_value()?;
            Ok(ConfigItem::Value(name, value))
        }
    }

    fn parse_config_value(&mut self) -> ParseResult<ConfigValue> {
        match &self.current.token {
            Token::True => {
                self.advance();
                Ok(ConfigValue::Bool(true))
            }
            Token::False => {
                self.advance();
                Ok(ConfigValue::Bool(false))
            }
            Token::Integer(n) => {
                let n = *n;
                self.advance();
                Ok(ConfigValue::Int(n))
            }
            Token::Float(n) => {
                let n = *n;
                self.advance();
                Ok(ConfigValue::Float(n))
            }
            Token::String(s) => {
                let s = s.clone();
                self.advance();
                Ok(ConfigValue::Str(s))
            }
            Token::Duration(d) => {
                let d = parse_duration(&d);
                self.advance();
                Ok(ConfigValue::Duration(d))
            }
            Token::Ident(s) => {
                let s = s.clone();
                self.advance();
                Ok(ConfigValue::Ident(s))
            }
            _ => Err(ParseError::UnexpectedToken {
                position: self.current.start,
                expected: "config value".to_string(),
                found: format!("{}", self.current.token),
            }),
        }
    }

    fn parse_import(&mut self) -> ParseResult<Stmt> {
        self.consume(&Token::Import, "import")?;
        let path = match &self.current.token {
            Token::String(s) => {
                let s = s.clone();
                self.advance();
                s
            }
            _ => return Err(ParseError::UnexpectedToken {
                position: self.current.start,
                expected: "string".to_string(),
                found: format!("{}", self.current.token),
            }),
        };

        let alias = if self.match_token(&Token::As) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Stmt::Import { path, alias })
    }

    // ========================================================================
    // Types
    // ========================================================================

    fn parse_type(&mut self) -> ParseResult<Type> {
        let base = self.parse_base_type()?;

        // Check for optional suffix
        if self.match_token(&Token::Question) {
            Ok(Type::Optional(Box::new(base)))
        } else {
            Ok(base)
        }
    }

    fn parse_base_type(&mut self) -> ParseResult<Type> {
        match &self.current.token {
            Token::IntType => {
                self.advance();
                Ok(Type::Int)
            }
            Token::FloatType => {
                self.advance();
                Ok(Type::Float)
            }
            Token::BoolType => {
                self.advance();
                Ok(Type::Bool)
            }
            Token::StrType => {
                self.advance();
                Ok(Type::Str)
            }
            Token::TimestampType => {
                self.advance();
                Ok(Type::Timestamp)
            }
            Token::DurationType => {
                self.advance();
                Ok(Type::Duration)
            }
            Token::StreamType => {
                self.advance();
                self.consume(&Token::Lt, "<")?;
                let inner = self.parse_type()?;
                self.consume(&Token::Gt, ">")?;
                Ok(Type::Stream(Box::new(inner)))
            }
            Token::LBracket => {
                self.advance();
                let inner = self.parse_type()?;
                self.consume(&Token::RBracket, "]")?;
                Ok(Type::Array(Box::new(inner)))
            }
            Token::LBrace => {
                self.advance();
                let key = self.parse_type()?;
                self.consume(&Token::Colon, ":")?;
                let value = self.parse_type()?;
                self.consume(&Token::RBrace, "}")?;
                Ok(Type::Map(Box::new(key), Box::new(value)))
            }
            Token::LParen => {
                self.advance();
                let first = self.parse_type()?;
                let mut types = vec![first];
                while self.match_token(&Token::Comma) {
                    types.push(self.parse_type()?);
                }
                self.consume(&Token::RParen, ")")?;
                Ok(Type::Tuple(types))
            }
            Token::Ident(name) => {
                let name = name.clone();
                self.advance();
                Ok(Type::Named(name))
            }
            _ => Err(ParseError::UnexpectedToken {
                position: self.current.start,
                expected: "type".to_string(),
                found: format!("{}", self.current.token),
            }),
        }
    }

    // ========================================================================
    // Expressions
    // ========================================================================

    fn parse_expr(&mut self) -> ParseResult<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_and_expr()?;
        while self.match_token(&Token::Or) {
            let right = self.parse_and_expr()?;
            left = Expr::Binary {
                op: BinOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_not_expr()?;
        while self.match_token(&Token::And) {
            let right = self.parse_not_expr()?;
            left = Expr::Binary {
                op: BinOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> ParseResult<Expr> {
        if self.match_token(&Token::Not) {
            let expr = self.parse_not_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }
        self.parse_comparison_expr()
    }

    fn parse_comparison_expr(&mut self) -> ParseResult<Expr> {
        let left = self.parse_additive_expr()?;

        let op = match &self.current.token {
            Token::EqEq => Some(BinOp::Eq),
            Token::NotEq => Some(BinOp::NotEq),
            Token::Lt => Some(BinOp::Lt),
            Token::Le => Some(BinOp::Le),
            Token::Gt => Some(BinOp::Gt),
            Token::Ge => Some(BinOp::Ge),
            Token::In => Some(BinOp::In),
            Token::Is => Some(BinOp::Is),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive_expr()?;
            Ok(Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_additive_expr(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_multiplicative_expr()?;
        loop {
            let op = match &self.current.token {
                Token::Plus => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative_expr()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_unary_expr()?;
        loop {
            let op = match &self.current.token {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary_expr()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> ParseResult<Expr> {
        if self.match_token(&Token::Minus) {
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            });
        }
        if self.match_token(&Token::Tilde) {
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::Unary {
                op: UnaryOp::BitNot,
                expr: Box::new(expr),
            });
        }
        self.parse_postfix_expr()
    }

    fn parse_postfix_expr(&mut self) -> ParseResult<Expr> {
        let mut expr = self.parse_primary_expr()?;

        loop {
            if self.match_token(&Token::Dot) {
                let member = self.parse_identifier()?;
                
                // Check if it's a method call
                if self.check(&Token::LParen) {
                    self.advance();
                    let args = if !self.check(&Token::RParen) {
                        self.parse_arg_list()?
                    } else {
                        Vec::new()
                    };
                    self.consume(&Token::RParen, ")")?;
                    
                    expr = Expr::Call {
                        func: Box::new(Expr::Member {
                            expr: Box::new(expr),
                            member,
                        }),
                        args,
                    };
                } else {
                    expr = Expr::Member {
                        expr: Box::new(expr),
                        member,
                    };
                }
            } else if self.match_token(&Token::QuestionDot) {
                let member = self.parse_identifier()?;
                expr = Expr::OptionalMember {
                    expr: Box::new(expr),
                    member,
                };
            } else if self.match_token(&Token::LBracket) {
                let index = self.parse_expr()?;
                self.consume(&Token::RBracket, "]")?;
                expr = Expr::Index {
                    expr: Box::new(expr),
                    index: Box::new(index),
                };
            } else if self.match_token(&Token::LParen) {
                let args = if !self.check(&Token::RParen) {
                    self.parse_arg_list()?
                } else {
                    Vec::new()
                };
                self.consume(&Token::RParen, ")")?;
                expr = Expr::Call {
                    func: Box::new(expr),
                    args,
                };
            } else if self.match_token(&Token::QuestionQuestion) {
                let default = self.parse_expr()?;
                expr = Expr::Coalesce {
                    expr: Box::new(expr),
                    default: Box::new(default),
                };
            } else {
                break;
            }
        }

        // Check for lambda arrow after identifier
        if let Expr::Ident(param) = &expr {
            if self.match_token(&Token::FatArrow) {
                let body = self.parse_expr()?;
                return Ok(Expr::Lambda {
                    params: vec![param.clone()],
                    body: Box::new(body),
                });
            }
        }

        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> ParseResult<Expr> {
        match &self.current.token {
            Token::Null => {
                self.advance();
                Ok(Expr::Null)
            }
            Token::True => {
                self.advance();
                Ok(Expr::Bool(true))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Bool(false))
            }
            Token::Integer(n) => {
                let n = *n;
                self.advance();
                Ok(Expr::Int(n))
            }
            Token::Float(n) => {
                let n = *n;
                self.advance();
                Ok(Expr::Float(n))
            }
            Token::String(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::Str(s))
            }
            Token::Duration(d) => {
                let ns = parse_duration(&d);
                self.advance();
                Ok(Expr::Duration(ns))
            }
            Token::Timestamp(t) => {
                let ns = parse_timestamp(&t);
                self.advance();
                Ok(Expr::Timestamp(ns))
            }
            Token::Ident(name) => {
                let name = name.clone();
                self.advance();
                Ok(Expr::Ident(name))
            }
            Token::LParen => {
                self.advance();
                
                // Check for lambda with multiple params
                if let Token::Ident(_) = &self.current.token {
                    let first_param = self.parse_identifier()?;
                    
                    if self.match_token(&Token::Comma) {
                        // Multiple params lambda
                        let mut params = vec![first_param];
                        params.push(self.parse_identifier()?);
                        while self.match_token(&Token::Comma) {
                            params.push(self.parse_identifier()?);
                        }
                        self.consume(&Token::RParen, ")")?;
                        self.consume(&Token::FatArrow, "=>")?;
                        let body = self.parse_expr()?;
                        return Ok(Expr::Lambda {
                            params,
                            body: Box::new(body),
                        });
                    } else if self.match_token(&Token::RParen) {
                        // Single param lambda or function call
                        if self.match_token(&Token::FatArrow) {
                            let body = self.parse_expr()?;
                            return Ok(Expr::Lambda {
                                params: vec![first_param],
                                body: Box::new(body),
                            });
                        }
                        // It was just (ident)
                        return Ok(Expr::Ident(first_param));
                    } else {
                        // It's a grouped expression starting with identifier
                        // Reparse as expression... this is tricky
                        // For MVP, treat as identifier
                        self.consume(&Token::RParen, ")")?;
                        return Ok(Expr::Ident(first_param));
                    }
                }
                
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(expr)
            }
            Token::LBracket => {
                self.advance();
                let mut elements = Vec::new();
                if !self.check(&Token::RBracket) {
                    elements.push(self.parse_expr()?);
                    while self.match_token(&Token::Comma) {
                        elements.push(self.parse_expr()?);
                    }
                }
                self.consume(&Token::RBracket, "]")?;
                Ok(Expr::Array(elements))
            }
            Token::LBrace => {
                self.advance();
                let mut entries = Vec::new();
                if !self.check(&Token::RBrace) {
                    let key = match &self.current.token {
                        Token::String(s) => {
                            let s = s.clone();
                            self.advance();
                            s
                        }
                        Token::Ident(s) => {
                            let s = s.clone();
                            self.advance();
                            s
                        }
                        _ => return Err(ParseError::UnexpectedToken {
                            position: self.current.start,
                            expected: "string or identifier".to_string(),
                            found: format!("{}", self.current.token),
                        }),
                    };
                    self.consume(&Token::Colon, ":")?;
                    let value = self.parse_expr()?;
                    entries.push((key, value));

                    while self.match_token(&Token::Comma) {
                        let key = match &self.current.token {
                            Token::String(s) => {
                                let s = s.clone();
                                self.advance();
                                s
                            }
                            Token::Ident(s) => {
                                let s = s.clone();
                                self.advance();
                                s
                            }
                            _ => break,
                        };
                        self.consume(&Token::Colon, ":")?;
                        let value = self.parse_expr()?;
                        entries.push((key, value));
                    }
                }
                self.consume(&Token::RBrace, "}")?;
                Ok(Expr::Map(entries))
            }
            Token::If => {
                self.advance();
                let cond = self.parse_expr()?;
                self.consume(&Token::Then, "then")?;
                let then_branch = self.parse_expr()?;
                self.consume(&Token::Else, "else")?;
                let else_branch = self.parse_expr()?;
                Ok(Expr::If {
                    cond: Box::new(cond),
                    then_branch: Box::new(then_branch),
                    else_branch: Box::new(else_branch),
                })
            }
            _ => Err(ParseError::UnexpectedToken {
                position: self.current.start,
                expected: "expression".to_string(),
                found: format!("{}", self.current.token),
            }),
        }
    }

    fn parse_arg_list(&mut self) -> ParseResult<Vec<Arg>> {
        let mut args = vec![self.parse_arg()?];
        while self.match_token(&Token::Comma) {
            args.push(self.parse_arg()?);
        }
        Ok(args)
    }

    fn parse_arg(&mut self) -> ParseResult<Arg> {
        // Check for named argument
        if let Token::Ident(name) = &self.current.token {
            let name = name.clone();
            let saved_pos = self.current.start;
            self.advance();
            
            if self.match_token(&Token::Colon) {
                let value = self.parse_expr()?;
                return Ok(Arg::Named(name, value));
            } else {
                // It was a positional argument that's an identifier
                // We need to handle this - for now return as expression
                // We already consumed the identifier, so reconstruct
                return Ok(Arg::Positional(Expr::Ident(name)));
            }
        }
        
        let expr = self.parse_expr()?;
        Ok(Arg::Positional(expr))
    }

    fn parse_identifier(&mut self) -> ParseResult<String> {
        match &self.current.token {
            Token::Ident(name) => {
                let name = name.clone();
                self.advance();
                Ok(name)
            }
            _ => Err(ParseError::UnexpectedToken {
                position: self.current.start,
                expected: "identifier".to_string(),
                found: format!("{}", self.current.token),
            }),
        }
    }

    /// Parse a stream operation name - can be a keyword or identifier
    fn parse_stream_op_name(&mut self) -> ParseResult<String> {
        let name = match &self.current.token {
            Token::Ident(name) => name.clone(),
            Token::Where => "where".to_string(),
            Token::Select => "select".to_string(),
            Token::Window => "window".to_string(),
            Token::Aggregate => "aggregate".to_string(),
            Token::PartitionBy => "partition_by".to_string(),
            Token::OrderBy => "order_by".to_string(),
            Token::Limit => "limit".to_string(),
            Token::Distinct => "distinct".to_string(),
            Token::Emit => "emit".to_string(),
            Token::To => "to".to_string(),
            Token::Pattern => "pattern".to_string(),
            Token::AttentionWindow => "attention_window".to_string(),
            _ => {
                return Err(ParseError::UnexpectedToken {
                    position: self.current.start,
                    expected: "stream operation".to_string(),
                    found: format!("{}", self.current.token),
                });
            }
        };
        self.advance();
        Ok(name)
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn parse_duration(s: &str) -> u64 {
    let len = s.len();
    let (num_str, unit) = if s.ends_with("ns") {
        (&s[..len-2], "ns")
    } else if s.ends_with("us") {
        (&s[..len-2], "us")
    } else if s.ends_with("ms") {
        (&s[..len-2], "ms")
    } else {
        (&s[..len-1], &s[len-1..])
    };

    let num: u64 = num_str.parse().unwrap_or(0);

    match unit {
        "ns" => num,
        "us" => num * 1_000,
        "ms" => num * 1_000_000,
        "s" => num * 1_000_000_000,
        "m" => num * 60 * 1_000_000_000,
        "h" => num * 3600 * 1_000_000_000,
        "d" => num * 86400 * 1_000_000_000,
        _ => 0,
    }
}

fn parse_timestamp(s: &str) -> i64 {
    // Remove @ prefix and parse ISO8601
    // For MVP, return 0
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stream_from() {
        let result = parse("stream Trades from TradeEvent");
        assert!(result.is_ok());
        let program = result.unwrap();
        assert_eq!(program.statements.len(), 1);
    }

    #[test]
    fn test_parse_stream_with_where() {
        let result = parse("stream HighValue = Trades.where(price > 1000)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_event_decl() {
        let result = parse("event Trade: symbol: str price: float volume: int");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_let() {
        let result = parse("let x = 42");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_lambda() {
        let result = parse("let f = x => x * 2");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_array() {
        let result = parse("let arr = [1, 2, 3]");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_map() {
        let result = parse(r#"let m = {"key": "value"}"#);
        assert!(result.is_ok());
    }
}
