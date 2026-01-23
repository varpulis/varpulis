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

        loop {
            if self.match_token(&Token::Dot) {
                ops.push(self.parse_stream_op()?);
            } else if self.match_token(&Token::Arrow) {
                ops.push(self.parse_followed_by()?);
            } else {
                break;
            }
        }

        Ok((source, ops))
    }

    /// Parse a followed-by clause: `-> EventType where condition as alias`
    fn parse_followed_by(&mut self) -> ParseResult<StreamOp> {
        // Check for .not() after ->
        if self.match_token(&Token::Dot) {
            let op_name = self.parse_stream_op_name()?;
            if op_name == "not" {
                return self.parse_not_clause();
            }
            // If it's not "not", we need to handle it differently
            return Err(ParseError::UnexpectedToken {
                position: self.current.start,
                expected: "not or event type".to_string(),
                found: format!("{}", op_name),
            });
        }

        // Check for 'all' quantifier
        let match_all = self.match_token(&Token::All);

        // Parse event type
        let event_type = self.parse_identifier()?;

        // Parse optional filter: `where condition`
        let filter = if self.match_token(&Token::Where) {
            Some(self.parse_followed_by_filter()?)
        } else {
            None
        };

        // Parse optional alias: `as name`
        let alias = if self.match_token(&Token::As) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(StreamOp::FollowedBy(FollowedByClause {
            event_type,
            filter,
            alias,
            match_all,
        }))
    }

    /// Parse the filter expression for followed-by, stopping at 'as', '->', or '.'
    fn parse_followed_by_filter(&mut self) -> ParseResult<Expr> {
        // Parse expression but need to handle 'as' specially since it's used for alias
        // We use parse_or_expr directly to avoid issues with 'as' being consumed
        self.parse_or_expr()
    }

    /// Parse .not(EventType where condition)
    fn parse_fork_clause(&mut self) -> ParseResult<StreamOp> {
        self.consume(&Token::LParen, "(")?;
        let mut paths = Vec::new();
        
        loop {
            if self.check(&Token::RParen) {
                break;
            }
            
            // Parse: path_name: -> EventType where cond .within(timeout)
            let name = self.parse_identifier()?;
            self.consume(&Token::Colon, ":")?;
            
            // Parse the sequence of operations for this path
            let mut ops = Vec::new();
            
            // Must start with ->
            if self.match_token(&Token::Arrow) {
                ops.push(self.parse_followed_by()?);
            }
            
            // Continue parsing operations until comma or rparen
            while self.check(&Token::Dot) && !self.check(&Token::RParen) {
                if self.is_stream_op_after_dot() {
                    self.advance(); // consume dot
                    let op = self.parse_stream_op()?;
                    ops.push(op);
                } else {
                    break;
                }
            }
            
            paths.push(ForkPath { name, ops });
            
            if !self.match_token(&Token::Comma) {
                break;
            }
        }
        
        self.consume(&Token::RParen, ")")?;
        Ok(StreamOp::Fork(paths))
    }

    fn parse_not_clause(&mut self) -> ParseResult<StreamOp> {
        self.consume(&Token::LParen, "(")?;
        
        let event_type = self.parse_identifier()?;
        
        let filter = if self.match_token(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        self.consume(&Token::RParen, ")")?;

        Ok(StreamOp::Not(FollowedByClause {
            event_type,
            filter,
            alias: None,
            match_all: false,
        }))
    }

    fn parse_stream_source(&mut self) -> ParseResult<StreamSource> {
        if self.match_token(&Token::Merge) {
            self.consume(&Token::LParen, "(")?;
            let streams = self.parse_inline_stream_list()?;
            self.consume(&Token::RParen, ")")?;
            return Ok(StreamSource::Merge(streams));
        }

        // sequence(alias1: Event1, alias2: Event2 where cond, ...)
        if self.check_ident("sequence") {
            self.advance();
            self.consume(&Token::LParen, "(")?;
            let decl = self.parse_sequence_decl()?;
            self.consume(&Token::RParen, ")")?;
            return Ok(StreamSource::Sequence(decl));
        }

        if self.match_token(&Token::Join) {
            self.consume(&Token::LParen, "(")?;
            let clauses = self.parse_join_clause_list()?;
            self.consume(&Token::RParen, ")")?;
            return Ok(StreamSource::Join(clauses));
        }

        // Handle 'all' quantifier: `all EventType as alias`
        let match_all = self.match_token(&Token::All);

        let name = self.parse_identifier()?;
        
        // Handle optional alias: `EventType as alias`
        let alias = if self.match_token(&Token::As) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        if match_all {
            Ok(StreamSource::AllWithAlias { name, alias })
        } else if let Some(alias) = alias {
            Ok(StreamSource::IdentWithAlias { name, alias })
        } else {
            Ok(StreamSource::Ident(name))
        }
    }

    /// Check if current token is a specific identifier
    fn check_ident(&self, name: &str) -> bool {
        matches!(&self.current.token, Token::Ident(s) if s == name)
    }

    /// Parse sequence declaration: alias1: Event1, alias2: Event2 where cond, ...
    fn parse_sequence_decl(&mut self) -> ParseResult<SequenceDecl> {
        let mut steps = Vec::new();
        let mut match_all = false;
        let mut timeout = None;

        // Check for optional mode: "all" or "one"
        if self.check_ident("mode") {
            self.advance();
            self.consume(&Token::Colon, ":")?;
            if let Token::String(mode) = self.current.token.clone() {
                match_all = mode == "all";
                self.advance();
            }
            if self.check(&Token::Comma) {
                self.advance();
            }
        }

        // Check for optional timeout
        if self.check_ident("timeout") {
            self.advance();
            self.consume(&Token::Colon, ":")?;
            timeout = Some(Box::new(self.parse_expr()?));
            if self.check(&Token::Comma) {
                self.advance();
            }
        }

        // Parse steps
        loop {
            if self.check(&Token::RParen) {
                break;
            }

            // Parse: alias: EventType where condition .within(timeout)
            let alias = self.parse_identifier()?;
            self.consume(&Token::Colon, ":")?;
            let event_type = self.parse_identifier()?;

            let filter = if self.match_token(&Token::Where) {
                Some(self.parse_or_expr()?)
            } else {
                None
            };

            let step_timeout = if self.check(&Token::Dot) && self.is_stream_op_after_dot() {
                // Check for .within()
                self.advance(); // consume dot
                if self.check_ident("within") || matches!(self.current.token, Token::Within) {
                    self.advance();
                    self.consume(&Token::LParen, "(")?;
                    let t = self.parse_expr()?;
                    self.consume(&Token::RParen, ")")?;
                    Some(Box::new(t))
                } else {
                    None
                }
            } else {
                None
            };

            steps.push(SequenceStepDecl {
                alias,
                event_type,
                filter,
                timeout: step_timeout,
            });

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        Ok(SequenceDecl {
            match_all,
            timeout,
            steps,
        })
    }

    fn parse_inline_stream_list(&mut self) -> ParseResult<Vec<InlineStreamDecl>> {
        let mut streams = vec![self.parse_inline_stream()?];
        while self.match_token(&Token::Comma) {
            streams.push(self.parse_inline_stream()?);
        }
        Ok(streams)
    }

    fn parse_inline_stream(&mut self) -> ParseResult<InlineStreamDecl> {
        // Support both syntaxes:
        // 1. merge(stream X from Y, ...) - full syntax
        // 2. merge(X, Y, Z) - simplified syntax with existing stream names
        if self.check(&Token::Stream) {
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
        } else {
            // Simplified syntax: just a stream name reference
            let name = self.parse_identifier()?;
            Ok(InlineStreamDecl { name: name.clone(), source: name, filter: None })
        }
    }

    fn parse_join_clause_list(&mut self) -> ParseResult<Vec<JoinClause>> {
        let mut clauses = vec![self.parse_join_clause()?];
        while self.match_token(&Token::Comma) {
            clauses.push(self.parse_join_clause()?);
        }
        Ok(clauses)
    }

    fn parse_join_clause(&mut self) -> ParseResult<JoinClause> {
        // Support both syntaxes:
        // 1. join(stream X from Y, stream Z from W) - full syntax
        // 2. join(X, Y, Z) - simplified syntax with existing stream names
        if self.check(&Token::Stream) {
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
        } else {
            // Simplified syntax: just a stream name reference
            let name = self.parse_identifier()?;
            Ok(JoinClause { name: name.clone(), source: name, on: None })
        }
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
            "on" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::On(expr))
            }
            "within" => {
                self.consume(&Token::LParen, "(")?;
                let expr = self.parse_expr()?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Within(expr))
            }
            "not" => {
                self.parse_not_clause()
            }
            "fork" => {
                self.parse_fork_clause()
            }
            "any" => {
                self.consume(&Token::LParen, "(")?;
                let count = if !self.check(&Token::RParen) {
                    if let Token::Integer(n) = self.current.token {
                        self.advance();
                        Some(n as usize)
                    } else {
                        None
                    }
                } else {
                    None
                };
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::Any(count))
            }
            "all" => {
                self.consume(&Token::LParen, "(")?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::All)
            }
            "first" => {
                self.consume(&Token::LParen, "(")?;
                self.consume(&Token::RParen, ")")?;
                Ok(StreamOp::First)
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
        // Parse a full expression (supports both simple func(arg) and complex expressions like sum(x) / 4)
        let expr = self.parse_expr()?;
        Ok(AggItem { alias, expr })
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
        let name = self.parse_identifier_or_keyword()?;
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

    /// Continue parsing binary expression given an already-parsed left side
    fn continue_binary_expr(&mut self, left: Expr) -> ParseResult<Expr> {
        self.continue_or_expr(left)
    }

    fn continue_or_expr(&mut self, mut left: Expr) -> ParseResult<Expr> {
        // First continue with and-level and below
        left = self.continue_and_expr(left)?;
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

    fn continue_and_expr(&mut self, mut left: Expr) -> ParseResult<Expr> {
        left = self.continue_comparison_expr(left)?;
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

    fn continue_comparison_expr(&mut self, left: Expr) -> ParseResult<Expr> {
        let mut left = self.continue_additive_expr(left)?;
        let op = match &self.current.token {
            Token::EqEq => Some(BinOp::Eq),
            Token::NotEq => Some(BinOp::NotEq),
            Token::Lt => Some(BinOp::Lt),
            Token::Le => Some(BinOp::Le),
            Token::Gt => Some(BinOp::Gt),
            Token::Ge => Some(BinOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive_expr()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn continue_additive_expr(&mut self, mut left: Expr) -> ParseResult<Expr> {
        left = self.continue_multiplicative_expr(left)?;
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

    fn continue_multiplicative_expr(&mut self, mut left: Expr) -> ParseResult<Expr> {
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
            if self.check(&Token::Dot) {
                // Peek ahead to see if this is a stream operation (not member access)
                // Stream operations like .within(), .emit(), .where() should not be
                // consumed as member access
                if self.is_stream_op_after_dot() {
                    break;
                }
                self.advance(); // consume the dot
                // Allow keywords as member names (e.g., obj.all)
                let member = self.parse_identifier_or_keyword()?;
                
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
                let member = self.parse_identifier_or_keyword()?;
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
            // Allow certain keywords as function names in expressions
            Token::AttentionScore => {
                self.advance();
                Ok(Expr::Ident("attention_score".to_string()))
            }
            // $ for previous event reference in sequences
            Token::Dollar => {
                self.advance();
                Ok(Expr::Ident("$".to_string()))
            }
            Token::LParen => {
                self.advance();
                
                // Check for multi-param lambda: (a, b) => expr
                // We need to look ahead to see if this is a lambda parameter list
                if let Token::Ident(_) = &self.current.token {
                    // Save position to potentially backtrack
                    let saved_pos = self.current.start;
                    let first_ident = self.parse_identifier()?;
                    
                    // Check for multi-param lambda: (a, b) => ...
                    if self.check(&Token::Comma) {
                        // Peek ahead to see if all items are identifiers followed by )
                        // For simplicity, try to parse as lambda params
                        let mut params = vec![first_ident.clone()];
                        let mut is_lambda = true;
                        
                        while self.match_token(&Token::Comma) {
                            if let Token::Ident(_) = &self.current.token {
                                params.push(self.parse_identifier()?);
                            } else {
                                is_lambda = false;
                                break;
                            }
                        }
                        
                        if is_lambda && self.match_token(&Token::RParen) {
                            if self.match_token(&Token::FatArrow) {
                                let body = self.parse_expr()?;
                                return Ok(Expr::Lambda {
                                    params,
                                    body: Box::new(body),
                                });
                            }
                        }
                        // Not a valid lambda - this is a parse error for tuples
                        return Err(ParseError::UnexpectedToken {
                            position: self.current.start,
                            expected: "=> for lambda".to_string(),
                            found: format!("{}", self.current.token),
                        });
                    } else if self.match_token(&Token::RParen) {
                        // Single param lambda: (x) => expr, or just grouped ident (x)
                        if self.match_token(&Token::FatArrow) {
                            let body = self.parse_expr()?;
                            return Ok(Expr::Lambda {
                                params: vec![first_ident],
                                body: Box::new(body),
                            });
                        }
                        // It was just (ident) - return the identifier
                        return Ok(Expr::Ident(first_ident));
                    } else {
                        // It's a grouped expression starting with identifier
                        // Could be (x + y), (func(a).b), etc.
                        // Build the initial expression and continue with postfix/binary
                        let mut expr = Expr::Ident(first_ident);
                        
                        // Handle postfix operations (function calls, member access)
                        loop {
                            if self.match_token(&Token::Dot) {
                                let member = self.parse_identifier()?;
                                if self.check(&Token::LParen) {
                                    // Method call
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
                            } else if self.match_token(&Token::LParen) {
                                // Function call
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
                            } else if self.match_token(&Token::LBracket) {
                                let index = self.parse_expr()?;
                                self.consume(&Token::RBracket, "]")?;
                                expr = Expr::Index {
                                    expr: Box::new(expr),
                                    index: Box::new(index),
                                };
                            } else {
                                break;
                            }
                        }
                        
                        // Now continue with binary operators
                        let expr = self.continue_binary_expr(expr)?;
                        self.consume(&Token::RParen, ")")?;
                        return Ok(expr);
                    }
                }
                
                // Not starting with identifier - parse full expression
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
                
                // Check if this is a block expression (starts with let/var) or a map literal
                if self.check(&Token::Let) || self.check(&Token::Var) {
                    // Block expression: { let a = 1; let b = 2; a + b }
                    let mut stmts = Vec::new();
                    
                    while self.check(&Token::Let) || self.check(&Token::Var) {
                        let is_mutable = self.check(&Token::Var);
                        self.advance(); // consume let/var
                        let name = self.parse_identifier()?;
                        let ty = if self.match_token(&Token::Colon) {
                            Some(self.parse_type()?)
                        } else {
                            None
                        };
                        self.consume(&Token::Eq, "=")?;
                        let value = self.parse_expr()?;
                        stmts.push((name, ty, value, is_mutable));
                    }
                    
                    // The final expression
                    let result = self.parse_expr()?;
                    self.consume(&Token::RBrace, "}")?;
                    
                    Ok(Expr::Block {
                        stmts,
                        result: Box::new(result),
                    })
                } else if self.check(&Token::RBrace) {
                    // Empty map
                    self.advance();
                    Ok(Expr::Map(Vec::new()))
                } else {
                    // Map literal: { "key": value, ... }
                    let mut entries = Vec::new();
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
                    self.consume(&Token::RBrace, "}")?;
                    Ok(Expr::Map(entries))
                }
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
        // Check for named argument (identifier followed by colon, but not ::)
        if let Token::Ident(_) = &self.current.token {
            // Peek ahead to check for named arg pattern: ident ':'
            // But we need to be careful - just parse the whole expression
            // and check if it's a named arg pattern
        }
        
        // Parse as a full expression
        let expr = self.parse_expr()?;
        Ok(Arg::Positional(expr))
    }

    /// Check if the token after the current dot is a stream operation keyword
    fn is_stream_op_after_dot(&mut self) -> bool {
        if !self.check(&Token::Dot) {
            return false;
        }
        // Peek at the token after the dot
        if let Some(next) = self.lexer.peek() {
            Self::is_stream_op_token(&next.token)
        } else {
            false
        }
    }

    /// Check if a token is a stream operation keyword
    fn is_stream_op_token(token: &Token) -> bool {
        matches!(token, 
            Token::Where | Token::Select | Token::Window | Token::Aggregate |
            Token::PartitionBy | Token::OrderBy | Token::Limit | Token::Distinct |
            Token::Emit | Token::To | Token::Pattern | Token::AttentionWindow |
            Token::On | Token::Within | Token::Not
        )
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

    /// Parse identifier or allow certain keywords as identifiers (for named args, member access)
    fn parse_identifier_or_keyword(&mut self) -> ParseResult<String> {
        let name = match &self.current.token {
            Token::Ident(name) => name.clone(),
            Token::DurationType => "duration".to_string(),
            Token::TimestampType => "timestamp".to_string(),
            Token::IntType => "int".to_string(),
            Token::FloatType => "float".to_string(),
            Token::BoolType => "bool".to_string(),
            Token::StrType => "str".to_string(),
            Token::Pattern => "pattern".to_string(),
            Token::Window => "window".to_string(),
            Token::All => "all".to_string(),
            _ => {
                return Err(ParseError::UnexpectedToken {
                    position: self.current.start,
                    expected: "identifier".to_string(),
                    found: format!("{}", self.current.token),
                });
            }
        };
        self.advance();
        Ok(name)
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
            Token::On => "on".to_string(),
            Token::Within => "within".to_string(),
            Token::Not => "not".to_string(),
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

    // ========================================================================
    // Basic Stream Tests
    // ========================================================================

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
    fn test_parse_stream_with_window() {
        let result = parse("stream Windowed = Source.window(5m)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_stream_with_aggregate() {
        let result = parse("stream Agg = Source.window(5m).aggregate(total: sum(value), count: count())");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_stream_partition_by() {
        let result = parse("stream Partitioned = Source.partition_by(zone)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_stream_select() {
        let result = parse("stream Selected = Source.select(id: id, value: price * 2)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_stream_emit() {
        let result = parse("stream Alerts = Source.where(value > 100).emit(alert_type: \"high\", severity: \"warning\")");
        assert!(result.is_ok());
    }

    // ========================================================================
    // Join Tests
    // ========================================================================

    #[test]
    fn test_parse_join_simple() {
        let result = parse("stream Joined = join(StreamA, StreamB).window(1m)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_join_with_on() {
        let result = parse("stream Joined = join(A, B).on(A.id == B.id).window(1m)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_join_with_select() {
        let result = parse(r#"
            stream Joined = join(A, B)
                .on(A.id == B.id)
                .window(1m)
                .select(
                    id: A.id,
                    val_a: A.value,
                    val_b: B.value
                )
        "#);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_join_multiple_streams() {
        let result = parse("stream Multi = join(A, B, C).window(1m)");
        assert!(result.is_ok());
    }

    // ========================================================================
    // Event Declaration Tests
    // ========================================================================

    #[test]
    fn test_parse_event_decl() {
        let result = parse("event Trade: symbol: str price: float volume: int");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_event_with_timestamp() {
        let result = parse("event Reading: sensor_id: str value: float ts: timestamp");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_multiple_events() {
        let result = parse(r#"
            event A: id: str
            event B: id: str value: float
        "#);
        assert!(result.is_ok());
        let program = result.unwrap();
        assert_eq!(program.statements.len(), 2);
    }

    // ========================================================================
    // Expression Tests
    // ========================================================================

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

    #[test]
    fn test_parse_binary_expr() {
        let result = parse("let x = a + b * c - d / e");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_comparison_expr() {
        let result = parse("let x = a > b and c <= d or e == f");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_function_call_with_expr() {
        let result = parse("let x = abs(value - 22)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_nested_function_calls() {
        let result = parse("let x = max(abs(a - b), abs(c - d))");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_member_access() {
        let result = parse("let x = obj.field.subfield");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_complex_parenthesized_expr() {
        let result = parse("let x = (last(events).price - first(events).price) / first(events).price");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_function_call_with_member_access() {
        let result = parse("let x = avg(events.volume) > 3 * baseline_volume(events[0].symbol)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_if_expr() {
        let result = parse("let x = if a > b then a else b");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_complex_if_expr() {
        let result = parse("let x = if a > 0 then a * 2 else if a < 0 then a * -1 else 0");
        assert!(result.is_ok());
    }

    // ========================================================================
    // Attention Window Tests
    // ========================================================================

    #[test]
    fn test_parse_attention_window() {
        let result = parse(r#"
            stream AttentionStream = Source
                .attention_window(duration: 1h, heads: 4, embedding: "rule_based")
        "#);
        assert!(result.is_ok());
    }

    // ========================================================================
    // Pattern Tests
    // ========================================================================

    #[test]
    fn test_parse_pattern_simple() {
        let result = parse(r#"
            stream PatternStream = Source
                .pattern(my_pattern: events => events.count() > 10)
        "#);
        assert!(result.is_ok());
    }

    // ========================================================================
    // Block Expression Tests (let in expressions)
    // ========================================================================

    #[test]
    fn test_parse_block_expr() {
        let result = parse(r#"
            let result = {
                let a = 1
                let b = 2
                a + b
            }
        "#);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_pattern_with_let() {
        let result = parse(r#"
            stream DegradationAlert = Source
                .pattern(
                    degradation: events => {
                        let values = events.map(e => e.value)
                        let trend = linear_slope(values)
                        trend < -0.1
                    }
                )
        "#);
        assert!(result.is_ok());
    }

    // ========================================================================
    // Complex Stream Pipeline Tests
    // ========================================================================

    #[test]
    fn test_parse_complex_pipeline() {
        let result = parse(r#"
            stream Result = Source
                .where(value > 0)
                .partition_by(zone)
                .window(5m)
                .aggregate(
                    zone: last(zone),
                    avg_value: avg(value),
                    max_value: max(value),
                    count: count()
                )
                .where(avg_value > 10)
                .emit(
                    alert_type: "threshold",
                    severity: "warning"
                )
        "#);
        assert!(result.is_ok());
    }

    // ========================================================================
    // Error Cases
    // ========================================================================

    #[test]
    fn test_parse_error_unclosed_paren() {
        let result = parse("stream X = Source.where(a > b");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_missing_from() {
        let result = parse("stream X TradeEvent");
        assert!(result.is_err());
    }

    // ========================================================================
    // Followed-By Sequence Tests (TDD)
    // ========================================================================

    #[test]
    fn test_parse_simple_followed_by() {
        let result = parse("stream S = A -> B");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            assert_eq!(ops.len(), 1);
            assert!(matches!(ops[0], StreamOp::FollowedBy(_)));
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_followed_by_chain() {
        let result = parse("stream S = A -> B -> C");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            assert_eq!(ops.len(), 2);
            assert!(matches!(ops[0], StreamOp::FollowedBy(_)));
            assert!(matches!(ops[1], StreamOp::FollowedBy(_)));
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_followed_by_with_filter() {
        let result = parse("stream S = A -> B where id == $.id");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            if let StreamOp::FollowedBy(clause) = &ops[0] {
                assert_eq!(clause.event_type, "B");
                assert!(clause.filter.is_some());
            } else {
                panic!("Expected FollowedBy");
            }
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_followed_by_with_alias() {
        let result = parse("stream S = A -> B as second");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            if let StreamOp::FollowedBy(clause) = &ops[0] {
                assert_eq!(clause.event_type, "B");
                assert_eq!(clause.alias, Some("second".to_string()));
            } else {
                panic!("Expected FollowedBy");
            }
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_followed_by_with_filter_and_alias() {
        let result = parse("stream S = A -> B where symbol == $.subject as tick");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            if let StreamOp::FollowedBy(clause) = &ops[0] {
                assert_eq!(clause.event_type, "B");
                assert!(clause.filter.is_some());
                assert_eq!(clause.alias, Some("tick".to_string()));
            } else {
                panic!("Expected FollowedBy");
            }
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_followed_by_with_within() {
        let result = parse("stream S = A -> B.within(5m)");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            assert_eq!(ops.len(), 2);
            assert!(matches!(ops[0], StreamOp::FollowedBy(_)));
            assert!(matches!(ops[1], StreamOp::Within(_)));
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_followed_by_all_quantifier() {
        let result = parse("stream S = all A -> B");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { source, ops, .. } = &program.statements[0].node {
            // The 'all' should be captured in the source or first element
            if let StreamSource::Ident(name) = source {
                assert_eq!(name, "A");
            }
            // First FollowedBy should exist
            assert!(matches!(ops[0], StreamOp::FollowedBy(_)));
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_fork_construct() {
        let source = r#"
            stream MultiPath = Order as order
                .fork(
                    payment: -> Payment where order_id == order.id .within(30m),
                    shipping: -> Shipping where order_id == order.id .within(2h)
                )
                .any()
                .emit(status: "completed")
        "#;
        let program = parse(source).expect("Failed to parse");
        assert_eq!(program.statements.len(), 1);
        
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            // Should have Fork, Any, Emit
            assert!(ops.iter().any(|op| matches!(op, StreamOp::Fork(_))));
            assert!(ops.iter().any(|op| matches!(op, StreamOp::Any(_))));
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_sequence_construct() {
        let source = r#"
            stream Correlation = sequence(
                news: NewsItem,
                tick: StockTick where symbol == news.subject,
                pump: StockTick where price >= tick.price * 1.05
            )
            .emit(status: "detected")
        "#;
        let program = parse(source).expect("Failed to parse");
        assert_eq!(program.statements.len(), 1);
        
        if let Stmt::StreamDecl { source, .. } = &program.statements[0].node {
            assert!(matches!(source, StreamSource::Sequence(_)));
            if let StreamSource::Sequence(decl) = source {
                assert_eq!(decl.steps.len(), 3);
                assert_eq!(decl.steps[0].alias, "news");
                assert_eq!(decl.steps[1].alias, "tick");
                assert_eq!(decl.steps[2].alias, "pump");
            }
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_followed_by_complex_sequence() {
        let result = parse(r#"
            stream PumpDetection = all NewsItem as news
                -> StockTick where symbol == news.subject as tick
                -> StockTick where symbol == news.subject and price >= tick.price * 1.05
                   .within(5m)
                .emit(alert_type: "PUMP")
        "#);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_followed_by_with_not() {
        let result = parse("stream S = A -> .not(B where id == $.id).within(30s)");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let program = result.unwrap();
        if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
            assert!(matches!(ops[0], StreamOp::Not(_)));
            assert!(matches!(ops[1], StreamOp::Within(_)));
        } else {
            panic!("Expected StreamDecl");
        }
    }

    #[test]
    fn test_parse_dollar_reference() {
        let result = parse("let x = $.field");
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    // ========================================================================
    // Additional Coverage Tests
    // ========================================================================

    #[test]
    fn test_parse_nested_binary_expr() {
        let result = parse("let x = (a + b) * (c - d)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_comparison_operators() {
        assert!(parse("let x = a < b").is_ok());
        assert!(parse("let x = a <= b").is_ok());
        assert!(parse("let x = a > b").is_ok());
        assert!(parse("let x = a >= b").is_ok());
        assert!(parse("let x = a == b").is_ok());
        assert!(parse("let x = a != b").is_ok());
    }

    #[test]
    fn test_parse_logical_operators() {
        assert!(parse("let x = a and b").is_ok());
        assert!(parse("let x = a or b").is_ok());
        assert!(parse("let x = not a").is_ok());
    }

    #[test]
    fn test_parse_unary_minus() {
        let result = parse("let x = -5");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_array_literal() {
        let result = parse("let arr = [1, 2, 3]");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_array_access() {
        let result = parse("let x = arr[0]");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_member_access_chain() {
        let result = parse("let x = a.b.c.d");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_function_call() {
        let result = parse("let x = max(a, b)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_function_call_no_args() {
        let result = parse("let x = now()");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_duration_literals() {
        assert!(parse("let d = 100ms").is_ok());
        assert!(parse("let d = 5s").is_ok());
        assert!(parse("let d = 10m").is_ok());
        assert!(parse("let d = 2h").is_ok());
        assert!(parse("let d = 1d").is_ok());
    }

    #[test]
    fn test_parse_string_escape() {
        let result = parse(r#"let s = "hello\nworld""#);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_sliding_window() {
        let result = parse("stream S = Source.window(5m, sliding: 1m)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_window_with_partition() {
        let result = parse("stream S = Source.partition_by(zone).window(5m)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_aggregate_functions() {
        let result = parse("stream S = Source.window(1m).aggregate(
            total: sum(value),
            average: avg(value),
            minimum: min(value),
            maximum: max(value),
            cnt: count(),
            std: stddev(value),
            first_val: first(value),
            last_val: last(value)
        )");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_merge() {
        let result = parse("stream S = merge(StreamA, StreamB)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_complex_filter() {
        let result = parse("stream S = Source.where(price > 100 and (status == \"active\" or priority >= 5))");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_boolean_literal() {
        assert!(parse("let t = true").is_ok());
        assert!(parse("let f = false").is_ok());
    }

    #[test]
    fn test_parse_null_literal() {
        let result = parse("let n = null");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_float_literal() {
        assert!(parse("let x = 3.14").is_ok());
        assert!(parse("let x = 0.5").is_ok());
        assert!(parse("let x = 100.0").is_ok());
    }

    #[test]
    fn test_parse_integer_literal() {
        assert!(parse("let x = 0").is_ok());
        assert!(parse("let x = 42").is_ok());
        assert!(parse("let x = 1000000").is_ok());
    }

    #[test]
    fn test_parse_source_alias() {
        let result = parse("stream S = Event as e -> Other .emit(x: e.id)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_chained_operations() {
        let result = parse("stream S = Source.where(x > 0).select(y: x * 2).emit(z: y)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_empty_emit() {
        let result = parse("stream S = A -> B .emit()");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_multiple_within() {
        let result = parse("stream S = A -> B.within(1m) -> C.within(2m)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_ema_aggregate() {
        let result = parse("stream S = Source.window(1h).aggregate(ema_val: ema(price, 12))");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_arithmetic_in_filter() {
        let result = parse("stream S = A -> B where value == a.base + 10 * 2");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_modulo_operator() {
        let result = parse("let x = a % b");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_division() {
        let result = parse("let x = a / b");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_all_in_followed_by() {
        let result = parse("stream S = A -> all B as b .emit(x: \"matched\")");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_nested_member_access_in_filter() {
        let result = parse("stream S = A as a -> B where data.nested.field == a.other.value");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    // ========================================================================
    // Error Cases
    // ========================================================================

    #[test]
    fn test_parse_error_unclosed_paren_nested() {
        let result = parse("let x = ((a + b)");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_unclosed_bracket_array() {
        let result = parse("let arr = [1, 2, 3");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_missing_rhs() {
        let result = parse("let x = a +");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_stream_no_name() {
        let result = parse("stream = Source");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_consecutive_dots() {
        let result = parse("stream S = Source..where(x > 0)");
        assert!(result.is_err());
    }
}
