//! Pest-based parser for VarpulisQL
//!
//! This module provides parsing using the pest PEG parser generator.

use pest::Parser;
use pest_derive::Parser;

use crate::error::{ParseError, ParseResult};
use crate::helpers::{parse_duration, parse_timestamp};
use crate::indent::preprocess_indentation;
use varpulis_core::ast::*;
use varpulis_core::span::{Span, Spanned};
use varpulis_core::types::Type;

#[derive(Parser)]
#[grammar = "varpulis.pest"]
pub struct VarpulisParser;

/// Parse a VarpulisQL source string into a Program AST
pub fn parse(source: &str) -> ParseResult<Program> {
    // Preprocess to add INDENT/DEDENT markers
    let preprocessed = preprocess_indentation(source);

    let pairs = VarpulisParser::parse(Rule::program, &preprocessed).map_err(convert_pest_error)?;

    let mut statements = Vec::new();

    for pair in pairs {
        if pair.as_rule() == Rule::program {
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::statement {
                    statements.push(parse_statement(inner)?);
                }
            }
        }
    }

    Ok(Program { statements })
}

fn convert_pest_error(e: pest::error::Error<Rule>) -> ParseError {
    let position = match e.location {
        pest::error::InputLocation::Pos(p) => p,
        pest::error::InputLocation::Span((s, _)) => s,
    };

    // Extract line/column from pest error
    let (line, column) = match e.line_col {
        pest::error::LineColLocation::Pos((l, c)) => (l, c),
        pest::error::LineColLocation::Span((l, c), _) => (l, c),
    };

    // Create a human-readable message based on what was expected
    let message = match &e.variant {
        pest::error::ErrorVariant::ParsingError {
            positives,
            negatives: _,
        } => {
            if positives.is_empty() {
                "Unexpected token".to_string()
            } else {
                let expected: Vec<String> = positives.iter().map(format_rule_name).collect();
                if expected.len() == 1 {
                    format!("Expected {}", expected[0])
                } else {
                    format!("Expected one of: {}", expected.join(", "))
                }
            }
        }
        pest::error::ErrorVariant::CustomError { message } => message.clone(),
    };

    ParseError::Located {
        line,
        column,
        position,
        message,
        hint: None,
    }
}

/// Convert pest Rule names to human-readable format
fn format_rule_name(rule: &Rule) -> String {
    match rule {
        Rule::identifier => "identifier".to_string(),
        Rule::integer => "number".to_string(),
        Rule::float => "number".to_string(),
        Rule::string => "string".to_string(),
        Rule::primitive_type => "type (int, float, bool, str, timestamp, duration)".to_string(),
        Rule::type_expr => "type".to_string(),
        Rule::expr => "expression".to_string(),
        Rule::statement => "statement".to_string(),
        Rule::stream_decl => "stream declaration".to_string(),
        Rule::event_decl => "event declaration".to_string(),
        Rule::fn_decl => "function declaration".to_string(),
        Rule::INDENT => "indented block".to_string(),
        Rule::DEDENT => "end of block".to_string(),
        Rule::field => "field declaration (name: type)".to_string(),
        Rule::comparison_op => "comparison operator (==, !=, <, >, <=, >=)".to_string(),
        Rule::additive_op => "operator (+, -)".to_string(),
        Rule::multiplicative_op => "operator (*, /, %)".to_string(),
        Rule::postfix_suffix => "method call or member access".to_string(),
        _ => format!("{:?}", rule).to_lowercase().replace('_', " "),
    }
}

fn parse_statement(pair: pest::iterators::Pair<Rule>) -> ParseResult<Spanned<Stmt>> {
    let span = Span::new(pair.as_span().start(), pair.as_span().end());
    let inner = pair.into_inner().next().unwrap();

    let stmt = match inner.as_rule() {
        Rule::stream_decl => parse_stream_decl(inner)?,
        Rule::event_decl => parse_event_decl(inner)?,
        Rule::type_decl => parse_type_decl(inner)?,
        Rule::var_decl => parse_var_decl(inner)?,
        Rule::const_decl => parse_const_decl(inner)?,
        Rule::fn_decl => parse_fn_decl(inner)?,
        Rule::config_block => parse_config_block(inner)?,
        Rule::import_stmt => parse_import_stmt(inner)?,
        Rule::if_stmt => parse_if_stmt(inner)?,
        Rule::for_stmt => parse_for_stmt(inner)?,
        Rule::while_stmt => parse_while_stmt(inner)?,
        Rule::return_stmt => parse_return_stmt(inner)?,
        Rule::break_stmt => Stmt::Break,
        Rule::continue_stmt => Stmt::Continue,
        Rule::expr_stmt => Stmt::Expr(parse_expr(inner.into_inner().next().unwrap())?),
        _ => {
            return Err(ParseError::UnexpectedToken {
                position: span.start,
                expected: "statement".to_string(),
                found: format!("{:?}", inner.as_rule()),
            })
        }
    };

    Ok(Spanned::new(stmt, span))
}

fn parse_stream_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut type_annotation = None;
    let mut source = StreamSource::Ident("".to_string());
    let mut ops = Vec::new();

    for p in inner {
        match p.as_rule() {
            Rule::type_annotation => {
                type_annotation = Some(parse_type(p.into_inner().next().unwrap())?);
            }
            Rule::identifier => {
                source = StreamSource::From(p.as_str().to_string());
            }
            Rule::stream_expr => {
                let (s, o) = parse_stream_expr(p)?;
                source = s;
                ops = o;
            }
            _ => {}
        }
    }

    Ok(Stmt::StreamDecl {
        name,
        type_annotation,
        source,
        ops,
    })
}

fn parse_stream_expr(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<(StreamSource, Vec<StreamOp>)> {
    let mut inner = pair.into_inner();
    let source = parse_stream_source(inner.next().unwrap())?;
    let mut ops = Vec::new();

    for p in inner {
        if p.as_rule() == Rule::stream_op {
            ops.push(parse_stream_op(p)?);
        }
    }

    Ok((source, ops))
}

fn parse_stream_source(pair: pest::iterators::Pair<Rule>) -> ParseResult<StreamSource> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::merge_source => {
            let mut streams = Vec::new();
            for p in inner.into_inner() {
                if p.as_rule() == Rule::inline_stream_list {
                    for is in p.into_inner() {
                        streams.push(parse_inline_stream(is)?);
                    }
                }
            }
            Ok(StreamSource::Merge(streams))
        }
        Rule::join_source => {
            let mut clauses = Vec::new();
            for p in inner.into_inner() {
                if p.as_rule() == Rule::join_clause_list {
                    for jc in p.into_inner() {
                        clauses.push(parse_join_clause(jc)?);
                    }
                }
            }
            Ok(StreamSource::Join(clauses))
        }
        Rule::sequence_source => {
            let decl = parse_sequence_decl(inner.into_inner().next().unwrap())?;
            Ok(StreamSource::Sequence(decl))
        }
        Rule::all_source => {
            let mut inner_iter = inner.into_inner();
            let name = inner_iter.next().unwrap().as_str().to_string();
            let alias = inner_iter.next().map(|p| p.as_str().to_string());
            Ok(StreamSource::AllWithAlias { name, alias })
        }
        Rule::aliased_source => {
            let mut inner_iter = inner.into_inner();
            let name = inner_iter.next().unwrap().as_str().to_string();
            let alias = inner_iter.next().unwrap().as_str().to_string();
            Ok(StreamSource::IdentWithAlias { name, alias })
        }
        Rule::identifier => Ok(StreamSource::Ident(inner.as_str().to_string())),
        _ => Err(ParseError::UnexpectedToken {
            position: 0,
            expected: "stream source".to_string(),
            found: format!("{:?}", inner.as_rule()),
        }),
    }
}

fn parse_inline_stream(pair: pest::iterators::Pair<Rule>) -> ParseResult<InlineStreamDecl> {
    let mut inner = pair.into_inner();

    // Check if it's a simple identifier or full declaration
    let first = inner.next().unwrap();
    if first.as_rule() == Rule::identifier && inner.clone().next().is_none() {
        let name = first.as_str().to_string();
        return Ok(InlineStreamDecl {
            name: name.clone(),
            source: name,
            filter: None,
        });
    }

    let name = first.as_str().to_string();
    let source = inner.next().unwrap().as_str().to_string();
    let filter = inner.next().map(|p| parse_expr(p)).transpose()?;

    Ok(InlineStreamDecl {
        name,
        source,
        filter,
    })
}

fn parse_join_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<JoinClause> {
    let mut inner = pair.into_inner();

    let first = inner.next().unwrap();
    if first.as_rule() == Rule::identifier && inner.clone().next().is_none() {
        let name = first.as_str().to_string();
        return Ok(JoinClause {
            name: name.clone(),
            source: name,
            on: None,
        });
    }

    let name = first.as_str().to_string();
    let source = inner.next().unwrap().as_str().to_string();
    let on = inner.next().map(|p| parse_expr(p)).transpose()?;

    Ok(JoinClause { name, source, on })
}

fn parse_sequence_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<SequenceDecl> {
    let mut steps = Vec::new();

    for p in pair.into_inner() {
        if p.as_rule() == Rule::sequence_step {
            steps.push(parse_sequence_step(p)?);
        }
    }

    Ok(SequenceDecl {
        match_all: false,
        timeout: None,
        steps,
    })
}

fn parse_sequence_step(pair: pest::iterators::Pair<Rule>) -> ParseResult<SequenceStepDecl> {
    let mut inner = pair.into_inner();
    let alias = inner.next().unwrap().as_str().to_string();
    let event_type = inner.next().unwrap().as_str().to_string();

    let mut filter = None;
    let mut timeout = None;

    for p in inner {
        match p.as_rule() {
            Rule::or_expr => filter = Some(parse_expr(p)?),
            Rule::within_suffix => {
                let expr = p.into_inner().next().unwrap();
                timeout = Some(Box::new(parse_expr(expr)?));
            }
            _ => {}
        }
    }

    Ok(SequenceStepDecl {
        alias,
        event_type,
        filter,
        timeout,
    })
}

fn parse_stream_op(pair: pest::iterators::Pair<Rule>) -> ParseResult<StreamOp> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::dot_op => {
            let op_inner = inner.into_inner().next().unwrap();
            parse_dot_op(op_inner)
        }
        Rule::followed_by_op => parse_followed_by_op(inner),
        _ => Err(ParseError::UnexpectedToken {
            position: 0,
            expected: "stream operation".to_string(),
            found: format!("{:?}", inner.as_rule()),
        }),
    }
}

fn parse_dot_op(pair: pest::iterators::Pair<Rule>) -> ParseResult<StreamOp> {
    match pair.as_rule() {
        Rule::where_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::Where(expr))
        }
        Rule::select_op => {
            let mut items = Vec::new();
            for p in pair.into_inner() {
                if p.as_rule() == Rule::select_list {
                    for si in p.into_inner() {
                        items.push(parse_select_item(si)?);
                    }
                }
            }
            Ok(StreamOp::Select(items))
        }
        Rule::window_op => {
            let args = parse_window_args(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::Window(args))
        }
        Rule::aggregate_op => {
            let mut items = Vec::new();
            for p in pair.into_inner() {
                if p.as_rule() == Rule::agg_list {
                    for ai in p.into_inner() {
                        items.push(parse_agg_item(ai)?);
                    }
                }
            }
            Ok(StreamOp::Aggregate(items))
        }
        Rule::map_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::Map(expr))
        }
        Rule::filter_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::Filter(expr))
        }
        Rule::within_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::Within(expr))
        }
        Rule::emit_op => {
            let args = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::named_arg_list)
                .flat_map(|p| p.into_inner())
                .map(parse_named_arg)
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(StreamOp::Emit(args))
        }
        Rule::print_op => {
            let exprs = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::expr_list)
                .flat_map(|p| p.into_inner())
                .map(parse_expr)
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(StreamOp::Print(exprs))
        }
        Rule::collect_op => Ok(StreamOp::Collect),
        Rule::pattern_op => {
            let def_pair = pair.into_inner().next().unwrap();
            let mut inner = def_pair.into_inner();
            let name = inner.next().unwrap().as_str().to_string();
            let body_pair = inner.next().unwrap();

            // pattern_body can be lambda_expr or pattern_or_expr
            let body_inner = body_pair.into_inner().next().unwrap();
            let matcher = match body_inner.as_rule() {
                Rule::lambda_expr => parse_lambda_expr(body_inner)?,
                Rule::pattern_or_expr => parse_pattern_expr_as_expr(body_inner)?,
                _ => parse_expr_inner(body_inner)?,
            };
            Ok(StreamOp::Pattern(PatternDef { name, matcher }))
        }
        Rule::attention_window_op => {
            let args = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::named_arg_list)
                .flat_map(|p| p.into_inner())
                .map(parse_named_arg)
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(StreamOp::AttentionWindow(args))
        }
        Rule::partition_by_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::PartitionBy(expr))
        }
        Rule::order_by_op => {
            let mut items = Vec::new();
            for p in pair.into_inner() {
                if p.as_rule() == Rule::order_list {
                    for oi in p.into_inner() {
                        items.push(parse_order_item(oi)?);
                    }
                }
            }
            Ok(StreamOp::OrderBy(items))
        }
        Rule::limit_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::Limit(expr))
        }
        Rule::distinct_op => {
            let expr = pair.into_inner().next().map(parse_expr).transpose()?;
            Ok(StreamOp::Distinct(expr))
        }
        Rule::tap_op => {
            let args = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::named_arg_list)
                .flat_map(|p| p.into_inner())
                .map(parse_named_arg)
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(StreamOp::Tap(args))
        }
        Rule::log_op => {
            let args = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::named_arg_list)
                .flat_map(|p| p.into_inner())
                .map(parse_named_arg)
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(StreamOp::Log(args))
        }
        Rule::to_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::To(expr))
        }
        Rule::process_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::Process(expr))
        }
        Rule::on_error_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::OnError(expr))
        }
        Rule::on_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(StreamOp::On(expr))
        }
        Rule::not_op => {
            let mut inner = pair.into_inner();
            let event_type = inner.next().unwrap().as_str().to_string();
            let filter = inner.next().map(parse_expr).transpose()?;
            Ok(StreamOp::Not(FollowedByClause {
                event_type,
                filter,
                alias: None,
                match_all: false,
            }))
        }
        Rule::fork_op => {
            let mut paths = Vec::new();
            for p in pair.into_inner() {
                if p.as_rule() == Rule::fork_path_list {
                    for fp in p.into_inner() {
                        paths.push(parse_fork_path(fp)?);
                    }
                }
            }
            Ok(StreamOp::Fork(paths))
        }
        Rule::any_op => {
            let count = pair
                .into_inner()
                .next()
                .map(|p| p.as_str().parse().unwrap_or(1));
            Ok(StreamOp::Any(count))
        }
        Rule::all_op => Ok(StreamOp::All),
        Rule::first_op => Ok(StreamOp::First),
        Rule::concurrent_op => {
            let args = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::named_arg_list)
                .flat_map(|p| p.into_inner())
                .map(parse_named_arg)
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(StreamOp::Concurrent(args))
        }
        _ => Err(ParseError::UnexpectedToken {
            position: 0,
            expected: "stream operation".to_string(),
            found: format!("{:?}", pair.as_rule()),
        }),
    }
}

fn parse_order_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<OrderItem> {
    let mut inner = pair.into_inner();
    let expr = parse_expr(inner.next().unwrap())?;
    let desc = inner.next().map(|p| p.as_str() == "desc").unwrap_or(false);
    Ok(OrderItem {
        expr,
        descending: desc,
    })
}

fn parse_fork_path(pair: pest::iterators::Pair<Rule>) -> ParseResult<ForkPath> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let mut ops = Vec::new();
    for p in inner {
        if p.as_rule() == Rule::stream_op {
            ops.push(parse_stream_op(p)?);
        }
    }
    Ok(ForkPath { name, ops })
}

fn parse_followed_by_op(pair: pest::iterators::Pair<Rule>) -> ParseResult<StreamOp> {
    let mut inner = pair.into_inner();
    let mut match_all = false;

    let first = inner.next().unwrap();
    let event_type = if first.as_rule() == Rule::match_all_keyword {
        match_all = true;
        inner.next().unwrap().as_str().to_string()
    } else {
        first.as_str().to_string()
    };

    let mut filter = None;
    let mut alias = None;

    for p in inner {
        match p.as_rule() {
            Rule::or_expr => filter = Some(parse_or_expr(p)?),
            Rule::filter_expr => filter = Some(parse_filter_expr(p)?),
            Rule::identifier => alias = Some(p.as_str().to_string()),
            _ => {}
        }
    }

    Ok(StreamOp::FollowedBy(FollowedByClause {
        event_type,
        filter,
        alias,
        match_all,
    }))
}

fn parse_select_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<SelectItem> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if let Some(second) = inner.next() {
        Ok(SelectItem::Alias(
            first.as_str().to_string(),
            parse_expr(second)?,
        ))
    } else {
        Ok(SelectItem::Field(first.as_str().to_string()))
    }
}

fn parse_window_args(pair: pest::iterators::Pair<Rule>) -> ParseResult<WindowArgs> {
    let mut inner = pair.into_inner();
    let duration = parse_expr(inner.next().unwrap())?;

    let mut sliding = None;
    let mut policy = None;

    for p in inner {
        if p.as_rule() == Rule::expr {
            // Need to determine if it's sliding or policy based on context
            if sliding.is_none() {
                sliding = Some(parse_expr(p)?);
            } else {
                policy = Some(parse_expr(p)?);
            }
        }
    }

    Ok(WindowArgs {
        duration,
        sliding,
        policy,
    })
}

fn parse_agg_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<AggItem> {
    let mut inner = pair.into_inner();
    let alias = inner.next().unwrap().as_str().to_string();
    let expr = parse_expr(inner.next().unwrap())?;
    Ok(AggItem { alias, expr })
}

fn parse_named_arg(pair: pest::iterators::Pair<Rule>) -> ParseResult<NamedArg> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let value = parse_expr(inner.next().unwrap())?;
    Ok(NamedArg { name, value })
}

fn parse_event_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut extends = None;
    let mut fields = Vec::new();

    for p in inner {
        match p.as_rule() {
            Rule::identifier => extends = Some(p.as_str().to_string()),
            Rule::field => fields.push(parse_field(p)?),
            _ => {}
        }
    }

    Ok(Stmt::EventDecl {
        name,
        extends,
        fields,
    })
}

fn parse_field(pair: pest::iterators::Pair<Rule>) -> ParseResult<Field> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let ty = parse_type(inner.next().unwrap())?;
    let optional = inner.next().is_some();
    Ok(Field { name, ty, optional })
}

fn parse_type_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let ty = parse_type(inner.next().unwrap())?;
    Ok(Stmt::TypeDecl { name, ty })
}

fn parse_type(pair: pest::iterators::Pair<Rule>) -> ParseResult<Type> {
    let cloned = pair.clone();
    let inner = pair.into_inner().next().unwrap_or(cloned);

    match inner.as_rule() {
        Rule::primitive_type => match inner.as_str() {
            "int" => Ok(Type::Int),
            "float" => Ok(Type::Float),
            "bool" => Ok(Type::Bool),
            "str" => Ok(Type::Str),
            "timestamp" => Ok(Type::Timestamp),
            "duration" => Ok(Type::Duration),
            _ => Ok(Type::Named(inner.as_str().to_string())),
        },
        Rule::array_type => {
            let inner_type = parse_type(inner.into_inner().next().unwrap())?;
            Ok(Type::Array(Box::new(inner_type)))
        }
        Rule::named_type | Rule::identifier => Ok(Type::Named(inner.as_str().to_string())),
        _ => Ok(Type::Named(inner.as_str().to_string())),
    }
}

fn parse_var_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let mutable = inner.next().unwrap().as_str() == "var";
    let name = inner.next().unwrap().as_str().to_string();

    let mut ty = None;
    let mut value = Expr::Null;

    for p in inner {
        match p.as_rule() {
            Rule::type_annotation => ty = Some(parse_type(p.into_inner().next().unwrap())?),
            _ => value = parse_expr(p)?,
        }
    }

    Ok(Stmt::VarDecl {
        mutable,
        name,
        ty,
        value,
    })
}

fn parse_const_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut ty = None;
    let mut value = Expr::Null;

    for p in inner {
        match p.as_rule() {
            Rule::type_annotation => ty = Some(parse_type(p.into_inner().next().unwrap())?),
            _ => value = parse_expr(p)?,
        }
    }

    Ok(Stmt::ConstDecl { name, ty, value })
}

fn parse_fn_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut params = Vec::new();
    let mut ret = None;
    let mut body = Vec::new();

    for p in inner {
        match p.as_rule() {
            Rule::param_list => {
                for param in p.into_inner() {
                    params.push(parse_param(param)?);
                }
            }
            Rule::type_expr => ret = Some(parse_type(p)?),
            Rule::block => body = parse_block(p)?,
            Rule::statement => body.push(parse_statement(p)?),
            _ => {}
        }
    }

    Ok(Stmt::FnDecl {
        name,
        params,
        ret,
        body,
    })
}

fn parse_block(pair: pest::iterators::Pair<Rule>) -> ParseResult<Vec<Spanned<Stmt>>> {
    let mut statements = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::statement {
            statements.push(parse_statement(p)?);
        }
    }
    Ok(statements)
}

fn parse_param(pair: pest::iterators::Pair<Rule>) -> ParseResult<Param> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let ty = parse_type(inner.next().unwrap())?;
    Ok(Param { name, ty })
}

fn parse_config_block(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    // Check if first token is identifier (new syntax) or config_item (old syntax)
    let (name, items_start) = if first.as_rule() == Rule::identifier {
        (first.as_str().to_string(), None)
    } else {
        // Old syntax: config: with indentation - use "default" as name
        ("default".to_string(), Some(first))
    };

    let mut items = Vec::new();

    // If we have a config_item from old syntax, parse it first
    if let Some(first_item) = items_start {
        if first_item.as_rule() == Rule::config_item {
            items.push(parse_config_item(first_item)?);
        }
    }

    for p in inner {
        if p.as_rule() == Rule::config_item {
            items.push(parse_config_item(p)?);
        }
    }
    Ok(Stmt::Config { name, items })
}

fn parse_config_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<ConfigItem> {
    let mut inner = pair.into_inner();
    let key = inner.next().unwrap().as_str().to_string();
    let value = parse_config_value(inner.next().unwrap())?;
    Ok(ConfigItem::Value(key, value))
}

fn parse_config_value(pair: pest::iterators::Pair<Rule>) -> ParseResult<ConfigValue> {
    let cloned = pair.clone();
    let inner = pair.into_inner().next().unwrap_or(cloned);

    match inner.as_rule() {
        Rule::integer => Ok(ConfigValue::Int(inner.as_str().parse().unwrap_or(0))),
        Rule::float => Ok(ConfigValue::Float(inner.as_str().parse().unwrap_or(0.0))),
        Rule::string => {
            let s = inner.as_str();
            Ok(ConfigValue::Str(s[1..s.len() - 1].to_string()))
        }
        Rule::duration => Ok(ConfigValue::Duration(parse_duration(inner.as_str()))),
        Rule::boolean => Ok(ConfigValue::Bool(inner.as_str() == "true")),
        Rule::identifier => Ok(ConfigValue::Ident(inner.as_str().to_string())),
        _ => Ok(ConfigValue::Ident(inner.as_str().to_string())),
    }
}

fn parse_import_stmt(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let path_pair = inner.next().unwrap();
    let path = path_pair.as_str();
    let path = path[1..path.len() - 1].to_string();
    let alias = inner.next().map(|p| p.as_str().to_string());
    Ok(Stmt::Import { path, alias })
}

fn parse_if_stmt(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let cond = parse_expr(inner.next().unwrap())?;

    let mut then_branch = Vec::new();
    let mut elif_branches = Vec::new();
    let mut else_branch = None;

    for p in inner {
        match p.as_rule() {
            Rule::block => then_branch = parse_block(p)?,
            Rule::statement => then_branch.push(parse_statement(p)?),
            Rule::elif_clause => {
                let mut elif_inner = p.into_inner();
                let elif_cond = parse_expr(elif_inner.next().unwrap())?;
                let mut elif_body = Vec::new();
                for ep in elif_inner {
                    match ep.as_rule() {
                        Rule::block => elif_body = parse_block(ep)?,
                        Rule::statement => elif_body.push(parse_statement(ep)?),
                        _ => {}
                    }
                }
                elif_branches.push((elif_cond, elif_body));
            }
            Rule::else_clause => {
                let mut else_body = Vec::new();
                for ep in p.into_inner() {
                    match ep.as_rule() {
                        Rule::block => else_body = parse_block(ep)?,
                        Rule::statement => else_body.push(parse_statement(ep)?),
                        _ => {}
                    }
                }
                else_branch = Some(else_body);
            }
            _ => {}
        }
    }

    Ok(Stmt::If {
        cond,
        then_branch,
        elif_branches,
        else_branch,
    })
}

fn parse_for_stmt(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let var = inner.next().unwrap().as_str().to_string();
    let iter = parse_expr(inner.next().unwrap())?;
    let mut body = Vec::new();
    for p in inner {
        match p.as_rule() {
            Rule::block => body = parse_block(p)?,
            Rule::statement => body.push(parse_statement(p)?),
            _ => {}
        }
    }
    Ok(Stmt::For { var, iter, body })
}

fn parse_while_stmt(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let mut inner = pair.into_inner();
    let cond = parse_expr(inner.next().unwrap())?;
    let mut body = Vec::new();
    for p in inner {
        match p.as_rule() {
            Rule::block => body = parse_block(p)?,
            Rule::statement => body.push(parse_statement(p)?),
            _ => {}
        }
    }
    Ok(Stmt::While { cond, body })
}

fn parse_return_stmt(pair: pest::iterators::Pair<Rule>) -> ParseResult<Stmt> {
    let expr = pair.into_inner().next().map(parse_expr).transpose()?;
    Ok(Stmt::Return(expr))
}

// ============================================================================
// Expression Parsing
// ============================================================================

fn parse_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let inner = pair.into_inner().next();

    match inner {
        Some(p) => parse_expr_inner(p),
        None => Ok(Expr::Null),
    }
}

fn parse_expr_inner(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    match pair.as_rule() {
        Rule::expr => parse_expr(pair),
        Rule::lambda_expr => parse_lambda_expr(pair),
        Rule::or_expr => parse_or_expr(pair),
        Rule::and_expr => parse_and_expr(pair),
        Rule::not_expr => parse_not_expr(pair),
        Rule::comparison_expr => parse_comparison_expr(pair),
        Rule::bitwise_or_expr => parse_bitwise_or_expr(pair),
        Rule::bitwise_xor_expr => parse_bitwise_xor_expr(pair),
        Rule::bitwise_and_expr => parse_bitwise_and_expr(pair),
        Rule::shift_expr => parse_shift_expr(pair),
        Rule::additive_expr => parse_additive_expr(pair),
        Rule::multiplicative_expr => parse_multiplicative_expr(pair),
        Rule::power_expr => parse_power_expr(pair),
        Rule::unary_expr => parse_unary_expr(pair),
        Rule::postfix_expr => parse_postfix_expr(pair),
        Rule::primary_expr => parse_primary_expr(pair),
        Rule::literal => parse_literal(pair),
        Rule::identifier => Ok(Expr::Ident(pair.as_str().to_string())),
        Rule::if_expr => parse_if_expr(pair),
        _ => Ok(Expr::Ident(pair.as_str().to_string())),
    }
}

fn parse_lambda_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut params = Vec::new();

    // Parse parameters
    let first = inner.next().unwrap();
    match first.as_rule() {
        Rule::identifier_list => {
            for p in first.into_inner() {
                params.push(p.as_str().to_string());
            }
        }
        Rule::identifier => {
            params.push(first.as_str().to_string());
        }
        _ => {}
    }

    // Parse body - could be expression or block
    let body_pair = inner.next().unwrap();
    let body = match body_pair.as_rule() {
        Rule::lambda_block => parse_lambda_block(body_pair)?,
        _ => parse_expr_inner(body_pair)?,
    };

    Ok(Expr::Lambda {
        params,
        body: Box::new(body),
    })
}

fn parse_lambda_block(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut stmts = Vec::new();
    let mut final_expr = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::statement => {
                // Check if it's a var_decl to extract for Block
                let stmt = parse_statement(p)?;
                match &stmt.node {
                    Stmt::VarDecl {
                        mutable,
                        name,
                        ty,
                        value,
                    } => {
                        stmts.push((name.clone(), ty.clone(), value.clone(), *mutable));
                    }
                    Stmt::Expr(e) => {
                        // Last expression becomes result
                        final_expr = Some(e.clone());
                    }
                    _ => {
                        // Other statements - treat as expression if possible
                    }
                }
            }
            _ => {
                // Expression at end of block
                final_expr = Some(parse_expr_inner(p)?);
            }
        }
    }

    // If we have variable declarations, wrap in a Block expression
    if stmts.is_empty() {
        Ok(final_expr.unwrap_or(Expr::Null))
    } else {
        Ok(Expr::Block {
            stmts,
            result: Box::new(final_expr.unwrap_or(Expr::Null)),
        })
    }
}

fn parse_pattern_expr_as_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    // Convert pattern_or_expr to an Expr representation
    // pattern_or_expr = pattern_and_expr ~ ("or" ~ pattern_and_expr)*
    let mut inner = pair.into_inner();
    let mut left = parse_pattern_and_as_expr(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_pattern_and_as_expr(right_pair)?;
        left = Expr::Binary {
            op: BinOp::Or,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_pattern_and_as_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_pattern_xor_as_expr(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_pattern_xor_as_expr(right_pair)?;
        left = Expr::Binary {
            op: BinOp::And,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_pattern_xor_as_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_pattern_unary_as_expr(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_pattern_unary_as_expr(right_pair)?;
        left = Expr::Binary {
            op: BinOp::Xor,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_pattern_unary_as_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if first.as_str() == "not" {
        let expr = parse_pattern_primary_as_expr(inner.next().unwrap())?;
        Ok(Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        })
    } else {
        parse_pattern_primary_as_expr(first)
    }
}

fn parse_pattern_primary_as_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::pattern_or_expr => parse_pattern_expr_as_expr(inner),
        Rule::pattern_sequence => parse_pattern_sequence_as_expr(inner),
        _ => Ok(Expr::Ident(inner.as_str().to_string())),
    }
}

fn parse_pattern_sequence_as_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    // pattern_sequence = identifier ~ ("->" ~ identifier)*
    // Convert to a chain of FollowedBy binary operations
    let mut inner = pair.into_inner();
    let mut left = Expr::Ident(inner.next().unwrap().as_str().to_string());

    for right_pair in inner {
        let right = Expr::Ident(right_pair.as_str().to_string());
        left = Expr::Binary {
            op: BinOp::FollowedBy,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_filter_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let inner = pair.into_inner().next().unwrap();
    parse_filter_or_expr(inner)
}

fn parse_filter_or_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_filter_and_expr(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_filter_and_expr(right_pair)?;
        left = Expr::Binary {
            op: BinOp::Or,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_filter_and_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_filter_not_expr(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_filter_not_expr(right_pair)?;
        left = Expr::Binary {
            op: BinOp::And,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_filter_not_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if first.as_str() == "not" {
        let expr = parse_filter_comparison_expr(inner.next().unwrap())?;
        Ok(Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        })
    } else {
        parse_filter_comparison_expr(first)
    }
}

fn parse_filter_comparison_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let left = parse_filter_additive_expr(inner.next().unwrap())?;

    if let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "==" => BinOp::Eq,
            "!=" => BinOp::NotEq,
            "<" => BinOp::Lt,
            "<=" => BinOp::Le,
            ">" => BinOp::Gt,
            ">=" => BinOp::Ge,
            "in" => BinOp::In,
            "is" => BinOp::Is,
            _ => BinOp::Eq,
        };
        let right = parse_filter_additive_expr(inner.next().unwrap())?;
        Ok(Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_filter_additive_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_filter_multiplicative_expr(inner.next().unwrap())?;

    while let Some(op_pair) = inner.next() {
        let op = if op_pair.as_str() == "-" {
            BinOp::Sub
        } else {
            BinOp::Add
        };
        if let Some(right_pair) = inner.next() {
            let right = parse_filter_multiplicative_expr(right_pair)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }
    Ok(left)
}

fn parse_filter_multiplicative_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_filter_unary_expr(inner.next().unwrap())?;

    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "*" => BinOp::Mul,
            "/" => BinOp::Div,
            "%" => BinOp::Mod,
            _ => BinOp::Mul,
        };
        if let Some(right_pair) = inner.next() {
            let right = parse_filter_unary_expr(right_pair)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }
    Ok(left)
}

fn parse_filter_unary_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if first.as_str() == "-" {
        let expr = parse_filter_postfix_expr(inner.next().unwrap())?;
        Ok(Expr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(expr),
        })
    } else if first.as_str() == "~" {
        let expr = parse_filter_postfix_expr(inner.next().unwrap())?;
        Ok(Expr::Unary {
            op: UnaryOp::BitNot,
            expr: Box::new(expr),
        })
    } else {
        parse_filter_postfix_expr(first)
    }
}

fn parse_filter_postfix_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut expr = parse_filter_primary_expr(inner.next().unwrap())?;

    for suffix in inner {
        expr = parse_filter_postfix_suffix(expr, suffix)?;
    }
    Ok(expr)
}

fn parse_filter_postfix_suffix(expr: Expr, pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();

    if let Some(first) = inner.next() {
        match first.as_rule() {
            Rule::identifier => {
                // Member access: .identifier
                Ok(Expr::Member {
                    expr: Box::new(expr),
                    member: first.as_str().to_string(),
                })
            }
            Rule::optional_member_access => {
                let member = first.into_inner().next().unwrap().as_str().to_string();
                Ok(Expr::OptionalMember {
                    expr: Box::new(expr),
                    member,
                })
            }
            Rule::index_access => {
                let index = parse_expr(first.into_inner().next().unwrap())?;
                Ok(Expr::Index {
                    expr: Box::new(expr),
                    index: Box::new(index),
                })
            }
            Rule::call_args => {
                let args = first
                    .into_inner()
                    .filter(|p| p.as_rule() == Rule::arg_list)
                    .flat_map(|p| p.into_inner())
                    .map(parse_arg)
                    .collect::<ParseResult<Vec<_>>>()?;
                Ok(Expr::Call {
                    func: Box::new(expr),
                    args,
                })
            }
            _ => Ok(expr),
        }
    } else {
        Ok(expr)
    }
}

fn parse_filter_primary_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::literal => parse_literal(inner),
        Rule::identifier => Ok(Expr::Ident(inner.as_str().to_string())),
        Rule::filter_expr => parse_filter_expr(inner),
        _ => Ok(Expr::Ident(inner.as_str().to_string())),
    }
}

fn parse_or_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_expr_inner(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_expr_inner(right_pair)?;
        left = Expr::Binary {
            op: BinOp::Or,
            left: Box::new(left),
            right: Box::new(right),
        };
    }

    Ok(left)
}

fn parse_and_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_expr_inner(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_expr_inner(right_pair)?;
        left = Expr::Binary {
            op: BinOp::And,
            left: Box::new(left),
            right: Box::new(right),
        };
    }

    Ok(left)
}

fn parse_not_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if first.as_str() == "not" {
        let expr = parse_expr_inner(inner.next().unwrap())?;
        Ok(Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        })
    } else {
        parse_expr_inner(first)
    }
}

fn parse_comparison_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let left = parse_expr_inner(inner.next().unwrap())?;

    if let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "==" => BinOp::Eq,
            "!=" => BinOp::NotEq,
            "<" => BinOp::Lt,
            "<=" => BinOp::Le,
            ">" => BinOp::Gt,
            ">=" => BinOp::Ge,
            "in" => BinOp::In,
            "is" => BinOp::Is,
            _ => BinOp::Eq,
        };
        let right = parse_expr_inner(inner.next().unwrap())?;
        Ok(Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_bitwise_or_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    parse_binary_chain(pair, BinOp::BitOr)
}

fn parse_bitwise_xor_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    parse_binary_chain(pair, BinOp::BitXor)
}

fn parse_bitwise_and_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    parse_binary_chain(pair, BinOp::BitAnd)
}

fn parse_shift_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_expr_inner(inner.next().unwrap())?;

    while let Some(op_or_expr) = inner.next() {
        let op = match op_or_expr.as_str() {
            "<<" => BinOp::Shl,
            ">>" => BinOp::Shr,
            _ => {
                let right = parse_expr_inner(op_or_expr)?;
                left = Expr::Binary {
                    op: BinOp::Shl,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                continue;
            }
        };
        if let Some(right_pair) = inner.next() {
            let right = parse_expr_inner(right_pair)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    Ok(left)
}

fn parse_additive_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_expr_inner(inner.next().unwrap())?;

    while let Some(op_pair) = inner.next() {
        let op_text = op_pair.as_str();
        let op = if op_text == "-" {
            BinOp::Sub
        } else {
            BinOp::Add
        };

        if let Some(right_pair) = inner.next() {
            let right = parse_expr_inner(right_pair)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    Ok(left)
}

fn parse_multiplicative_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_expr_inner(inner.next().unwrap())?;

    while let Some(op_pair) = inner.next() {
        let op_text = op_pair.as_str();
        let op = match op_text {
            "*" => BinOp::Mul,
            "/" => BinOp::Div,
            "%" => BinOp::Mod,
            _ => BinOp::Mul,
        };

        if let Some(right_pair) = inner.next() {
            let right = parse_expr_inner(right_pair)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    Ok(left)
}

fn parse_power_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let base = parse_expr_inner(inner.next().unwrap())?;

    if let Some(exp_pair) = inner.next() {
        let exp = parse_expr_inner(exp_pair)?;
        Ok(Expr::Binary {
            op: BinOp::Pow,
            left: Box::new(base),
            right: Box::new(exp),
        })
    } else {
        Ok(base)
    }
}

fn parse_unary_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    match first.as_str() {
        "-" => {
            let expr = parse_expr_inner(inner.next().unwrap())?;
            Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            })
        }
        "~" => {
            let expr = parse_expr_inner(inner.next().unwrap())?;
            Ok(Expr::Unary {
                op: UnaryOp::BitNot,
                expr: Box::new(expr),
            })
        }
        _ => parse_expr_inner(first),
    }
}

fn parse_postfix_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut expr = parse_expr_inner(inner.next().unwrap())?;

    for suffix in inner {
        expr = parse_postfix_suffix(expr, suffix)?;
    }

    Ok(expr)
}

fn parse_postfix_suffix(expr: Expr, pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::member_access => {
            let member = inner.into_inner().next().unwrap().as_str().to_string();
            Ok(Expr::Member {
                expr: Box::new(expr),
                member,
            })
        }
        Rule::optional_member_access => {
            let member = inner.into_inner().next().unwrap().as_str().to_string();
            Ok(Expr::OptionalMember {
                expr: Box::new(expr),
                member,
            })
        }
        Rule::index_access => {
            let index = parse_expr(inner.into_inner().next().unwrap())?;
            Ok(Expr::Index {
                expr: Box::new(expr),
                index: Box::new(index),
            })
        }
        Rule::call_args => {
            let mut args = Vec::new();
            for p in inner.into_inner() {
                if p.as_rule() == Rule::arg_list {
                    for arg in p.into_inner() {
                        args.push(parse_arg(arg)?);
                    }
                }
            }
            Ok(Expr::Call {
                func: Box::new(expr),
                args,
            })
        }
        _ => Ok(expr),
    }
}

fn parse_arg(pair: pest::iterators::Pair<Rule>) -> ParseResult<Arg> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if let Some(second) = inner.next() {
        Ok(Arg::Named(
            first.as_str().to_string(),
            parse_expr_inner(second)?,
        ))
    } else {
        Ok(Arg::Positional(parse_expr_inner(first)?))
    }
}

fn parse_primary_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::if_expr => parse_if_expr(inner),
        Rule::literal => parse_literal(inner),
        Rule::identifier => Ok(Expr::Ident(inner.as_str().to_string())),
        Rule::array_literal => parse_array_literal(inner),
        Rule::map_literal => parse_map_literal(inner),
        Rule::expr => parse_expr(inner),
        _ => Ok(Expr::Ident(inner.as_str().to_string())),
    }
}

fn parse_if_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let cond = parse_expr_inner(inner.next().unwrap())?;
    let then_branch = parse_expr_inner(inner.next().unwrap())?;
    let else_branch = parse_expr_inner(inner.next().unwrap())?;

    Ok(Expr::If {
        cond: Box::new(cond),
        then_branch: Box::new(then_branch),
        else_branch: Box::new(else_branch),
    })
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::integer => Ok(Expr::Int(inner.as_str().parse().unwrap_or(0))),
        Rule::float => Ok(Expr::Float(inner.as_str().parse().unwrap_or(0.0))),
        Rule::string => {
            let s = inner.as_str();
            Ok(Expr::Str(s[1..s.len() - 1].to_string()))
        }
        Rule::duration => Ok(Expr::Duration(parse_duration(inner.as_str()))),
        Rule::timestamp => Ok(Expr::Timestamp(parse_timestamp(inner.as_str()))),
        Rule::boolean => Ok(Expr::Bool(inner.as_str() == "true")),
        Rule::null => Ok(Expr::Null),
        _ => Ok(Expr::Null),
    }
}

fn parse_array_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut items = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::expr_list {
            for expr in p.into_inner() {
                items.push(parse_expr(expr)?);
            }
        }
    }
    Ok(Expr::Array(items))
}

fn parse_map_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expr> {
    let mut entries = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::map_entry_list {
            for entry in p.into_inner() {
                let mut inner = entry.into_inner();
                let key = inner.next().unwrap().as_str().to_string();
                let key = if key.starts_with('"') {
                    key[1..key.len() - 1].to_string()
                } else {
                    key
                };
                let value = parse_expr(inner.next().unwrap())?;
                entries.push((key, value));
            }
        }
    }
    Ok(Expr::Map(entries))
}

fn parse_binary_chain(pair: pest::iterators::Pair<Rule>, op: BinOp) -> ParseResult<Expr> {
    let mut inner = pair.into_inner();
    let mut left = parse_expr_inner(inner.next().unwrap())?;

    for right_pair in inner {
        let right = parse_expr_inner(right_pair)?;
        left = Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        };
    }

    Ok(left)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_stream() {
        let result = parse("stream output from input");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_stream_with_filter() {
        let result = parse("stream output = input.where(value > 100)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_stream_with_map() {
        let result = parse("stream output = input.map(x * 2)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_event_declaration() {
        let result = parse("event SensorReading:\n  sensor_id: str\n  value: float");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_variable() {
        let result = parse("let x = 42");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_function() {
        let result = parse("fn add(a: int, b: int) -> int:\n  return a + b");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_lambda() {
        let result = parse("let f = (x) => x * 2");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_if_expression() {
        let result = parse("let x = if a > b then a else b");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_followed_by() {
        let result = parse("stream alerts = orders.where(amount > 1000) -> Payment where payment.order_id == orders.id");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_window() {
        let result = parse("stream windowed = input.window(5s)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_aggregate() {
        let result =
            parse("stream stats = input.window(1m).aggregate(count: count(), avg: avg(value))");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_merge() {
        let result = parse("stream combined = merge(stream1, stream2)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_sequence() {
        let result = parse("stream seq = sequence(a: EventA, b: EventB where b.id == a.id)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_config() {
        let result = parse("config:\n  window_size: 5s\n  batch_size: 100");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_complex_expression() {
        let result = parse("let x = (a + b) * c / d - e");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_sliding_window() {
        let result = parse("stream output = input.window(5m, sliding: 1m)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_fork_construct() {
        let result =
            parse("stream forked = input.fork(branch1: .where(x > 0), branch2: .where(x < 0))");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_aggregate_functions() {
        let result = parse(
            "stream stats = input.window(1h).aggregate(total: sum(value), average: avg(value))",
        );
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_complex_parentheses() {
        let result = parse("let x = ((a + b) * (c - d)) / e");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_sequence_with_alias() {
        let result = parse(
            r#"
            stream TwoTicks = StockTick as first
                -> StockTick as second
                .emit(result: "two_ticks")
        "#,
        );
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_followed_by_with_alias() {
        let result = parse("stream alerts = Order as a -> Payment as b");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_followed_by_with_filter_and_alias() {
        // This is the problematic case: 'as b' should be the alias, not part of the expression
        let result = parse(
            r#"
            stream Test = A as a
                -> B where value == a.base + 10 as b
                .emit(status: "matched")
        "#,
        );
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_pattern_with_lambda() {
        let result =
            parse("stream Test = Trade.window(1m).pattern(p: x => x.len() > 3).emit(alert: true)");
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }
}
