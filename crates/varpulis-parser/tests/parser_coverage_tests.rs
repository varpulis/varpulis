//! Parser coverage tests targeting uncovered branches in pest_parser.rs
//!
//! These tests focus on less-common VPL syntax constructs, edge cases,
//! and branches that existing unit tests do not exercise.

use varpulis_core::ast::*;
use varpulis_core::types::Type;
use varpulis_parser::parse;

// ============================================================================
// Helper: extract the first statement from a parse result
// ============================================================================

fn parse_first_stmt(source: &str) -> Stmt {
    let program = parse(source).unwrap_or_else(|e| panic!("Parse failed: {:?}", e));
    assert!(
        !program.statements.is_empty(),
        "Expected at least one statement"
    );
    program.statements[0].node.clone()
}

// ============================================================================
// 1. Type Declarations
// ============================================================================

#[test]
fn test_type_decl_float() {
    let stmt = parse_first_stmt("type Temperature = float");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Temperature");
            assert_eq!(ty, Type::Float);
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_int() {
    let stmt = parse_first_stmt("type Counter = int");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Counter");
            assert_eq!(ty, Type::Int);
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_bool() {
    let stmt = parse_first_stmt("type Flag = bool");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Flag");
            assert_eq!(ty, Type::Bool);
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_str() {
    let stmt = parse_first_stmt("type Label = str");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Label");
            assert_eq!(ty, Type::Str);
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_timestamp() {
    let stmt = parse_first_stmt("type EventTime = timestamp");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "EventTime");
            assert_eq!(ty, Type::Timestamp);
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_duration() {
    let stmt = parse_first_stmt("type Interval = duration");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Interval");
            assert_eq!(ty, Type::Duration);
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_array() {
    let stmt = parse_first_stmt("type IntList = [int]");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "IntList");
            assert_eq!(ty, Type::Array(Box::new(Type::Int)));
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_map() {
    let stmt = parse_first_stmt("type Lookup = {str: int}");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Lookup");
            assert_eq!(ty, Type::Map(Box::new(Type::Str), Box::new(Type::Int)));
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_tuple() {
    let stmt = parse_first_stmt("type Pair = (int, float)");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Pair");
            assert_eq!(ty, Type::Tuple(vec![Type::Int, Type::Float]));
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_stream_type() {
    let stmt = parse_first_stmt("type EventStream = Stream<float>");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "EventStream");
            assert_eq!(ty, Type::Stream(Box::new(Type::Float)));
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_optional() {
    let stmt = parse_first_stmt("type MaybeInt = int?");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "MaybeInt");
            assert_eq!(ty, Type::Optional(Box::new(Type::Int)));
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

#[test]
fn test_type_decl_named_type() {
    let stmt = parse_first_stmt("type Alias = SensorReading");
    match stmt {
        Stmt::TypeDecl { name, ty } => {
            assert_eq!(name, "Alias");
            assert_eq!(ty, Type::Named("SensorReading".to_string()));
        }
        other => panic!("Expected TypeDecl, got {:?}", other),
    }
}

// ============================================================================
// 2. Const Declarations
// ============================================================================

#[test]
fn test_const_decl_int() {
    let stmt = parse_first_stmt("const MAX_TEMP = 100");
    match stmt {
        Stmt::ConstDecl { name, ty, value } => {
            assert_eq!(name, "MAX_TEMP");
            assert!(ty.is_none());
            assert_eq!(value, Expr::Int(100));
        }
        other => panic!("Expected ConstDecl, got {:?}", other),
    }
}

#[test]
fn test_const_decl_with_type_annotation() {
    let stmt = parse_first_stmt("const THRESHOLD: float = 99.5");
    match stmt {
        Stmt::ConstDecl { name, ty, value } => {
            assert_eq!(name, "THRESHOLD");
            assert_eq!(ty, Some(Type::Float));
            assert_eq!(value, Expr::Float(99.5));
        }
        other => panic!("Expected ConstDecl, got {:?}", other),
    }
}

#[test]
fn test_const_decl_string() {
    let stmt = parse_first_stmt(r#"const NAME = "hello""#);
    match stmt {
        Stmt::ConstDecl { name, value, .. } => {
            assert_eq!(name, "NAME");
            assert_eq!(value, Expr::Str("hello".to_string()));
        }
        other => panic!("Expected ConstDecl, got {:?}", other),
    }
}

#[test]
fn test_const_decl_bool() {
    let stmt = parse_first_stmt("const ENABLED = true");
    match stmt {
        Stmt::ConstDecl { name, value, .. } => {
            assert_eq!(name, "ENABLED");
            assert_eq!(value, Expr::Bool(true));
        }
        other => panic!("Expected ConstDecl, got {:?}", other),
    }
}

// ============================================================================
// 3. Function Declarations with Complex Bodies
// ============================================================================

#[test]
fn test_fn_decl_with_return_type() {
    let stmt = parse_first_stmt("fn calc(a: int, b: float) -> float:\n  return a + b");
    match stmt {
        Stmt::FnDecl {
            name,
            params,
            ret,
            body,
        } => {
            assert_eq!(name, "calc");
            assert_eq!(params.len(), 2);
            assert_eq!(params[0].name, "a");
            assert_eq!(params[0].ty, Type::Int);
            assert_eq!(params[1].name, "b");
            assert_eq!(params[1].ty, Type::Float);
            assert_eq!(ret, Some(Type::Float));
            assert!(!body.is_empty());
        }
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

#[test]
fn test_fn_decl_no_params_no_return_type() {
    let stmt = parse_first_stmt("fn do_nothing():\n  return");
    match stmt {
        Stmt::FnDecl {
            name,
            params,
            ret,
            body,
        } => {
            assert_eq!(name, "do_nothing");
            assert!(params.is_empty());
            assert!(ret.is_none());
            // Body should have a return statement with no value
            match &body[0].node {
                Stmt::Return(None) => {}
                other => panic!("Expected Return(None), got {:?}", other),
            }
        }
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

#[test]
fn test_fn_decl_with_multiple_statements() {
    let src = "fn compute(x: int) -> int:\n  let y = x * 2\n  return y + 1";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { name, body, .. } => {
            assert_eq!(name, "compute");
            assert_eq!(body.len(), 2);
            assert!(matches!(&body[0].node, Stmt::VarDecl { .. }));
            assert!(matches!(&body[1].node, Stmt::Return(Some(_))));
        }
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

// ============================================================================
// 4. Nested Expressions: Binary ops, unary ops, member access, array indexing
// ============================================================================

#[test]
fn test_binary_power_expression() {
    // Note: constant folding turns `2 ** 10` into `Int(1024)`, so use
    // non-constant operands to exercise the power operator parsing path.
    let stmt = parse_first_stmt("let x = a ** b");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Binary { op: BinOp::Pow, .. } => {}
            other => panic!("Expected Pow binary, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_unary_neg_expression() {
    // Negate in a where clause to exercise the unary path
    let result = parse("stream S = E.where(-x > 0)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_unary_bitnot_expression() {
    let result = parse("stream S = E.where(~flags == 0)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_unary_not_expression() {
    let result = parse("stream S = E.where(not active)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_member_access_expression() {
    let stmt = parse_first_stmt("let x = event.temperature");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Member { member, .. } => assert_eq!(member, "temperature"),
            other => panic!("Expected Member, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_optional_member_access() {
    let stmt = parse_first_stmt("let x = event?.temperature");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::OptionalMember { member, .. } => assert_eq!(member, "temperature"),
            other => panic!("Expected OptionalMember, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_index_access_expression() {
    let stmt = parse_first_stmt("let x = arr[0]");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Index { index, .. } => {
                assert_eq!(*index, Expr::Int(0));
            }
            other => panic!("Expected Index, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_slice_access_full() {
    let stmt = parse_first_stmt("let x = arr[1:3]");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Slice { start, end, .. } => {
                assert!(start.is_some());
                assert!(end.is_some());
            }
            other => panic!("Expected Slice, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_slice_access_open_start() {
    let stmt = parse_first_stmt("let x = arr[:3]");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Slice { start, end, .. } => {
                assert!(start.is_none());
                assert!(end.is_some());
            }
            other => panic!("Expected Slice, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_slice_access_open_end() {
    let stmt = parse_first_stmt("let x = arr[1:]");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Slice { start, end, .. } => {
                assert!(start.is_some());
                assert!(end.is_none());
            }
            other => panic!("Expected Slice, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_slice_access_both_open() {
    let stmt = parse_first_stmt("let x = arr[:]");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Slice { start, end, .. } => {
                assert!(start.is_none());
                assert!(end.is_none());
            }
            other => panic!("Expected Slice, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_call_expression() {
    let stmt = parse_first_stmt("let x = foo(1, 2, 3)");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Call { args, .. } => assert_eq!(args.len(), 3),
            other => panic!("Expected Call, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_call_with_named_args() {
    let stmt = parse_first_stmt("let x = foo(a: 1, b: 2)");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Call { args, .. } => {
                assert_eq!(args.len(), 2);
                match &args[0] {
                    Arg::Named(name, _) => assert_eq!(name, "a"),
                    other => panic!("Expected Named arg, got {:?}", other),
                }
            }
            other => panic!("Expected Call, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

// ============================================================================
// 5. Complex Stream Sources
// ============================================================================

#[test]
fn test_sequence_source_with_within() {
    let result = parse("stream S = sequence(a: EventA, b: EventB where b.id == a.id.within(30s))");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_aliased_source() {
    let stmt = parse_first_stmt("stream S = SensorReading as sr");
    match stmt {
        Stmt::StreamDecl { source, .. } => match source {
            StreamSource::IdentWithAlias { name, alias } => {
                assert_eq!(name, "SensorReading");
                assert_eq!(alias, "sr");
            }
            other => panic!("Expected IdentWithAlias, got {:?}", other),
        },
        other => panic!("Expected StreamDecl, got {:?}", other),
    }
}

#[test]
fn test_all_source_with_alias() {
    let stmt = parse_first_stmt("stream S = all SensorReading as sr");
    match stmt {
        Stmt::StreamDecl { source, .. } => match source {
            StreamSource::AllWithAlias { name, alias } => {
                assert_eq!(name, "SensorReading");
                assert_eq!(alias, Some("sr".to_string()));
            }
            other => panic!("Expected AllWithAlias, got {:?}", other),
        },
        other => panic!("Expected StreamDecl, got {:?}", other),
    }
}

#[test]
fn test_all_source_without_alias() {
    let stmt = parse_first_stmt("stream S = all SensorReading");
    match stmt {
        Stmt::StreamDecl { source, .. } => match source {
            StreamSource::AllWithAlias { name, alias } => {
                assert_eq!(name, "SensorReading");
                assert!(alias.is_none());
            }
            other => panic!("Expected AllWithAlias, got {:?}", other),
        },
        other => panic!("Expected StreamDecl, got {:?}", other),
    }
}

#[test]
fn test_followed_by_match_all() {
    let result = parse("stream S = A as a -> all B where b.id == a.id as b");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let fb = ops.iter().find(|op| matches!(op, StreamOp::FollowedBy(_)));
        assert!(fb.is_some(), "Expected FollowedBy op");
        if let StreamOp::FollowedBy(clause) = fb.unwrap() {
            assert!(clause.match_all);
            assert_eq!(clause.event_type, "B");
        }
    } else {
        panic!("Expected StreamDecl");
    }
}

#[test]
fn test_merge_source_with_inline_where() {
    let result = parse(
        r#"stream S = merge(
            stream high = Source.where(value > 100),
            stream low = Source.where(value < 10)
        )"#,
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { source, .. } = &program.statements[0].node {
        match source {
            StreamSource::Merge(streams) => {
                assert_eq!(streams.len(), 2);
                assert_eq!(streams[0].name, "high");
                assert_eq!(streams[0].source, "Source");
                assert!(streams[0].filter.is_some());
            }
            other => panic!("Expected Merge, got {:?}", other),
        }
    } else {
        panic!("Expected StreamDecl");
    }
}

#[test]
fn test_join_source_with_on_clause() {
    let result = parse(
        r#"stream S = join(
            stream orders = OrderEvent.on(order_id),
            stream payments = PaymentEvent.on(order_id)
        )"#,
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { source, .. } = &program.statements[0].node {
        match source {
            StreamSource::Join(clauses) => {
                assert_eq!(clauses.len(), 2);
                assert_eq!(clauses[0].name, "orders");
                assert!(clauses[0].on.is_some());
            }
            other => panic!("Expected Join, got {:?}", other),
        }
    } else {
        panic!("Expected StreamDecl");
    }
}

#[test]
fn test_join_source_simple_identifiers() {
    let result = parse("stream S = join(A, B)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { source, .. } = &program.statements[0].node {
        match source {
            StreamSource::Join(clauses) => {
                assert_eq!(clauses.len(), 2);
                assert_eq!(clauses[0].name, "A");
                assert!(clauses[0].on.is_none());
            }
            other => panic!("Expected Join, got {:?}", other),
        }
    } else {
        panic!("Expected StreamDecl");
    }
}

#[test]
fn test_timer_source_with_initial_delay() {
    let stmt = parse_first_stmt(r#"stream S = timer(5s, initial_delay: 1s).emit(type: "tick")"#);
    match stmt {
        Stmt::StreamDecl { source, .. } => match source {
            StreamSource::Timer(decl) => {
                assert!(decl.initial_delay.is_some());
            }
            other => panic!("Expected Timer, got {:?}", other),
        },
        other => panic!("Expected StreamDecl, got {:?}", other),
    }
}

// ============================================================================
// 6. Pattern Declarations (SASE+)
// ============================================================================

#[test]
fn test_pattern_decl_seq_kleene_star() {
    let stmt = parse_first_stmt("pattern P = SEQ(A, B*, C)");
    match stmt {
        Stmt::PatternDecl { name, expr, .. } => {
            assert_eq!(name, "P");
            match expr {
                SasePatternExpr::Seq(items) => {
                    assert_eq!(items.len(), 3);
                    assert_eq!(items[1].kleene, Some(KleeneOp::Star));
                }
                other => panic!("Expected Seq, got {:?}", other),
            }
        }
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_seq_optional() {
    let stmt = parse_first_stmt("pattern P = SEQ(A, B?, C)");
    match stmt {
        Stmt::PatternDecl { expr, .. } => match expr {
            SasePatternExpr::Seq(items) => {
                assert_eq!(items[1].kleene, Some(KleeneOp::Optional));
            }
            other => panic!("Expected Seq, got {:?}", other),
        },
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_with_within_and_partition() {
    let stmt = parse_first_stmt("pattern P = SEQ(A, B+, C?) within 30s partition by user_id");
    match stmt {
        Stmt::PatternDecl {
            within,
            partition_by,
            ..
        } => {
            assert!(within.is_some());
            // 30s = 30_000_000_000 ns
            assert_eq!(within.unwrap(), Expr::Duration(30_000_000_000));
            assert!(partition_by.is_some());
            assert_eq!(partition_by.unwrap(), Expr::Ident("user_id".to_string()));
        }
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_negated_item() {
    let stmt = parse_first_stmt("pattern P = SEQ(Login, NOT Logout, Transaction)");
    match stmt {
        Stmt::PatternDecl { expr, .. } => match expr {
            SasePatternExpr::Seq(items) => {
                assert_eq!(items.len(), 3);
                // Negated items are prefixed with "!"
                assert!(items[1].event_type.starts_with('!'));
            }
            other => panic!("Expected Seq, got {:?}", other),
        },
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_or() {
    let stmt = parse_first_stmt("pattern P = A OR B");
    match stmt {
        Stmt::PatternDecl { expr, .. } => match expr {
            SasePatternExpr::Or(_, _) => {}
            other => panic!("Expected Or, got {:?}", other),
        },
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_and() {
    let stmt = parse_first_stmt("pattern P = A AND B");
    match stmt {
        Stmt::PatternDecl { expr, .. } => match expr {
            SasePatternExpr::And(_, _) => {}
            other => panic!("Expected And, got {:?}", other),
        },
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_not_in_seq() {
    // Standalone `NOT A` at top level is limited by the grammar (NOT is consumed
    // silently as a literal). Use NOT inside a SEQ item instead, which produces
    // the "!" prefix on the event_type.
    let stmt = parse_first_stmt("pattern P = SEQ(Login, NOT Logout)");
    match stmt {
        Stmt::PatternDecl { expr, .. } => match expr {
            SasePatternExpr::Seq(items) => {
                assert_eq!(items.len(), 2);
                assert!(
                    items[1].event_type.starts_with('!'),
                    "Expected negated item"
                );
            }
            other => panic!("Expected Seq with negated item, got {:?}", other),
        },
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_grouped() {
    let stmt = parse_first_stmt("pattern P = (A OR B) AND C");
    match stmt {
        Stmt::PatternDecl { expr, .. } => match expr {
            SasePatternExpr::And(left, _right) => match *left {
                SasePatternExpr::Group(_) => {}
                other => panic!("Expected Group inside And, got {:?}", other),
            },
            other => panic!("Expected And with Group, got {:?}", other),
        },
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

#[test]
fn test_pattern_decl_event_with_kleene_as_sase_event_ref() {
    // Event ref with kleene, where, alias => should wrap in single-item Seq
    let stmt = parse_first_stmt("pattern P = Transaction+ where amount > 100 as txs");
    match stmt {
        Stmt::PatternDecl { expr, .. } => match expr {
            SasePatternExpr::Seq(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].kleene, Some(KleeneOp::Plus));
                assert!(items[0].filter.is_some());
                assert_eq!(items[0].alias, Some("txs".to_string()));
            }
            other => panic!("Expected single-item Seq, got {:?}", other),
        },
        other => panic!("Expected PatternDecl, got {:?}", other),
    }
}

// ============================================================================
// 7. Connector Declarations
// ============================================================================

#[test]
fn test_connector_mqtt() {
    let stmt = parse_first_stmt(r#"connector MyMqtt = mqtt(host: "localhost", port: 1883)"#);
    match stmt {
        Stmt::ConnectorDecl {
            name,
            connector_type,
            params,
        } => {
            assert_eq!(name, "MyMqtt");
            assert_eq!(connector_type, "mqtt");
            assert_eq!(params.len(), 2);
            assert_eq!(params[0].name, "host");
            assert_eq!(params[0].value, ConfigValue::Str("localhost".to_string()));
            assert_eq!(params[1].name, "port");
            assert_eq!(params[1].value, ConfigValue::Int(1883));
        }
        other => panic!("Expected ConnectorDecl, got {:?}", other),
    }
}

#[test]
fn test_connector_kafka_with_array() {
    let stmt =
        parse_first_stmt(r#"connector MyKafka = kafka(brokers: ["kafka1:9092", "kafka2:9092"])"#);
    match stmt {
        Stmt::ConnectorDecl { params, .. } => {
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name, "brokers");
            match &params[0].value {
                ConfigValue::Array(values) => {
                    assert_eq!(values.len(), 2);
                }
                other => panic!("Expected Array config value, got {:?}", other),
            }
        }
        other => panic!("Expected ConnectorDecl, got {:?}", other),
    }
}

#[test]
fn test_connector_no_params() {
    let stmt = parse_first_stmt("connector MyConn = websocket()");
    match stmt {
        Stmt::ConnectorDecl {
            name,
            connector_type,
            params,
        } => {
            assert_eq!(name, "MyConn");
            assert_eq!(connector_type, "websocket");
            assert!(params.is_empty());
        }
        other => panic!("Expected ConnectorDecl, got {:?}", other),
    }
}

#[test]
fn test_connector_with_boolean_param() {
    let stmt =
        parse_first_stmt(r#"connector MyConn = http(base_url: "http://localhost", ssl: true)"#);
    match stmt {
        Stmt::ConnectorDecl { params, .. } => {
            assert_eq!(params.len(), 2);
            assert_eq!(params[1].name, "ssl");
            assert_eq!(params[1].value, ConfigValue::Bool(true));
        }
        other => panic!("Expected ConnectorDecl, got {:?}", other),
    }
}

#[test]
fn test_connector_with_duration_param() {
    let stmt = parse_first_stmt(r#"connector MyConn = mqtt(host: "localhost", timeout: 30s)"#);
    match stmt {
        Stmt::ConnectorDecl { params, .. } => {
            assert_eq!(params[1].name, "timeout");
            assert_eq!(params[1].value, ConfigValue::Duration(30_000_000_000));
        }
        other => panic!("Expected ConnectorDecl, got {:?}", other),
    }
}

// ============================================================================
// 8. Window Types
// ============================================================================

#[test]
fn test_tumbling_window() {
    let result = parse("stream S = E.window(5s)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Window(args) = &ops[0] {
            assert!(args.sliding.is_none());
            assert!(args.session_gap.is_none());
        } else {
            panic!("Expected Window op");
        }
    }
}

#[test]
fn test_sliding_window() {
    let result = parse("stream S = E.window(5m, sliding: 1m)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Window(args) = &ops[0] {
            assert!(args.sliding.is_some());
        } else {
            panic!("Expected Window op");
        }
    }
}

#[test]
fn test_session_window() {
    let result = parse("stream S = E.window(session: 5m)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Window(args) = &ops[0] {
            assert!(args.session_gap.is_some());
        } else {
            panic!("Expected Window op");
        }
    }
}

#[test]
fn test_count_window() {
    let result = parse("stream S = E.window(100)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

// ============================================================================
// 9. Aggregate Expressions
// ============================================================================

#[test]
fn test_aggregate_sum_avg_count() {
    let result = parse(
        "stream S = E.window(1m).aggregate(total: sum(value), average: avg(value), n: count())",
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let agg_op = ops.iter().find(|op| matches!(op, StreamOp::Aggregate(_)));
        assert!(agg_op.is_some());
        if let StreamOp::Aggregate(items) = agg_op.unwrap() {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0].alias, "total");
            assert_eq!(items[1].alias, "average");
            assert_eq!(items[2].alias, "n");
        }
    }
}

#[test]
fn test_aggregate_min_max() {
    let result = parse("stream S = E.window(1h).aggregate(lo: min(price), hi: max(price))");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_aggregate_with_nested_call() {
    // count(distinct(x)) parses as a call expression
    let result = parse("stream S = E.window(1m).aggregate(unique: count(distinct(x)))");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

// ============================================================================
// 10. Control Flow in Functions
// ============================================================================

#[test]
fn test_if_elif_else() {
    let src = r#"fn classify(x: int) -> str:
  if x > 100:
    return "high"
  elif x > 50:
    return "medium"
  else:
    return "low""#;
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { body, .. } => {
            // The body should contain an if statement
            match &body[0].node {
                Stmt::If {
                    elif_branches,
                    else_branch,
                    ..
                } => {
                    assert_eq!(elif_branches.len(), 1);
                    assert!(else_branch.is_some());
                }
                other => panic!("Expected If, got {:?}", other),
            }
        }
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

#[test]
fn test_for_loop() {
    let src = "fn process(n: int):\n  for i in 0..n:\n    let x = i * 2";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { body, .. } => match &body[0].node {
            Stmt::For { var, .. } => {
                assert_eq!(var, "i");
            }
            other => panic!("Expected For, got {:?}", other),
        },
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

#[test]
fn test_while_loop() {
    let src = "fn loop_fn():\n  while x > 0:\n    x := x - 1";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { body, .. } => match &body[0].node {
            Stmt::While { .. } => {}
            other => panic!("Expected While, got {:?}", other),
        },
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

#[test]
fn test_break_continue_in_loop() {
    let src = r#"fn loop_fn():
  for i in 0..10:
    if i == 5:
      break
    if i == 3:
      continue"#;
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { body, .. } => {
            // body[0] is the for loop
            match &body[0].node {
                Stmt::For { body: for_body, .. } => {
                    // for_body should have two if stmts
                    assert_eq!(for_body.len(), 2);
                    // First if has break
                    match &for_body[0].node {
                        Stmt::If { then_branch, .. } => {
                            assert!(matches!(&then_branch[0].node, Stmt::Break));
                        }
                        other => panic!("Expected If with break, got {:?}", other),
                    }
                    // Second if has continue
                    match &for_body[1].node {
                        Stmt::If { then_branch, .. } => {
                            assert!(matches!(&then_branch[0].node, Stmt::Continue));
                        }
                        other => panic!("Expected If with continue, got {:?}", other),
                    }
                }
                other => panic!("Expected For, got {:?}", other),
            }
        }
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

#[test]
fn test_return_with_value() {
    let src = "fn get_val() -> int:\n  return 42";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { body, .. } => match &body[0].node {
            Stmt::Return(Some(expr)) => {
                assert_eq!(*expr, Expr::Int(42));
            }
            other => panic!("Expected Return(Some(42)), got {:?}", other),
        },
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

// ============================================================================
// 11. String Escaping
// ============================================================================

#[test]
fn test_string_with_escaped_quotes() {
    let stmt = parse_first_stmt(r#"let x = "hello \"world\"""#);
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Str(s) => {
                assert!(s.contains("\\\""));
            }
            other => panic!("Expected Str, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_string_with_backslash_n() {
    let stmt = parse_first_stmt(r#"let x = "line1\nline2""#);
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Str(s) => {
                assert!(s.contains("\\n"));
            }
            other => panic!("Expected Str, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

// ============================================================================
// 12. Comments
// ============================================================================

#[test]
fn test_line_comment_only() {
    let result = parse("# This is a comment\nlet x = 1");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    // Comment is ignored, only the variable declaration remains
    assert_eq!(program.statements.len(), 1);
}

#[test]
fn test_block_comment() {
    let result = parse("/* block comment */\nlet x = 1");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_inline_comment() {
    let result = parse("let x = 42 # inline comment");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

// ============================================================================
// 13. Complex Within Clauses (duration units)
// ============================================================================

#[test]
fn test_within_seconds() {
    let result = parse("stream S = A as a -> B as b .within(30s)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_within_milliseconds() {
    let result = parse("stream S = A as a -> B as b .within(1000ms)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_within_minutes() {
    let result = parse("stream S = A as a -> B as b .within(5m)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_within_hours() {
    let result = parse("stream S = A as a -> B as b .within(1h)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_within_days() {
    let result = parse("stream S = A as a -> B as b .within(1d)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_within_nanoseconds() {
    let result = parse("stream S = A as a -> B as b .within(500ns)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_within_microseconds() {
    let result = parse("stream S = A as a -> B as b .within(100us)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

// ============================================================================
// 14. Trend Aggregate
// ============================================================================

#[test]
fn test_trend_aggregate_single_no_arg() {
    let result = parse(
        "stream S = A as first -> all A as rising .within(60s) .trend_aggregate(count: count_trends()) .emit(trends: count)",
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let ta = ops
            .iter()
            .find(|op| matches!(op, StreamOp::TrendAggregate(_)));
        assert!(ta.is_some());
        if let StreamOp::TrendAggregate(items) = ta.unwrap() {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].alias, "count");
            assert_eq!(items[0].func, "count_trends");
            assert!(items[0].arg.is_none());
        }
    }
}

#[test]
fn test_trend_aggregate_with_arg() {
    let result = parse(
        "stream S = A as first -> all A as rising .within(60s) .trend_aggregate(events: count_events(rising)) .emit(ev: events)",
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        for op in ops {
            if let StreamOp::TrendAggregate(items) = op {
                assert_eq!(items[0].func, "count_events");
                assert!(items[0].arg.is_some());
                return;
            }
        }
        panic!("No TrendAggregate found");
    }
}

// ============================================================================
// 15. Edge Cases
// ============================================================================

#[test]
fn test_empty_program() {
    let result = parse("");
    assert!(result.is_ok());
    let program = result.unwrap();
    assert!(program.statements.is_empty());
}

#[test]
fn test_whitespace_only_program() {
    let result = parse("   \n\n   \n");
    assert!(result.is_ok());
    let program = result.unwrap();
    assert!(program.statements.is_empty());
}

#[test]
fn test_comment_only_program() {
    let result = parse("# Just a comment\n# Another comment");
    assert!(result.is_ok());
    let program = result.unwrap();
    assert!(program.statements.is_empty());
}

#[test]
fn test_minimal_stream() {
    let result = parse("stream X = Y");
    assert!(result.is_ok());
    let program = result.unwrap();
    assert_eq!(program.statements.len(), 1);
    if let Stmt::StreamDecl {
        name, source, ops, ..
    } = &program.statements[0].node
    {
        assert_eq!(name, "X");
        assert_eq!(*source, StreamSource::Ident("Y".to_string()));
        assert!(ops.is_empty());
    }
}

// ============================================================================
// 16. Additional Stream Operations
// ============================================================================

#[test]
fn test_distinct_with_expr() {
    let result = parse("stream S = E.distinct(category)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        match &ops[0] {
            StreamOp::Distinct(Some(_)) => {}
            other => panic!("Expected Distinct(Some(_)), got {:?}", other),
        }
    }
}

#[test]
fn test_distinct_without_expr() {
    let result = parse("stream S = E.distinct()");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        match &ops[0] {
            StreamOp::Distinct(None) => {}
            other => panic!("Expected Distinct(None), got {:?}", other),
        }
    }
}

#[test]
fn test_order_by_asc_desc() {
    // Test order_by with ascending (default) items
    let result = parse("stream S = E.order_by(price, name)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::OrderBy(items) = &ops[0] {
            assert_eq!(items.len(), 2);
            // Without explicit asc/desc, default is ascending (descending=false)
            assert!(!items[0].descending);
            assert!(!items[1].descending);
        } else {
            panic!("Expected OrderBy");
        }
    }
}

#[test]
fn test_limit_op() {
    let result = parse("stream S = E.limit(10)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        match &ops[0] {
            StreamOp::Limit(Expr::Int(10)) => {}
            other => panic!("Expected Limit(10), got {:?}", other),
        }
    }
}

#[test]
fn test_collect_op() {
    let result = parse("stream S = E.collect()");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::Collect));
    }
}

#[test]
fn test_all_op() {
    let result = parse("stream S = E.fork(a: .where(x > 0), b: .where(x < 0)).all()");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let has_all = ops.iter().any(|op| matches!(op, StreamOp::All));
        assert!(has_all, "Expected All op");
    }
}

#[test]
fn test_first_op() {
    let result = parse("stream S = E.fork(a: .where(x > 0), b: .where(x < 0)).first()");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let has_first = ops.iter().any(|op| matches!(op, StreamOp::First));
        assert!(has_first, "Expected First op");
    }
}

#[test]
fn test_any_op_with_count() {
    let result = parse("stream S = E.fork(a: .where(x > 0), b: .where(x < 0)).any(1)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let any_op = ops.iter().find(|op| matches!(op, StreamOp::Any(_)));
        assert!(any_op.is_some());
        if let StreamOp::Any(Some(n)) = any_op.unwrap() {
            assert_eq!(*n, 1);
        }
    }
}

#[test]
fn test_any_op_without_count() {
    let result = parse("stream S = E.fork(a: .where(x > 0), b: .where(x < 0)).any()");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let any_op = ops.iter().find(|op| matches!(op, StreamOp::Any(_)));
        assert!(any_op.is_some());
        assert!(matches!(any_op.unwrap(), StreamOp::Any(None)));
    }
}

// ============================================================================
// 17. Emit Operations
// ============================================================================

#[test]
fn test_emit_as_type() {
    let result = parse(r#"stream S = E.emit as AlertEvent(severity: "high")"#);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Emit {
            output_type,
            fields,
            ..
        } = &ops[0]
        {
            assert_eq!(output_type.as_deref(), Some("AlertEvent"));
            assert_eq!(fields.len(), 1);
        } else {
            panic!("Expected Emit op");
        }
    }
}

#[test]
fn test_emit_empty_args() {
    let result = parse("stream S = E.emit()");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Emit { fields, .. } = &ops[0] {
            assert!(fields.is_empty());
        } else {
            panic!("Expected Emit op");
        }
    }
}

// ============================================================================
// 18. Config Block
// ============================================================================

#[test]
fn test_config_block_brace_syntax() {
    let result = parse(
        r#"config mqtt {
            broker: "localhost",
            port: 1883,
        }"#,
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::Config { name, items } = &program.statements[0].node {
        assert_eq!(name, "mqtt");
        assert_eq!(items.len(), 2);
    } else {
        panic!("Expected Config");
    }
}

#[test]
fn test_config_with_float_value() {
    let result = parse(
        r#"config settings {
            threshold: 99.5,
        }"#,
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::Config { items, .. } = &program.statements[0].node {
        if let ConfigItem::Value(key, ConfigValue::Float(v)) = &items[0] {
            assert_eq!(key, "threshold");
            assert!((v - 99.5).abs() < 0.001);
        } else {
            panic!("Expected float config value");
        }
    }
}

// ============================================================================
// 19. Import Statements
// ============================================================================

#[test]
fn test_import_basic() {
    let stmt = parse_first_stmt(r#"import "utils.vpl""#);
    match stmt {
        Stmt::Import { path, alias } => {
            assert_eq!(path, "utils.vpl");
            assert!(alias.is_none());
        }
        other => panic!("Expected Import, got {:?}", other),
    }
}

#[test]
fn test_import_with_alias() {
    let stmt = parse_first_stmt(r#"import "lib/math.vpl" as math"#);
    match stmt {
        Stmt::Import { path, alias } => {
            assert_eq!(path, "lib/math.vpl");
            assert_eq!(alias, Some("math".to_string()));
        }
        other => panic!("Expected Import, got {:?}", other),
    }
}

// ============================================================================
// 20. Event Declarations with Extends and Optional Fields
// ============================================================================

#[test]
fn test_event_decl_with_extends() {
    let src = "event HighTempAlert extends SensorReading:\n  threshold: float";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::EventDecl {
            name,
            extends,
            fields,
        } => {
            assert_eq!(name, "HighTempAlert");
            assert_eq!(extends, Some("SensorReading".to_string()));
            assert_eq!(fields.len(), 1);
        }
        other => panic!("Expected EventDecl, got {:?}", other),
    }
}

#[test]
fn test_event_decl_optional_field() {
    // `str?` is parsed as optional_type by type_expr, so the field's `ty` is
    // Type::Optional(Str) and the field's own `optional` flag stays false.
    let src = "event Sensor:\n  id: str\n  label: str?";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::EventDecl { fields, .. } => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].ty, Type::Str);
            assert_eq!(fields[1].ty, Type::Optional(Box::new(Type::Str)));
        }
        other => panic!("Expected EventDecl, got {:?}", other),
    }
}

// ============================================================================
// 21. Context Declaration
// ============================================================================

#[test]
fn test_context_decl_with_cores() {
    let stmt = parse_first_stmt("context ingestion (cores: [0, 1, 2])");
    match stmt {
        Stmt::ContextDecl { name, cores } => {
            assert_eq!(name, "ingestion");
            assert_eq!(cores, Some(vec![0, 1, 2]));
        }
        other => panic!("Expected ContextDecl, got {:?}", other),
    }
}

#[test]
fn test_context_decl_without_cores() {
    let stmt = parse_first_stmt("context analytics");
    match stmt {
        Stmt::ContextDecl { name, cores } => {
            assert_eq!(name, "analytics");
            assert!(cores.is_none());
        }
        other => panic!("Expected ContextDecl, got {:?}", other),
    }
}

// ============================================================================
// 22. Expressions: Ranges, If-Expressions, Bitwise, Comparison Operators
// ============================================================================

#[test]
fn test_range_exclusive() {
    let stmt = parse_first_stmt("let r = 0..10");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Range { inclusive, .. } => assert!(!inclusive),
            other => panic!("Expected Range, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_range_inclusive() {
    let stmt = parse_first_stmt("let r = 0..=10");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Range { inclusive, .. } => assert!(inclusive),
            other => panic!("Expected Range, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_if_expression() {
    let stmt = parse_first_stmt("let x = if a > b then a else b");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::If { .. } => {}
            other => panic!("Expected If expr, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_comparison_operators() {
    for (op_str, expected_op) in [
        ("==", BinOp::Eq),
        ("!=", BinOp::NotEq),
        ("<", BinOp::Lt),
        ("<=", BinOp::Le),
        (">", BinOp::Gt),
        (">=", BinOp::Ge),
    ] {
        let src = format!("let x = a {} b", op_str);
        let stmt = parse_first_stmt(&src);
        match stmt {
            Stmt::VarDecl { value, .. } => match value {
                Expr::Binary { op, .. } => {
                    assert_eq!(op, expected_op, "Failed for operator {}", op_str);
                }
                other => panic!("Expected Binary for {}, got {:?}", op_str, other),
            },
            other => panic!("Expected VarDecl for {}, got {:?}", op_str, other),
        }
    }
}

#[test]
fn test_in_operator() {
    let result = parse("stream S = E.where(x in items)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_is_operator() {
    let result = parse("stream S = E.where(x is null)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_modulo_operator() {
    let stmt = parse_first_stmt("let x = a % b");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Binary { op: BinOp::Mod, .. } => {}
            other => panic!("Expected Mod, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_logical_or_and() {
    let result = parse("stream S = E.where(a > 0 or b > 0 and c > 0)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

// ============================================================================
// 23. Literals
// ============================================================================

#[test]
fn test_literal_null() {
    let stmt = parse_first_stmt("let x = null");
    match stmt {
        Stmt::VarDecl { value, .. } => assert_eq!(value, Expr::Null),
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_literal_false() {
    let stmt = parse_first_stmt("let x = false");
    match stmt {
        Stmt::VarDecl { value, .. } => assert_eq!(value, Expr::Bool(false)),
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_literal_float_with_exponent() {
    let stmt = parse_first_stmt("let x = 1.5e10");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Float(v) => assert!((v - 1.5e10).abs() < 1.0),
            other => panic!("Expected Float, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_literal_timestamp() {
    let stmt = parse_first_stmt("let t = @2024-01-15");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Timestamp(ns) => {
                assert!(ns > 0, "Timestamp should be positive");
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_literal_timestamp_with_time() {
    let stmt = parse_first_stmt("let t = @2024-01-15T10:30:00Z");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Timestamp(ns) => {
                assert!(ns > 0, "Timestamp should be positive");
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_array_literal() {
    let stmt = parse_first_stmt("let x = [1, 2, 3]");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Array(items) => assert_eq!(items.len(), 3),
            other => panic!("Expected Array, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_empty_array_literal() {
    let stmt = parse_first_stmt("let x = []");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Array(items) => assert!(items.is_empty()),
            other => panic!("Expected empty Array, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_map_literal() {
    let stmt = parse_first_stmt(r#"let x = {"key": 1, name: 2}"#);
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Map(entries) => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].0, "key");
                assert_eq!(entries[1].0, "name");
            }
            other => panic!("Expected Map, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_empty_map_literal() {
    let stmt = parse_first_stmt("let x = {}");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Map(entries) => assert!(entries.is_empty()),
            other => panic!("Expected empty Map, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

// ============================================================================
// 24. Lambda Expressions
// ============================================================================

#[test]
fn test_lambda_single_param() {
    let stmt = parse_first_stmt("let f = x => x * 2");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Lambda { params, .. } => {
                assert_eq!(params, vec!["x"]);
            }
            other => panic!("Expected Lambda, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_lambda_multi_params() {
    let stmt = parse_first_stmt("let f = (a, b) => a + b");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Lambda { params, .. } => {
                assert_eq!(params, vec!["a", "b"]);
            }
            other => panic!("Expected Lambda, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_lambda_no_params() {
    // Empty-param lambdas `() => 42` are not supported by the grammar.
    // The grammar requires at least one identifier: `("(" ~ identifier_list? ~ ")") | identifier`.
    // When identifier_list is empty, the parser cannot distinguish parameters from body.
    // Verify that this correctly fails to parse rather than panicking.
    let result = parse("let f = () => 42");
    assert!(result.is_err(), "Empty-param lambda should fail to parse");
}

// ============================================================================
// 25. Var Decl: mutable vs immutable
// ============================================================================

#[test]
fn test_var_mutable() {
    let stmt = parse_first_stmt("var counter = 0");
    match stmt {
        Stmt::VarDecl { mutable, name, .. } => {
            assert!(mutable);
            assert_eq!(name, "counter");
        }
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_let_immutable() {
    let stmt = parse_first_stmt("let constant = 42");
    match stmt {
        Stmt::VarDecl { mutable, name, .. } => {
            assert!(!mutable);
            assert_eq!(name, "constant");
        }
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

#[test]
fn test_var_with_type_annotation() {
    let stmt = parse_first_stmt("var count: int = 0");
    match stmt {
        Stmt::VarDecl {
            mutable,
            name,
            ty,
            value,
        } => {
            assert!(mutable);
            assert_eq!(name, "count");
            assert_eq!(ty, Some(Type::Int));
            assert_eq!(value, Expr::Int(0));
        }
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

// ============================================================================
// 26. Stream Operations: tap, log, on_error, process, watermark, allowed_lateness
// ============================================================================

#[test]
fn test_tap_op() {
    let result = parse(r#"stream S = E.tap(name: "debug_tap")"#);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::Tap(_)));
    }
}

#[test]
fn test_log_op() {
    let result = parse(r#"stream S = E.log(level: "info", message: "received")"#);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Log(args) = &ops[0] {
            assert_eq!(args.len(), 2);
        } else {
            panic!("Expected Log op");
        }
    }
}

#[test]
fn test_on_error_op() {
    let result = parse("stream S = E.on_error(handle_error)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::OnError(_)));
    }
}

#[test]
fn test_watermark_op() {
    let result = parse("stream S = E.watermark(out_of_order: 10s)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::Watermark(_)));
    }
}

#[test]
fn test_allowed_lateness_op() {
    let result = parse("stream S = E.allowed_lateness(30s)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::AllowedLateness(_)));
    }
}

#[test]
fn test_concurrent_op() {
    let result = parse("stream S = E.concurrent(workers: 4)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::Concurrent(_)));
    }
}

// ============================================================================
// 27. Not Op in Stream
// ============================================================================

#[test]
fn test_not_op_in_stream() {
    let result = parse("stream S = A as a -> B as b .not(C where c.id == a.id)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let not_op = ops.iter().find(|op| matches!(op, StreamOp::Not(_)));
        assert!(not_op.is_some(), "Expected Not op");
        if let StreamOp::Not(clause) = not_op.unwrap() {
            assert_eq!(clause.event_type, "C");
            assert!(clause.filter.is_some());
        }
    }
}

// ============================================================================
// 28. Select Operation
// ============================================================================

#[test]
fn test_select_with_alias_and_field() {
    let result = parse("stream S = E.select(name, total: price * quantity)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Select(items) = &ops[0] {
            assert_eq!(items.len(), 2);
            assert!(matches!(&items[0], SelectItem::Field(f) if f == "name"));
            assert!(matches!(&items[1], SelectItem::Alias(a, _) if a == "total"));
        } else {
            panic!("Expected Select op");
        }
    }
}

// ============================================================================
// 29. Stream With Type Annotation
// ============================================================================

#[test]
fn test_stream_with_type_annotation() {
    let result = parse("stream S: int = E.map(x * 2)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl {
        type_annotation, ..
    } = &program.statements[0].node
    {
        assert_eq!(*type_annotation, Some(Type::Int));
    } else {
        panic!("Expected StreamDecl");
    }
}

// ============================================================================
// 30. Assignment Statement
// ============================================================================

#[test]
fn test_assignment_stmt() {
    let src = "fn test():\n  x := x + 1";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { body, .. } => match &body[0].node {
            Stmt::Assignment { name, value } => {
                assert_eq!(name, "x");
                match value {
                    Expr::Binary { op: BinOp::Add, .. } => {}
                    other => panic!("Expected Add, got {:?}", other),
                }
            }
            other => panic!("Expected Assignment, got {:?}", other),
        },
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

// ============================================================================
// 31. From Connector Source
// ============================================================================

#[test]
fn test_from_connector_source() {
    let stmt =
        parse_first_stmt(r#"stream S = TemperatureReading.from(MqttSensors, topic: "sensors/#")"#);
    match stmt {
        Stmt::StreamDecl { source, .. } => match source {
            StreamSource::FromConnector {
                event_type,
                connector_name,
                params,
            } => {
                assert_eq!(event_type, "TemperatureReading");
                assert_eq!(connector_name, "MqttSensors");
                assert_eq!(params.len(), 1);
                assert_eq!(params[0].name, "topic");
            }
            other => panic!("Expected FromConnector, got {:?}", other),
        },
        other => panic!("Expected StreamDecl, got {:?}", other),
    }
}

// ============================================================================
// 32. Attention Window
// ============================================================================

#[test]
fn test_attention_window_op() {
    let result = parse(r#"stream S = E.attention_window(heads: 4, window_size: 100)"#);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::AttentionWindow(args) = &ops[0] {
            assert_eq!(args.len(), 2);
        } else {
            panic!("Expected AttentionWindow op");
        }
    }
}

// ============================================================================
// 33. Print Op
// ============================================================================

#[test]
fn test_print_op_with_args() {
    let result = parse(r#"stream S = E.print("debug", value)"#);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Print(exprs) = &ops[0] {
            assert_eq!(exprs.len(), 2);
        } else {
            panic!("Expected Print op");
        }
    }
}

#[test]
fn test_print_op_no_args() {
    let result = parse("stream S = E.print()");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Print(exprs) = &ops[0] {
            assert!(exprs.is_empty());
        } else {
            panic!("Expected Print op");
        }
    }
}

// ============================================================================
// 34. Score Operation
// ============================================================================

#[test]
fn test_score_op_structure() {
    let result = parse(
        r#"stream S = E.score(model: "fraud.onnx", inputs: [amount, risk], outputs: [prob])"#,
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Score(spec) = &ops[0] {
            assert_eq!(spec.model_path, "fraud.onnx");
            assert_eq!(spec.inputs, vec!["amount", "risk"]);
            assert_eq!(spec.outputs, vec!["prob"]);
        } else {
            panic!("Expected Score op");
        }
    }
}

// ============================================================================
// 35. Context Op
// ============================================================================

#[test]
fn test_context_op_in_stream() {
    let result = parse("stream S = E.context(ingestion)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        match &ops[0] {
            StreamOp::Context(name) => assert_eq!(name, "ingestion"),
            other => panic!("Expected Context op, got {:?}", other),
        }
    }
}

// ============================================================================
// 36. On Op
// ============================================================================

#[test]
fn test_on_op() {
    let result = parse("stream S = E.on(timestamp)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::On(_)));
    }
}

// ============================================================================
// 37. To Connector
// ============================================================================

#[test]
fn test_to_connector_with_params() {
    let result = parse(r#"stream S = E.to(KafkaOutput, topic: "output", key: "id")"#);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::To {
            connector_name,
            params,
        } = &ops[0]
        {
            assert_eq!(connector_name, "KafkaOutput");
            assert_eq!(params.len(), 2);
        } else {
            panic!("Expected To op");
        }
    }
}

// ============================================================================
// 38. Multi-Statement Programs
// ============================================================================

#[test]
fn test_multi_statement_program() {
    let src = r#"
        type Temperature = float
        const MAX_TEMP = 100
        event SensorReading:
            sensor_id: str
            value: float
        stream Alerts = SensorReading
            .where(value > MAX_TEMP)
            .emit(alert: "high_temp")
    "#;
    let result = parse(src);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    assert_eq!(program.statements.len(), 4);
    assert!(matches!(&program.statements[0].node, Stmt::TypeDecl { .. }));
    assert!(matches!(
        &program.statements[1].node,
        Stmt::ConstDecl { .. }
    ));
    assert!(matches!(
        &program.statements[2].node,
        Stmt::EventDecl { .. }
    ));
    assert!(matches!(
        &program.statements[3].node,
        Stmt::StreamDecl { .. }
    ));
}

// ============================================================================
// 39. Having Op
// ============================================================================

#[test]
fn test_having_op_structure() {
    let result = parse("stream S = E.window(1m).aggregate(n: count()).having(n > 10)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let having = ops.iter().find(|op| matches!(op, StreamOp::Having(_)));
        assert!(having.is_some());
    }
}

// ============================================================================
// 40. Filter Expression in FollowedBy (exercises filter_expr path)
// ============================================================================

#[test]
fn test_followed_by_complex_filter() {
    // This exercises the filter_expr parsing path with arithmetic and comparison
    let result =
        parse("stream S = A as a -> B where b.value * 2 + 1 > a.threshold .emit(done: true)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_followed_by_filter_with_or_and() {
    // Exercises filter_or_expr and filter_and_expr paths
    let result = parse("stream S = A as a -> B where x > 0 and y > 0 or z > 0 .emit(done: true)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_followed_by_filter_with_not() {
    // Exercises filter_not_expr path
    let result = parse("stream S = A as a -> B where not active .emit(done: true)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

// ============================================================================
// 41. Pattern Op in Stream (pattern_or_expr -> pattern_sequence)
// ============================================================================

#[test]
fn test_pattern_op_sequence() {
    let result = parse("stream S = E.window(1m).pattern(p: A -> B -> C).emit(alert: true)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        let pat = ops.iter().find(|op| matches!(op, StreamOp::Pattern(_)));
        assert!(pat.is_some(), "Expected Pattern op");
        if let StreamOp::Pattern(def) = pat.unwrap() {
            assert_eq!(def.name, "p");
            // Matcher should be FollowedBy chain
            match &def.matcher {
                Expr::Binary {
                    op: BinOp::FollowedBy,
                    ..
                } => {}
                other => panic!("Expected FollowedBy chain, got {:?}", other),
            }
        }
    }
}

// ============================================================================
// 42. Parenthesized Expressions (primary_expr -> "(expr)")
// ============================================================================

#[test]
fn test_parenthesized_expression() {
    let stmt = parse_first_stmt("let x = (a + b) * c");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Binary { op: BinOp::Mul, .. } => {}
            other => panic!("Expected Mul at top, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

// ============================================================================
// 43. Emit Statement (not the stream op, but the standalone statement)
// ============================================================================

#[test]
fn test_emit_statement_with_fields() {
    let src = "fn test():\n  emit Alert(severity: 1, message: \"oops\")";
    let stmt = parse_first_stmt(src);
    match stmt {
        Stmt::FnDecl { body, .. } => match &body[0].node {
            Stmt::Emit { event_type, fields } => {
                assert_eq!(event_type, "Alert");
                assert_eq!(fields.len(), 2);
            }
            other => panic!("Expected Emit stmt, got {:?}", other),
        },
        other => panic!("Expected FnDecl, got {:?}", other),
    }
}

// ============================================================================
// 44. Process Op
// ============================================================================

#[test]
fn test_process_op() {
    let result = parse("stream S = E.process(do_stuff)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::Process(_)));
    }
}

// ============================================================================
// 45. Map Op
// ============================================================================

#[test]
fn test_map_op() {
    let result = parse("stream S = E.map(x * 2 + 1)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::Map(_)));
    }
}

// ============================================================================
// 46. Filter Op (distinct from .where())
// ============================================================================

#[test]
fn test_filter_op() {
    let result = parse("stream S = E.filter(x > 0)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::Filter(_)));
    }
}

// ============================================================================
// 47. PartitionBy Op
// ============================================================================

#[test]
fn test_partition_by_op() {
    let result = parse("stream S = E.partition_by(sensor_id)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        assert!(matches!(&ops[0], StreamOp::PartitionBy(_)));
    }
}

// ============================================================================
// 48. Inline Merge with simple identifiers
// ============================================================================

#[test]
fn test_merge_simple_identifiers() {
    let result = parse("stream S = merge(StreamA, StreamB)");
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { source, .. } = &program.statements[0].node {
        match source {
            StreamSource::Merge(streams) => {
                assert_eq!(streams.len(), 2);
                // Simple identifier: name == source
                assert_eq!(streams[0].name, "StreamA");
                assert_eq!(streams[0].source, "StreamA");
                assert!(streams[0].filter.is_none());
            }
            other => panic!("Expected Merge, got {:?}", other),
        }
    }
}

// ============================================================================
// 49. Fork Op
// ============================================================================

#[test]
fn test_fork_op_structure() {
    let result = parse(
        "stream S = E.fork(high: .where(x > 100).emit(level: \"high\"), low: .where(x < 10).emit(level: \"low\"))",
    );
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Fork(paths) = &ops[0] {
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].name, "high");
            assert_eq!(paths[1].name, "low");
            // Each path should have where + emit = 2 ops
            assert_eq!(paths[0].ops.len(), 2);
            assert_eq!(paths[1].ops.len(), 2);
        } else {
            panic!("Expected Fork op");
        }
    }
}

// ============================================================================
// 50. Emit with context targeting
// ============================================================================

#[test]
fn test_emit_with_context_target() {
    let result = parse(r#"stream S = E.emit(context: analytics, value: x)"#);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let program = result.unwrap();
    if let Stmt::StreamDecl { ops, .. } = &program.statements[0].node {
        if let StreamOp::Emit {
            target_context,
            fields,
            ..
        } = &ops[0]
        {
            assert_eq!(target_context.as_deref(), Some("analytics"));
            // context: analytics should NOT appear in fields
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "value");
        } else {
            panic!("Expected Emit op");
        }
    }
}

// ============================================================================
// 51-80. Lexer edge case tests
// ============================================================================

use varpulis_parser::lexer::{tokenize, Lexer, Token};

// ---------------------------------------------------------------------------
// 51. Identifiers with underscores and digits
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_identifiers_with_underscores() {
    let tokens: Vec<_> = tokenize("_foo bar_baz _123 a1b2c3")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Ident("_foo".to_string()));
    assert_eq!(tokens[1], Token::Ident("bar_baz".to_string()));
    assert_eq!(tokens[2], Token::Ident("_123".to_string()));
    assert_eq!(tokens[3], Token::Ident("a1b2c3".to_string()));
}

// ---------------------------------------------------------------------------
// 52. Single-character identifiers
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_single_char_identifiers() {
    let tokens: Vec<_> = tokenize("a b c x y z _")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Ident("a".to_string()));
    assert_eq!(tokens[1], Token::Ident("b".to_string()));
    assert_eq!(tokens[6], Token::Ident("_".to_string()));
}

// ---------------------------------------------------------------------------
// 53. Very long string literal
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_very_long_string() {
    let long = "a".repeat(10000);
    let input = format!("\"{}\"", long);
    let tokens: Vec<_> = tokenize(&input).into_iter().map(|t| t.token).collect();
    match &tokens[0] {
        Token::String(s) => assert_eq!(s.len(), 10000),
        other => panic!("Expected String, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 54. String with single quotes
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_single_quote_string() {
    let tokens: Vec<_> = tokenize("'hello world'")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::String("hello world".to_string()));
}

// ---------------------------------------------------------------------------
// 55. Mixed quote strings
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_mixed_quote_strings() {
    let tokens: Vec<_> = tokenize(r#""double" 'single' "mixed""#)
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::String("double".to_string()));
    assert_eq!(tokens[1], Token::String("single".to_string()));
    assert_eq!(tokens[2], Token::String("mixed".to_string()));
}

// ---------------------------------------------------------------------------
// 56. String with various escape sequences
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_string_escape_sequences() {
    let tokens: Vec<_> = tokenize(r#""tab\there" "newline\nhere" "quote\"here""#)
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::String(s) if s.contains("\\t")));
    assert!(matches!(&tokens[1], Token::String(s) if s.contains("\\n")));
    assert!(matches!(&tokens[2], Token::String(s) if s.contains("\\\"")));
}

// ---------------------------------------------------------------------------
// 57. Float with scientific notation
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_float_scientific_notation() {
    let tokens: Vec<_> = tokenize("1.5e10 2.0e3 3.14e2")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::Float(f) if (*f - 1.5e10).abs() < 1.0));
    assert!(matches!(&tokens[1], Token::Float(f) if (*f - 2.0e3).abs() < 0.001));
    assert!(matches!(&tokens[2], Token::Float(f) if (*f - 3.14e2).abs() < 0.001));
}

// ---------------------------------------------------------------------------
// 58. Float with negative exponent
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_float_negative_exponent() {
    let tokens: Vec<_> = tokenize("1.0e-5 2.5E-3")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::Float(f) if (*f - 1.0e-5).abs() < 1e-10));
    assert!(matches!(&tokens[1], Token::Float(f) if (*f - 2.5e-3).abs() < 1e-8));
}

// ---------------------------------------------------------------------------
// 59. Float with positive exponent marker
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_float_positive_exponent() {
    let tokens: Vec<_> = tokenize("1.0e+5 2.5E+3")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::Float(f) if (*f - 1.0e5).abs() < 1.0));
    assert!(matches!(&tokens[1], Token::Float(f) if (*f - 2.5e3).abs() < 0.001));
}

// ---------------------------------------------------------------------------
// 60. Large integer
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_large_integer() {
    let tokens: Vec<_> = tokenize("9999999999999")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Integer(9999999999999));
}

// ---------------------------------------------------------------------------
// 61. Zero and small integers
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_zero_and_small_integers() {
    let tokens: Vec<_> = tokenize("0 1 2 3").into_iter().map(|t| t.token).collect();
    assert_eq!(tokens[0], Token::Integer(0));
    assert_eq!(tokens[1], Token::Integer(1));
    assert_eq!(tokens[2], Token::Integer(2));
    assert_eq!(tokens[3], Token::Integer(3));
}

// ---------------------------------------------------------------------------
// 62. Duration units: ns, us
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_duration_ns_us() {
    let tokens: Vec<_> = tokenize("500ns 100us")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::Duration(s) if s == "500ns"));
    assert!(matches!(&tokens[1], Token::Duration(s) if s == "100us"));
}

// ---------------------------------------------------------------------------
// 63. Timestamp with timezone offset
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_timestamp_with_timezone() {
    let tokens: Vec<_> = tokenize("@2024-01-15T10:30:00+02:00")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::Timestamp(s) if s.contains("+02:00")));
}

#[test]
fn test_lexer_timestamp_utc() {
    let tokens: Vec<_> = tokenize("@2024-06-15T00:00:00Z")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::Timestamp(s) if s.ends_with("Z")));
}

// ---------------------------------------------------------------------------
// 64. Timestamp date-only
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_timestamp_date_only() {
    let tokens: Vec<_> = tokenize("@2025-12-31")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert!(matches!(&tokens[0], Token::Timestamp(s) if s == "@2025-12-31"));
}

// ---------------------------------------------------------------------------
// 65. All operator tokens
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_all_comparison_operators() {
    let tokens: Vec<_> = tokenize("== != < <= > >=")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::EqEq);
    assert_eq!(tokens[1], Token::NotEq);
    assert_eq!(tokens[2], Token::Lt);
    assert_eq!(tokens[3], Token::Le);
    assert_eq!(tokens[4], Token::Gt);
    assert_eq!(tokens[5], Token::Ge);
}

// ---------------------------------------------------------------------------
// 66. All arithmetic operators
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_all_arithmetic_operators() {
    let tokens: Vec<_> = tokenize("+ - * / % **")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Plus);
    assert_eq!(tokens[1], Token::Minus);
    assert_eq!(tokens[2], Token::Star);
    assert_eq!(tokens[3], Token::Slash);
    assert_eq!(tokens[4], Token::Percent);
    assert_eq!(tokens[5], Token::DoubleStar);
}

// ---------------------------------------------------------------------------
// 67. All bitwise operators
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_all_bitwise_operators() {
    let tokens: Vec<_> = tokenize("& | ^ ~ << >>")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Amp);
    assert_eq!(tokens[1], Token::Pipe);
    assert_eq!(tokens[2], Token::Caret);
    assert_eq!(tokens[3], Token::Tilde);
    assert_eq!(tokens[4], Token::Shl);
    assert_eq!(tokens[5], Token::Shr);
}

// ---------------------------------------------------------------------------
// 68. All assignment operators
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_all_assignment_operators() {
    let tokens: Vec<_> = tokenize("= += -= *= /= %=")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Eq);
    assert_eq!(tokens[1], Token::PlusEq);
    assert_eq!(tokens[2], Token::MinusEq);
    assert_eq!(tokens[3], Token::StarEq);
    assert_eq!(tokens[4], Token::SlashEq);
    assert_eq!(tokens[5], Token::PercentEq);
}

// ---------------------------------------------------------------------------
// 69. All special operators
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_all_special_operators() {
    let tokens: Vec<_> = tokenize(". ?. ?? => -> .. ..= $")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Dot);
    assert_eq!(tokens[1], Token::QuestionDot);
    assert_eq!(tokens[2], Token::QuestionQuestion);
    assert_eq!(tokens[3], Token::FatArrow);
    assert_eq!(tokens[4], Token::Arrow);
    assert_eq!(tokens[5], Token::DotDot);
    assert_eq!(tokens[6], Token::DotDotEq);
    assert_eq!(tokens[7], Token::Dollar);
}

// ---------------------------------------------------------------------------
// 70. All delimiters
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_all_delimiters() {
    let tokens: Vec<_> = tokenize("( ) [ ] { } , : ? @")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::LParen);
    assert_eq!(tokens[1], Token::RParen);
    assert_eq!(tokens[2], Token::LBracket);
    assert_eq!(tokens[3], Token::RBracket);
    assert_eq!(tokens[4], Token::LBrace);
    assert_eq!(tokens[5], Token::RBrace);
    assert_eq!(tokens[6], Token::Comma);
    assert_eq!(tokens[7], Token::Colon);
    assert_eq!(tokens[8], Token::Question);
    assert_eq!(tokens[9], Token::At);
}

// ---------------------------------------------------------------------------
// 71. Lexer error recovery: invalid characters become identifiers
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_error_recovery_invalid_char() {
    // Characters like backtick are not in the VPL lexer grammar
    // The lexer should recover and produce tokens for adjacent valid items
    let tokens = tokenize("`x");
    // Should not panic, may produce error tokens
    assert!(!tokens.is_empty());
}

// ---------------------------------------------------------------------------
// 72. Lexer peek then iterate
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_peek_then_iterate() {
    let mut lexer = Lexer::new("x + y");
    // Peek should return 'x'
    let peeked = lexer.peek().unwrap().token.clone();
    assert_eq!(peeked, Token::Ident("x".to_string()));
    // Consuming via next() should return the peeked 'x'
    let first = lexer.next().unwrap().token;
    assert_eq!(first, Token::Ident("x".to_string()));
    // Next should be '+'
    let second = lexer.next().unwrap().token;
    assert_eq!(second, Token::Plus);
    // Next should be 'y'
    let third = lexer.next().unwrap().token;
    assert_eq!(third, Token::Ident("y".to_string()));
    // Next should be Eof
    let eof = lexer.next().unwrap().token;
    assert_eq!(eof, Token::Eof);
    // After Eof, next returns None
    assert!(lexer.next().is_none());
}

// ---------------------------------------------------------------------------
// 73. Lexer double peek returns same result
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_double_peek_idempotent() {
    let mut lexer = Lexer::new("hello world");
    let p1 = lexer.peek().unwrap().token.clone();
    let p2 = lexer.peek().unwrap().token.clone();
    assert_eq!(p1, p2, "Peeking twice should return the same token");
}

// ---------------------------------------------------------------------------
// 74. All logical keywords
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_all_logical_keywords() {
    let tokens: Vec<_> = tokenize("and or xor not in is")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::And);
    assert_eq!(tokens[1], Token::Or);
    assert_eq!(tokens[2], Token::Xor);
    assert_eq!(tokens[3], Token::Not);
    assert_eq!(tokens[4], Token::In);
    assert_eq!(tokens[5], Token::Is);
}

// ---------------------------------------------------------------------------
// 75. Module keywords: as, extends, import, export
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_module_keywords() {
    let tokens: Vec<_> = tokenize("as extends import export")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::As);
    assert_eq!(tokens[1], Token::Extends);
    assert_eq!(tokens[2], Token::Import);
    assert_eq!(tokens[3], Token::Export);
}

// ---------------------------------------------------------------------------
// 76. Stream-specific keywords
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_stream_specific_keywords() {
    let tokens: Vec<_> =
        tokenize("from partition_by order_by limit distinct to on all within pattern")
            .into_iter()
            .map(|t| t.token)
            .collect();
    assert_eq!(tokens[0], Token::From);
    assert_eq!(tokens[1], Token::PartitionBy);
    assert_eq!(tokens[2], Token::OrderBy);
    assert_eq!(tokens[3], Token::Limit);
    assert_eq!(tokens[4], Token::Distinct);
    assert_eq!(tokens[5], Token::To);
    assert_eq!(tokens[6], Token::On);
    assert_eq!(tokens[7], Token::All);
    assert_eq!(tokens[8], Token::Within);
    assert_eq!(tokens[9], Token::Pattern);
}

// ---------------------------------------------------------------------------
// 77. Attention keywords
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_attention_keywords() {
    let tokens: Vec<_> = tokenize("attention_window attention_score")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::AttentionWindow);
    assert_eq!(tokens[1], Token::AttentionScore);
}

// ---------------------------------------------------------------------------
// 78. Deeply nested parenthesized expression
// ---------------------------------------------------------------------------

#[test]
fn test_deeply_nested_parens() {
    let result = parse("let x = ((((((a + b))))))");
    assert!(
        result.is_ok(),
        "Deeply nested parens should parse: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// 79. Deeply nested member access
// ---------------------------------------------------------------------------

#[test]
fn test_deeply_nested_member_access() {
    let stmt = parse_first_stmt("let x = a.b.c.d.e.f");
    match stmt {
        Stmt::VarDecl { value, .. } => match value {
            Expr::Member { member, expr } => {
                assert_eq!(member, "f");
                // The inner expr should be a.b.c.d.e
                match *expr {
                    Expr::Member { member: m, .. } => assert_eq!(m, "e"),
                    other => panic!("Expected inner Member, got {:?}", other),
                }
            }
            other => panic!("Expected Member chain, got {:?}", other),
        },
        other => panic!("Expected VarDecl, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 80. Deeply nested array indexing
// ---------------------------------------------------------------------------

#[test]
fn test_deeply_nested_index_access() {
    let result = parse("let x = arr[0][1][2]");
    assert!(
        result.is_ok(),
        "Nested index should parse: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// 81. Whitespace-only between tokens
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_various_whitespace() {
    // Tabs, spaces, newlines, carriage returns, form feeds
    let tokens: Vec<_> = tokenize("a\t\nb\r\nc")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Ident("a".to_string()));
    assert_eq!(tokens[1], Token::Ident("b".to_string()));
    assert_eq!(tokens[2], Token::Ident("c".to_string()));
}

// ---------------------------------------------------------------------------
// 82. Line comment at end of input
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_comment_at_end() {
    let tokens: Vec<_> = tokenize("x # this is a comment")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Ident("x".to_string()));
    assert_eq!(tokens[1], Token::Eof);
}

// ---------------------------------------------------------------------------
// 83. Block comment spanning multiple lines
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_multiline_block_comment() {
    let tokens: Vec<_> = tokenize("a /* this is\na multiline\ncomment */ b")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Ident("a".to_string()));
    assert_eq!(tokens[1], Token::Ident("b".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

// ---------------------------------------------------------------------------
// 84. Token Display for all remaining variants
// ---------------------------------------------------------------------------

#[test]
fn test_token_display_xor() {
    assert_eq!(format!("{}", Token::Xor), "xor");
}

#[test]
fn test_token_display_question_and_at() {
    assert_eq!(format!("{}", Token::Question), "?");
    assert_eq!(format!("{}", Token::At), "@");
}

#[test]
fn test_token_display_null() {
    assert_eq!(format!("{}", Token::Null), "null");
}

#[test]
fn test_token_display_from() {
    assert_eq!(format!("{}", Token::From), "from");
}

#[test]
fn test_token_display_comma() {
    assert_eq!(format!("{}", Token::Comma), ",");
}

#[test]
fn test_token_display_colon() {
    assert_eq!(format!("{}", Token::Colon), ":");
}

#[test]
fn test_token_display_dot_dot() {
    assert_eq!(format!("{}", Token::DotDot), "..");
    assert_eq!(format!("{}", Token::DotDotEq), "..=");
}

// ---------------------------------------------------------------------------
// 85. Keywords that look like identifiers
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_keyword_vs_identifier() {
    // "streaming" contains "stream" but should be an identifier
    let tokens: Vec<_> = tokenize("streaming stream_name")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Ident("streaming".to_string()));
    assert_eq!(tokens[1], Token::Ident("stream_name".to_string()));
}

// ---------------------------------------------------------------------------
// 86. Empty string literal
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_empty_string() {
    let tokens: Vec<_> = tokenize(r#""""#).into_iter().map(|t| t.token).collect();
    assert_eq!(tokens[0], Token::String("".to_string()));
}

// ---------------------------------------------------------------------------
// 87. Spanned token position tracking with multi-byte
// ---------------------------------------------------------------------------

#[test]
fn test_spanned_token_positions_operators() {
    let tokens: Vec<_> = tokenize("x == y").into_iter().collect();
    // x at 0..1
    assert_eq!(tokens[0].start, 0);
    assert_eq!(tokens[0].end, 1);
    // == at 2..4
    assert_eq!(tokens[1].start, 2);
    assert_eq!(tokens[1].end, 4);
    // y at 5..6
    assert_eq!(tokens[2].start, 5);
    assert_eq!(tokens[2].end, 6);
}

// ---------------------------------------------------------------------------
// 88. Tokenize function returns complete list
// ---------------------------------------------------------------------------

#[test]
fn test_tokenize_complete_program() {
    let tokens = tokenize("stream S = E .where(x > 0) .emit(val: x)");
    // Should contain: stream S = E . where ( x > 0 ) . emit ( val : x ) EOF
    let token_types: Vec<_> = tokens.into_iter().map(|t| t.token).collect();
    assert!(token_types.contains(&Token::Stream));
    assert!(token_types.contains(&Token::Where));
    assert!(token_types.contains(&Token::Emit));
    assert!(token_types.last() == Some(&Token::Eof));
}

// ---------------------------------------------------------------------------
// 89. Boolean literals
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_boolean_literals() {
    let tokens: Vec<_> = tokenize("true false")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::True);
    assert_eq!(tokens[1], Token::False);
}

// ---------------------------------------------------------------------------
// 90. Control flow keywords
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_control_flow_keywords() {
    let tokens: Vec<_> = tokenize("if else elif then match for while break continue return")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::If);
    assert_eq!(tokens[1], Token::Else);
    assert_eq!(tokens[2], Token::Elif);
    assert_eq!(tokens[3], Token::Then);
    assert_eq!(tokens[4], Token::Match);
    assert_eq!(tokens[5], Token::For);
    assert_eq!(tokens[6], Token::While);
    assert_eq!(tokens[7], Token::Break);
    assert_eq!(tokens[8], Token::Continue);
    assert_eq!(tokens[9], Token::Return);
}

// ---------------------------------------------------------------------------
// 91. Type keywords in sequence
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_type_keywords_in_sequence() {
    let tokens: Vec<_> = tokenize("int float bool str timestamp duration Stream")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::IntType);
    assert_eq!(tokens[1], Token::FloatType);
    assert_eq!(tokens[2], Token::BoolType);
    assert_eq!(tokens[3], Token::StrType);
    assert_eq!(tokens[4], Token::TimestampType);
    assert_eq!(tokens[5], Token::DurationType);
    assert_eq!(tokens[6], Token::StreamType);
}

// ---------------------------------------------------------------------------
// 92. Complex expression tokenization
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_complex_expression() {
    let tokens: Vec<_> = tokenize("a.b[0].c?.d ?? e ** 2")
        .into_iter()
        .map(|t| t.token)
        .collect();
    // a . b [ 0 ] . c ?. d ?? e ** 2
    assert_eq!(tokens[0], Token::Ident("a".to_string()));
    assert_eq!(tokens[1], Token::Dot);
    assert_eq!(tokens[2], Token::Ident("b".to_string()));
    assert_eq!(tokens[3], Token::LBracket);
    assert_eq!(tokens[4], Token::Integer(0));
    assert_eq!(tokens[5], Token::RBracket);
    assert_eq!(tokens[6], Token::Dot);
    assert_eq!(tokens[7], Token::Ident("c".to_string()));
    assert_eq!(tokens[8], Token::QuestionDot);
    assert_eq!(tokens[9], Token::Ident("d".to_string()));
    assert_eq!(tokens[10], Token::QuestionQuestion);
    assert_eq!(tokens[11], Token::Ident("e".to_string()));
    assert_eq!(tokens[12], Token::DoubleStar);
    assert_eq!(tokens[13], Token::Integer(2));
}

// ---------------------------------------------------------------------------
// 93. Lexer iterator exhaustion
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_exhaustion() {
    let mut lexer = Lexer::new("x");
    assert!(matches!(lexer.next().unwrap().token, Token::Ident(_)));
    assert_eq!(lexer.next().unwrap().token, Token::Eof);
    // After Eof, should return None
    assert!(lexer.next().is_none());
    assert!(lexer.next().is_none()); // Multiple calls after exhaustion
}

// ---------------------------------------------------------------------------
// 94. Adjacent operators without spaces
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_adjacent_operators() {
    let tokens: Vec<_> = tokenize("x>=y").into_iter().map(|t| t.token).collect();
    assert_eq!(tokens[0], Token::Ident("x".to_string()));
    assert_eq!(tokens[1], Token::Ge);
    assert_eq!(tokens[2], Token::Ident("y".to_string()));
}

// ---------------------------------------------------------------------------
// 95. Declaration keywords
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_declaration_keywords() {
    let tokens: Vec<_> = tokenize("stream event type let var const fn config")
        .into_iter()
        .map(|t| t.token)
        .collect();
    assert_eq!(tokens[0], Token::Stream);
    assert_eq!(tokens[1], Token::Event);
    assert_eq!(tokens[2], Token::Type);
    assert_eq!(tokens[3], Token::Let);
    assert_eq!(tokens[4], Token::Var);
    assert_eq!(tokens[5], Token::Const);
    assert_eq!(tokens[6], Token::Fn);
    assert_eq!(tokens[7], Token::Config);
}

// ---------------------------------------------------------------------------
// 96. Complex stream declaration tokenization
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_full_stream_decl() {
    let input = "stream Alerts = SensorReading as sr -> all Tick as t .within(30s) .emit(val: x)";
    let tokens = tokenize(input);
    let types: Vec<_> = tokens.into_iter().map(|t| t.token).collect();
    assert!(types.contains(&Token::Stream));
    assert!(types.contains(&Token::As));
    assert!(types.contains(&Token::Arrow));
    assert!(types.contains(&Token::All));
    assert!(types.contains(&Token::Within));
    assert!(types.contains(&Token::Emit));
    assert!(types.contains(&Token::Duration("30s".to_string())));
}

// ---------------------------------------------------------------------------
// 97. Null literal in token stream
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_null_literal_in_context() {
    let tokens: Vec<_> = tokenize("x is null").into_iter().map(|t| t.token).collect();
    assert_eq!(tokens[0], Token::Ident("x".to_string()));
    assert_eq!(tokens[1], Token::Is);
    assert_eq!(tokens[2], Token::Null);
}

// ---------------------------------------------------------------------------
// 98. Multiple comments interspersed
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_multiple_comments() {
    let input = "# first\na # second\n# third\nb";
    let tokens: Vec<_> = tokenize(input).into_iter().map(|t| t.token).collect();
    assert_eq!(tokens[0], Token::Ident("a".to_string()));
    assert_eq!(tokens[1], Token::Ident("b".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

// ---------------------------------------------------------------------------
// 99. Connector configuration tokenization
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_connector_config() {
    let input = r#"connector my_mqtt = mqtt(host: "localhost", port: 1883, ssl: true)"#;
    let tokens = tokenize(input);
    let types: Vec<_> = tokens.into_iter().map(|t| t.token).collect();
    assert!(types.contains(&Token::Ident("connector".to_string())));
    assert!(types.contains(&Token::Ident("my_mqtt".to_string())));
    assert!(types.contains(&Token::String("localhost".to_string())));
    assert!(types.contains(&Token::Integer(1883)));
    assert!(types.contains(&Token::True));
}

// ---------------------------------------------------------------------------
// 100. Function declaration tokenization
// ---------------------------------------------------------------------------

#[test]
fn test_lexer_function_decl() {
    let input = "fn calc(a: int, b: float) -> float:\n  return a + b";
    let tokens = tokenize(input);
    let types: Vec<_> = tokens.into_iter().map(|t| t.token).collect();
    assert!(types.contains(&Token::Fn));
    assert!(types.contains(&Token::Arrow));
    assert!(types.contains(&Token::FloatType));
    assert!(types.contains(&Token::Return));
    assert!(types.contains(&Token::Plus));
}

// ============================================================================
// Parser Error Module Tests (error.rs coverage)
// ============================================================================

use varpulis_parser::error::{suggest_fix, ParseError, SourceLocation};

// ---------------------------------------------------------------------------
// SourceLocation::from_position
// ---------------------------------------------------------------------------

#[test]
fn source_location_start_of_input() {
    let loc = SourceLocation::from_position("hello world", 0);
    assert_eq!(loc.line, 1);
    assert_eq!(loc.column, 1);
    assert_eq!(loc.position, 0);
}

#[test]
fn source_location_middle_of_line() {
    let loc = SourceLocation::from_position("hello world", 5);
    assert_eq!(loc.line, 1);
    assert_eq!(loc.column, 6);
    assert_eq!(loc.position, 5);
}

#[test]
fn source_location_end_of_input() {
    let src = "hello";
    let loc = SourceLocation::from_position(src, src.len());
    assert_eq!(loc.line, 1);
    assert_eq!(loc.column, 6);
    assert_eq!(loc.position, 5);
}

#[test]
fn source_location_multiline_second_line() {
    let src = "line1\nline2";
    let loc = SourceLocation::from_position(src, 6);
    assert_eq!(loc.line, 2);
    assert_eq!(loc.column, 1);
    assert_eq!(loc.position, 6);
}

#[test]
fn source_location_multiline_third_line_middle() {
    let src = "aaa\nbbb\nccc";
    // Position 9 is 'c' at line 3, column 2 (after "aaa\nbbb\nc")
    let loc = SourceLocation::from_position(src, 9);
    assert_eq!(loc.line, 3);
    assert_eq!(loc.column, 2);
    assert_eq!(loc.position, 9);
}

#[test]
fn source_location_at_newline() {
    let src = "ab\ncd";
    // Position 2 is the '\n' character
    let loc = SourceLocation::from_position(src, 2);
    assert_eq!(loc.line, 1);
    assert_eq!(loc.column, 3);
    assert_eq!(loc.position, 2);
}

#[test]
fn source_location_empty_input() {
    let loc = SourceLocation::from_position("", 0);
    assert_eq!(loc.line, 1);
    assert_eq!(loc.column, 1);
    assert_eq!(loc.position, 0);
}

// ---------------------------------------------------------------------------
// ParseError Display impls (all variants)
// ---------------------------------------------------------------------------

#[test]
fn parse_error_display_located() {
    let err = ParseError::Located {
        line: 3,
        column: 10,
        position: 42,
        message: "unexpected token".to_string(),
        hint: Some("try this".to_string()),
    };
    let msg = format!("{}", err);
    assert_eq!(msg, "Line 3, column 10: unexpected token");
}

#[test]
fn parse_error_display_located_no_hint() {
    let err = ParseError::Located {
        line: 1,
        column: 1,
        position: 0,
        message: "syntax error".to_string(),
        hint: None,
    };
    let msg = format!("{}", err);
    assert_eq!(msg, "Line 1, column 1: syntax error");
}

#[test]
fn parse_error_display_unexpected_token() {
    let err = ParseError::UnexpectedToken {
        position: 15,
        expected: "identifier".to_string(),
        found: "number".to_string(),
    };
    let msg = format!("{}", err);
    assert_eq!(
        msg,
        "Unexpected token at position 15: expected identifier, found number"
    );
}

#[test]
fn parse_error_display_unexpected_eof() {
    let err = ParseError::UnexpectedEof;
    let msg = format!("{}", err);
    assert_eq!(msg, "Unexpected end of input");
}

#[test]
fn parse_error_display_invalid_token() {
    let err = ParseError::InvalidToken {
        position: 7,
        message: "bad char".to_string(),
    };
    let msg = format!("{}", err);
    assert_eq!(msg, "Invalid token at position 7: bad char");
}

#[test]
fn parse_error_display_invalid_number() {
    let err = ParseError::InvalidNumber("12.34.56".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Invalid number literal: 12.34.56");
}

#[test]
fn parse_error_display_invalid_duration() {
    let err = ParseError::InvalidDuration("5x".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Invalid duration literal: 5x");
}

#[test]
fn parse_error_display_invalid_timestamp() {
    let err = ParseError::InvalidTimestamp("not-a-date".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Invalid timestamp literal: not-a-date");
}

#[test]
fn parse_error_display_unterminated_string() {
    let err = ParseError::UnterminatedString(20);
    let msg = format!("{}", err);
    assert_eq!(msg, "Unterminated string starting at position 20");
}

#[test]
fn parse_error_display_invalid_escape() {
    let err = ParseError::InvalidEscape("\\q".to_string());
    let msg = format!("{}", err);
    assert_eq!(msg, "Invalid escape sequence: \\q");
}

#[test]
fn parse_error_display_custom() {
    let err = ParseError::Custom {
        span: varpulis_core::Span::new(0, 5),
        message: "custom error message".to_string(),
    };
    let msg = format!("{}", err);
    assert_eq!(msg, "custom error message");
}

// ---------------------------------------------------------------------------
// ParseError::at_location
// ---------------------------------------------------------------------------

#[test]
fn parse_error_at_location_with_hint() {
    let src = "abc\ndef";
    let err = ParseError::at_location(src, 4, "bad token", Some("try something".to_string()));
    match err {
        ParseError::Located {
            line,
            column,
            position,
            message,
            hint,
        } => {
            assert_eq!(line, 2);
            assert_eq!(column, 1);
            assert_eq!(position, 4);
            assert_eq!(message, "bad token");
            assert_eq!(hint, Some("try something".to_string()));
        }
        other => panic!("Expected Located, got {:?}", other),
    }
}

#[test]
fn parse_error_at_location_without_hint() {
    let src = "hello";
    let err = ParseError::at_location(src, 2, "oops", None);
    match err {
        ParseError::Located {
            line,
            column,
            position,
            message,
            hint,
        } => {
            assert_eq!(line, 1);
            assert_eq!(column, 3);
            assert_eq!(position, 2);
            assert_eq!(message, "oops");
            assert_eq!(hint, None);
        }
        other => panic!("Expected Located, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// ParseError::custom constructor
// ---------------------------------------------------------------------------

#[test]
fn parse_error_custom_constructor() {
    let span = varpulis_core::Span::new(10, 20);
    let err = ParseError::custom(span, "something went wrong");
    match err {
        ParseError::Custom { span: s, message } => {
            assert_eq!(s.start, 10);
            assert_eq!(s.end, 20);
            assert_eq!(message, "something went wrong");
        }
        other => panic!("Expected Custom, got {:?}", other),
    }
}

#[test]
fn parse_error_custom_constructor_string_arg() {
    let span = varpulis_core::Span::new(0, 1);
    let err = ParseError::custom(span, String::from("owned message"));
    match err {
        ParseError::Custom { message, .. } => {
            assert_eq!(message, "owned message");
        }
        other => panic!("Expected Custom, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// suggest_fix for all known tokens
// ---------------------------------------------------------------------------

#[test]
fn suggest_fix_string() {
    let fix = suggest_fix("string");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("str"));
}

#[test]
fn suggest_fix_string_uppercase() {
    let fix = suggest_fix("String");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("str"));
}

#[test]
fn suggest_fix_integer() {
    let fix = suggest_fix("integer");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("int"));
}

#[test]
fn suggest_fix_integer_uppercase() {
    let fix = suggest_fix("INTEGER");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("int"));
}

#[test]
fn suggest_fix_boolean() {
    let fix = suggest_fix("boolean");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("bool"));
}

#[test]
fn suggest_fix_boolean_mixed_case() {
    let fix = suggest_fix("Boolean");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("bool"));
}

#[test]
fn suggest_fix_double_ampersand() {
    let fix = suggest_fix("&&");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("and"));
}

#[test]
fn suggest_fix_double_pipe() {
    let fix = suggest_fix("||");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("or"));
}

#[test]
fn suggest_fix_exclamation() {
    let fix = suggest_fix("!");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("not"));
}

#[test]
fn suggest_fix_function() {
    let fix = suggest_fix("function");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("fn"));
}

#[test]
fn suggest_fix_func() {
    let fix = suggest_fix("func");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("fn"));
}

#[test]
fn suggest_fix_def() {
    let fix = suggest_fix("def");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("fn"));
}

#[test]
fn suggest_fix_class() {
    let fix = suggest_fix("class");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("event"));
}

#[test]
fn suggest_fix_struct() {
    let fix = suggest_fix("struct");
    assert!(fix.is_some());
    assert!(fix.unwrap().contains("event"));
}

#[test]
fn suggest_fix_unknown_token() {
    let fix = suggest_fix("foobar");
    assert!(fix.is_none());
}

#[test]
fn suggest_fix_empty_string() {
    let fix = suggest_fix("");
    assert!(fix.is_none());
}
