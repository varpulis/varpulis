//! Logical planner: translates AST → LogicalPlan
//!
//! This is a straightforward 1:1 mapping from AST types to logical plan types.
//! No optimization or materialization occurs here.

use varpulis_core::ast::{Program, Stmt, StreamOp, StreamSource};
use varpulis_core::plan::{
    LogicalConnector, LogicalEvent, LogicalFunction, LogicalMergeSource, LogicalOp, LogicalPattern,
    LogicalPlan, LogicalSource, LogicalStream, LogicalVariable,
};

/// Translate a parsed VPL program into a logical plan.
///
/// This performs a 1:1 mapping of AST constructs to plan types. The resulting
/// plan can be inspected, optimized, and then materialized into a physical plan.
pub fn logical_plan(program: &Program) -> Result<LogicalPlan, String> {
    let mut streams = Vec::new();
    let mut functions = Vec::new();
    let mut variables = Vec::new();
    let mut connectors = Vec::new();
    let mut patterns = Vec::new();
    let mut events = Vec::new();
    let mut stream_id: u32 = 0;

    for stmt in &program.statements {
        match &stmt.node {
            Stmt::StreamDecl {
                name, source, ops, ..
            } => {
                let logical_source = translate_source(source)?;
                let logical_ops = translate_ops(ops)?;

                streams.push(LogicalStream {
                    id: stream_id,
                    name: name.clone(),
                    source: logical_source,
                    operations: logical_ops,
                    estimated_cardinality: None,
                });
                stream_id += 1;
            }

            Stmt::ConnectorDecl {
                name,
                connector_type,
                params,
            } => {
                connectors.push(LogicalConnector {
                    name: name.clone(),
                    connector_type: connector_type.clone(),
                    params: params.clone(),
                });
            }

            Stmt::FnDecl {
                name, params, ret, ..
            } => {
                functions.push(LogicalFunction {
                    name: name.clone(),
                    param_count: params.len(),
                    has_return_type: ret.is_some(),
                    param_types: params.iter().map(|p| p.ty.clone()).collect(),
                    return_type: ret.clone(),
                });
            }

            Stmt::VarDecl { mutable, name, .. } => {
                variables.push(LogicalVariable {
                    name: name.clone(),
                    is_mutable: *mutable,
                });
            }

            Stmt::ConstDecl { name, .. } => {
                variables.push(LogicalVariable {
                    name: name.clone(),
                    is_mutable: false,
                });
            }

            Stmt::PatternDecl {
                name,
                expr,
                within,
                partition_by,
            } => {
                patterns.push(LogicalPattern {
                    name: name.clone(),
                    expr: expr.clone(),
                    within: within.clone(),
                    partition_by: partition_by.clone(),
                });
            }

            Stmt::EventDecl {
                name,
                fields,
                extends,
            } => {
                events.push(LogicalEvent {
                    name: name.clone(),
                    field_count: fields.len(),
                    extends: extends.clone(),
                });
            }

            // Config, Import, Expr, control flow etc. are not part of the logical plan
            _ => {}
        }
    }

    Ok(LogicalPlan {
        streams,
        functions,
        variables,
        connectors,
        patterns,
        events,
    })
}

fn translate_source(source: &StreamSource) -> Result<LogicalSource, String> {
    match source {
        StreamSource::Ident(name) => Ok(LogicalSource::EventType(name.clone())),

        StreamSource::IdentWithAlias { name, .. } | StreamSource::AllWithAlias { name, .. } => {
            Ok(LogicalSource::EventType(name.clone()))
        }

        StreamSource::FromConnector {
            event_type,
            connector_name,
            params,
        } => Ok(LogicalSource::FromConnector {
            event_type: event_type.clone(),
            connector_name: connector_name.clone(),
            params: params.clone(),
        }),

        StreamSource::Merge(inline_decls) => {
            let sources = inline_decls
                .iter()
                .map(|d| LogicalMergeSource {
                    name: d.name.clone(),
                    source: d.source.clone(),
                    filter: d.filter.clone(),
                })
                .collect();
            Ok(LogicalSource::Merge(sources))
        }

        StreamSource::Join(clauses) => Ok(LogicalSource::Join(clauses.clone())),

        StreamSource::Sequence(decl) => Ok(LogicalSource::Sequence(decl.clone())),

        StreamSource::Timer(decl) => Ok(LogicalSource::Timer {
            interval: decl.interval.clone(),
            initial_delay: decl.initial_delay.as_deref().cloned(),
        }),
    }
}

fn translate_ops(ops: &[StreamOp]) -> Result<Vec<LogicalOp>, String> {
    let mut logical_ops = Vec::with_capacity(ops.len());

    for op in ops {
        let logical_op = match op {
            StreamOp::Where(expr) | StreamOp::Filter(expr) => LogicalOp::Filter(expr.clone()),
            StreamOp::Select(items) => LogicalOp::Project(items.clone()),
            StreamOp::Window(args) => LogicalOp::Window(args.clone()),
            StreamOp::Aggregate(items) => LogicalOp::Aggregate(items.clone()),
            StreamOp::Having(expr) => LogicalOp::Having(expr.clone()),
            StreamOp::PartitionBy(expr) => LogicalOp::PartitionBy(expr.clone()),
            StreamOp::Limit(expr) => LogicalOp::Limit(expr.clone()),
            StreamOp::Distinct(expr) => LogicalOp::Distinct(expr.clone()),
            StreamOp::Map(expr) => LogicalOp::Map(expr.clone()),
            StreamOp::Print(exprs) => LogicalOp::Print(exprs.clone()),
            StreamOp::Log(args) => LogicalOp::Log(args.clone()),
            StreamOp::Process(expr) => LogicalOp::Process(expr.clone()),
            StreamOp::Concurrent(args) => LogicalOp::Concurrent(args.clone()),
            StreamOp::Pattern(def) => LogicalOp::Pattern(def.clone()),
            StreamOp::Score(spec) => LogicalOp::Score(spec.clone()),
            StreamOp::Forecast(spec) => LogicalOp::Forecast(spec.clone()),
            StreamOp::Enrich(spec) => LogicalOp::Enrich(spec.clone()),
            StreamOp::TrendAggregate(items) => LogicalOp::TrendAggregate(items.clone()),
            StreamOp::OrderBy(items) => LogicalOp::OrderBy(items.clone()),
            StreamOp::Context(name) => LogicalOp::Context(name.clone()),
            StreamOp::Watermark(args) => LogicalOp::Watermark(args.clone()),
            StreamOp::AllowedLateness(expr) => LogicalOp::AllowedLateness(expr.clone()),
            StreamOp::Fork(paths) => LogicalOp::Fork(paths.clone()),

            StreamOp::Emit {
                output_type,
                fields,
                target_context,
            } => LogicalOp::Emit {
                output_type: output_type.clone(),
                fields: fields.clone(),
                target_context: target_context.clone(),
            },

            StreamOp::To {
                connector_name,
                params,
            } => LogicalOp::Sink {
                connector_name: connector_name.clone(),
                params: params.clone(),
            },

            StreamOp::FollowedBy(clause) => LogicalOp::FollowedBy(clause.clone()),
            StreamOp::Not(clause) => LogicalOp::Not(clause.clone()),
            StreamOp::Within(expr) => LogicalOp::Within(expr.clone()),

            // Operations that don't have a logical plan equivalent yet
            // (they affect materialization, not the logical plan)
            StreamOp::Tap(_)
            | StreamOp::ToExpr(_)
            | StreamOp::On(_)
            | StreamOp::OnError(_)
            | StreamOp::Collect
            | StreamOp::Any(_)
            | StreamOp::All
            | StreamOp::First => continue,
        };

        logical_ops.push(logical_op);
    }

    Ok(logical_ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use varpulis_core::ast::{Expr, SelectItem, StreamSource};
    use varpulis_core::span::{Span, Spanned};

    fn spanned<T>(node: T) -> Spanned<T> {
        Spanned {
            node,
            span: Span { start: 0, end: 0 },
        }
    }

    #[test]
    fn test_simple_stream() {
        let program = Program {
            statements: vec![spanned(Stmt::StreamDecl {
                name: "HighTemp".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("SensorReading".to_string()),
                ops: vec![
                    StreamOp::Where(Expr::Bool(true)),
                    StreamOp::Select(vec![SelectItem::Field("temperature".to_string())]),
                ],
                op_spans: vec![],
            })],
        };

        let plan = logical_plan(&program).unwrap();
        assert_eq!(plan.streams.len(), 1);
        assert_eq!(plan.streams[0].name, "HighTemp");
        assert_eq!(plan.streams[0].id, 0);
        assert!(matches!(
            plan.streams[0].source,
            LogicalSource::EventType(ref t) if t == "SensorReading"
        ));
        assert_eq!(plan.streams[0].operations.len(), 2);
        assert!(matches!(
            plan.streams[0].operations[0],
            LogicalOp::Filter(_)
        ));
        assert!(matches!(
            plan.streams[0].operations[1],
            LogicalOp::Project(_)
        ));
    }

    #[test]
    fn test_connector_and_function() {
        let program = Program {
            statements: vec![
                spanned(Stmt::ConnectorDecl {
                    name: "MyMqtt".to_string(),
                    connector_type: "mqtt".to_string(),
                    params: vec![],
                }),
                spanned(Stmt::FnDecl {
                    name: "double".to_string(),
                    params: vec![],
                    ret: None,
                    body: vec![],
                }),
            ],
        };

        let plan = logical_plan(&program).unwrap();
        assert_eq!(plan.connectors.len(), 1);
        assert_eq!(plan.connectors[0].name, "MyMqtt");
        assert_eq!(plan.functions.len(), 1);
        assert_eq!(plan.functions[0].name, "double");
    }

    #[test]
    fn test_explain_output() {
        let program = Program {
            statements: vec![spanned(Stmt::StreamDecl {
                name: "Alerts".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("Sensor".to_string()),
                ops: vec![
                    StreamOp::Where(Expr::Bool(true)),
                    StreamOp::Emit {
                        output_type: Some("Alert".to_string()),
                        fields: vec![],
                        target_context: None,
                    },
                ],
                op_spans: vec![],
            })],
        };

        let plan = logical_plan(&program).unwrap();
        let text = plan.explain();
        assert!(text.contains("[0] Alerts"));
        assert!(text.contains("Filter"));
        assert!(text.contains("Emit"));
    }
}
