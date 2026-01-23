//! Main execution engine for Varpulis

use crate::aggregation::{AggResult, Aggregator, Avg, Count, Max, Min, Sum};
use crate::event::Event;
use crate::window::{SlidingWindow, TumblingWindow};
use chrono::Duration;
use crate::metrics::Metrics;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use varpulis_core::ast::{Program, Stmt, StreamOp, StreamSource};
use varpulis_core::Value;

/// Alert emitted by the engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_type: String,
    pub severity: String,
    pub message: String,
    pub data: IndexMap<String, Value>,
}

/// The main Varpulis engine
pub struct Engine {
    /// Registered stream definitions
    streams: HashMap<String, StreamDefinition>,
    /// Event type to stream mapping
    event_sources: HashMap<String, Vec<String>>,
    /// Alert sender
    alert_tx: mpsc::Sender<Alert>,
    /// Metrics
    events_processed: u64,
    alerts_generated: u64,
    /// Prometheus metrics
    metrics: Option<Metrics>,
}

/// Runtime stream definition
struct StreamDefinition {
    name: String,
    source: RuntimeSource,
    operations: Vec<RuntimeOp>,
}

enum RuntimeSource {
    EventType(String),
    Stream(String),
}

enum RuntimeOp {
    Where(Box<dyn Fn(&Event) -> bool + Send + Sync>),
    Window(WindowType),
    Aggregate(Aggregator),
    Emit(EmitConfig),
}

enum WindowType {
    Tumbling(TumblingWindow),
    Sliding(SlidingWindow),
}

struct EmitConfig {
    fields: Vec<(String, String)>, // (output_name, source_field or literal)
}

impl Engine {
    pub fn new(alert_tx: mpsc::Sender<Alert>) -> Self {
        Self {
            streams: HashMap::new(),
            event_sources: HashMap::new(),
            alert_tx,
            events_processed: 0,
            alerts_generated: 0,
            metrics: None,
        }
    }

    /// Enable Prometheus metrics
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Load a program into the engine
    pub fn load(&mut self, program: &Program) -> Result<(), String> {
        for stmt in &program.statements {
            match &stmt.node {
                Stmt::StreamDecl { name, source, ops, .. } => {
                    self.register_stream(name, source, ops)?;
                }
                Stmt::EventDecl { name, fields, .. } => {
                    info!("Registered event type: {} with {} fields", name, fields.len());
                }
                _ => {
                    debug!("Skipping statement: {:?}", stmt.node);
                }
            }
        }
        Ok(())
    }

    fn register_stream(
        &mut self,
        name: &str,
        source: &StreamSource,
        ops: &[StreamOp],
    ) -> Result<(), String> {
        let runtime_source = match source {
            StreamSource::From(event_type) => {
                self.event_sources
                    .entry(event_type.clone())
                    .or_default()
                    .push(name.to_string());
                RuntimeSource::EventType(event_type.clone())
            }
            StreamSource::Ident(stream_name) => {
                RuntimeSource::Stream(stream_name.clone())
            }
            _ => {
                return Err(format!("Unsupported source type for stream {}", name));
            }
        };

        let runtime_ops = self.compile_ops(ops)?;

        self.streams.insert(
            name.to_string(),
            StreamDefinition {
                name: name.to_string(),
                source: runtime_source,
                operations: runtime_ops,
            },
        );

        info!("Registered stream: {}", name);
        Ok(())
    }

    fn compile_ops(&self, ops: &[StreamOp]) -> Result<Vec<RuntimeOp>, String> {
        let mut runtime_ops = Vec::new();

        for op in ops {
            match op {
                StreamOp::Window(args) => {
                    // Parse duration from expression
                    // For MVP, assume it's a duration literal
                    let duration_ns = match &args.duration {
                        varpulis_core::ast::Expr::Duration(ns) => *ns,
                        _ => 300_000_000_000, // 5 minutes default
                    };
                    let duration = Duration::nanoseconds(duration_ns as i64);

                    if let Some(sliding) = &args.sliding {
                        let slide_ns = match sliding {
                            varpulis_core::ast::Expr::Duration(ns) => *ns,
                            _ => 60_000_000_000, // 1 minute default
                        };
                        let slide = Duration::nanoseconds(slide_ns as i64);
                        runtime_ops.push(RuntimeOp::Window(WindowType::Sliding(
                            SlidingWindow::new(duration, slide),
                        )));
                    } else {
                        runtime_ops.push(RuntimeOp::Window(WindowType::Tumbling(
                            TumblingWindow::new(duration),
                        )));
                    }
                }
                StreamOp::Aggregate(items) => {
                    let mut aggregator = Aggregator::new();
                    for item in items {
                        // Extract function name and argument from the expression
                        // Supports patterns like: func(arg) or func()
                        let (func_name, arg_expr) = match &item.expr {
                            varpulis_core::ast::Expr::Call { func, args } => {
                                let name = match func.as_ref() {
                                    varpulis_core::ast::Expr::Ident(s) => s.clone(),
                                    _ => continue,
                                };
                                let arg = args.first().and_then(|a| match a {
                                    varpulis_core::ast::Arg::Positional(e) => Some(e.clone()),
                                    _ => None,
                                });
                                (name, arg)
                            }
                            // For complex expressions, store as-is (runtime will evaluate)
                            _ => {
                                warn!("Complex aggregate expression not yet supported: {:?}", item.expr);
                                continue;
                            }
                        };
                        
                        let func: Box<dyn crate::aggregation::AggregateFunc> = match func_name.as_str() {
                            "count" => Box::new(Count),
                            "sum" => Box::new(Sum),
                            "avg" => Box::new(Avg),
                            "min" => Box::new(Min),
                            "max" => Box::new(Max),
                            "last" => Box::new(Max), // TODO: implement Last
                            "first" => Box::new(Min), // TODO: implement First
                            "stddev" => Box::new(Avg), // TODO: implement StdDev
                            _ => {
                                warn!("Unknown aggregation function: {}", func_name);
                                continue;
                            }
                        };
                        let field = arg_expr.as_ref().and_then(|e| match e {
                            varpulis_core::ast::Expr::Ident(s) => Some(s.clone()),
                            _ => None,
                        });
                        aggregator = aggregator.add(item.alias.clone(), func, field);
                    }
                    runtime_ops.push(RuntimeOp::Aggregate(aggregator));
                }
                StreamOp::Emit(args) => {
                    let fields: Vec<(String, String)> = args
                        .iter()
                        .filter_map(|arg| {
                            let value = match &arg.value {
                                varpulis_core::ast::Expr::Str(s) => s.clone(),
                                varpulis_core::ast::Expr::Ident(s) => s.clone(),
                                _ => return None,
                            };
                            Some((arg.name.clone(), value))
                        })
                        .collect();
                    runtime_ops.push(RuntimeOp::Emit(EmitConfig { fields }));
                }
                _ => {
                    debug!("Skipping operation: {:?}", op);
                }
            }
        }

        Ok(runtime_ops)
    }

    /// Process an incoming event
    pub async fn process(&mut self, event: Event) -> Result<(), String> {
        self.events_processed += 1;

        // Find streams that source from this event type
        let stream_names = self
            .event_sources
            .get(&event.event_type)
            .cloned()
            .unwrap_or_default();

        for stream_name in stream_names {
            if let Some(stream) = self.streams.get_mut(&stream_name) {
                let alerts = Self::process_stream_inner(stream, event.clone()).await?;
                for alert in alerts {
                    self.alerts_generated += 1;
                    if let Err(e) = self.alert_tx.send(alert).await {
                        warn!("Failed to send alert: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_stream_inner(stream: &mut StreamDefinition, event: Event) -> Result<Vec<Alert>, String> {
        let mut current_events = vec![event];
        let mut alerts = Vec::new();

        for op in &mut stream.operations {
            match op {
                RuntimeOp::Where(predicate) => {
                    current_events.retain(|e| predicate(e));
                    if current_events.is_empty() {
                        return Ok(alerts);
                    }
                }
                RuntimeOp::Window(window) => {
                    let mut window_results = Vec::new();
                    for event in current_events {
                        match window {
                            WindowType::Tumbling(w) => {
                                if let Some(completed) = w.add(event) {
                                    window_results = completed;
                                }
                            }
                            WindowType::Sliding(w) => {
                                if let Some(window_events) = w.add(event) {
                                    window_results = window_events;
                                }
                            }
                        }
                    }
                    current_events = window_results;
                    if current_events.is_empty() {
                        return Ok(alerts);
                    }
                }
                RuntimeOp::Aggregate(aggregator) => {
                    let result = aggregator.apply(&current_events);
                    // Create synthetic event from aggregation result
                    let mut agg_event = Event::new("AggregationResult");
                    for (key, value) in result {
                        agg_event.data.insert(key, value);
                    }
                    current_events = vec![agg_event];
                }
                RuntimeOp::Emit(config) => {
                    for event in &current_events {
                        let mut alert_data = IndexMap::new();
                        for (out_name, source) in &config.fields {
                            if let Some(value) = event.get(source) {
                                alert_data.insert(out_name.clone(), value.clone());
                            } else {
                                alert_data.insert(out_name.clone(), Value::Str(source.clone()));
                            }
                        }

                        let alert = Alert {
                            alert_type: "stream_output".to_string(),
                            severity: "info".to_string(),
                            message: format!("Output from stream {}", stream.name),
                            data: alert_data,
                        };
                        alerts.push(alert);
                    }
                }
            }
        }

        Ok(alerts)
    }

    /// Get metrics
    pub fn metrics(&self) -> EngineMetrics {
        EngineMetrics {
            events_processed: self.events_processed,
            alerts_generated: self.alerts_generated,
            streams_count: self.streams.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineMetrics {
    pub events_processed: u64,
    pub alerts_generated: u64,
    pub streams_count: usize,
}
