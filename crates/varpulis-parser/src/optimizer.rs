//! Rule-based logical plan optimizer
//!
//! Applies optimization rules to a `LogicalPlan` before physical materialization.
//! Rules are applied iteratively (up to `MAX_PASSES`) until no rule produces changes.
//!
//! # Current Rules
//!
//! - **FilterPushdown**: Move filters before windows/aggregates when safe
//! - **TemporalFilterPushdown**: Push time-based filters before windows
//! - **WindowMerge**: Merge adjacent windows with compatible configurations
//! - **ProjectionPruning**: Remove duplicate projections (keep last)

use varpulis_core::ast::Expr;
use varpulis_core::plan::{LogicalOp, LogicalPlan, LogicalStream};

/// Maximum optimization passes before stopping.
const MAX_PASSES: usize = 10;

/// A single optimization rule that can transform a logical stream.
pub trait OptimizationRule: Send + Sync {
    /// Human-readable name for logging/debugging.
    fn name(&self) -> &str;

    /// Apply the rule to a stream, returning `true` if any changes were made.
    fn apply(&self, stream: &mut LogicalStream) -> bool;
}

/// The plan optimizer: runs a set of rules iteratively until convergence.
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl Optimizer {
    /// Create an optimizer with the default set of rules.
    pub fn default_rules() -> Self {
        Self {
            rules: vec![
                Box::new(FilterPushdown),
                Box::new(TemporalFilterPushdown),
                Box::new(WindowMerge),
                Box::new(ProjectionPruning),
            ],
        }
    }

    /// Create an optimizer with a custom set of rules.
    pub fn with_rules(rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self { rules }
    }

    /// Optimize a logical plan by applying rules iteratively.
    ///
    /// Returns the optimized plan and the number of passes performed.
    pub fn optimize(&self, mut plan: LogicalPlan) -> (LogicalPlan, usize) {
        let mut total_passes = 0;

        for pass in 0..MAX_PASSES {
            let mut any_changed = false;

            for stream in &mut plan.streams {
                for rule in &self.rules {
                    if rule.apply(stream) {
                        any_changed = true;
                    }
                }
            }

            total_passes = pass + 1;
            if !any_changed {
                break;
            }
        }

        (plan, total_passes)
    }
}

/// Convenience function: optimize a plan with default rules.
pub fn optimize_plan(plan: LogicalPlan) -> LogicalPlan {
    let optimizer = Optimizer::default_rules();
    let (optimized, _) = optimizer.optimize(plan);
    optimized
}

// =============================================================================
// Rule: FilterPushdown
// =============================================================================

/// Move filter operations before windows and aggregates when safe.
///
/// A filter can be pushed before a window/aggregate if it only references
/// fields that are available before the window (i.e., input fields, not
/// aggregation output aliases).
struct FilterPushdown;

impl OptimizationRule for FilterPushdown {
    fn name(&self) -> &'static str {
        "FilterPushdown"
    }

    fn apply(&self, stream: &mut LogicalStream) -> bool {
        let ops = &mut stream.operations;
        let mut changed = false;

        // Scan for Filter ops that appear after a Window or Aggregate
        // and can be moved before them.
        let mut i = 0;
        while i < ops.len() {
            if let LogicalOp::Filter(expr) = &ops[i] {
                // Look backwards for a Window or Aggregate to push past
                if i > 0 {
                    let prev_idx = i - 1;
                    let can_push = match &ops[prev_idx] {
                        LogicalOp::Window(_) => !references_aggregation_output(expr),
                        _ => false,
                    };

                    if can_push {
                        ops.swap(prev_idx, i);
                        changed = true;
                        // Don't increment i — check if we can push further
                        continue;
                    }
                }
            }
            i += 1;
        }

        changed
    }
}

// =============================================================================
// Rule: TemporalFilterPushdown
// =============================================================================

/// Push time-based filters before windows.
///
/// Filters on timestamp fields can safely be pushed before window operations
/// to reduce the number of events entering the window.
struct TemporalFilterPushdown;

impl OptimizationRule for TemporalFilterPushdown {
    fn name(&self) -> &'static str {
        "TemporalFilterPushdown"
    }

    fn apply(&self, stream: &mut LogicalStream) -> bool {
        let ops = &mut stream.operations;
        let mut changed = false;

        let mut i = 0;
        while i < ops.len() {
            if let LogicalOp::Filter(expr) = &ops[i] {
                if is_temporal_filter(expr) && i > 0 {
                    let prev_idx = i - 1;
                    if matches!(ops[prev_idx], LogicalOp::Window(_)) {
                        ops.swap(prev_idx, i);
                        changed = true;
                        continue;
                    }
                }
            }
            i += 1;
        }

        changed
    }
}

// =============================================================================
// Rule: WindowMerge
// =============================================================================

/// Merge adjacent window operations with compatible configurations.
///
/// Two adjacent windows with the same duration and sliding parameters
/// can be merged into a single window, reducing processing overhead.
struct WindowMerge;

impl OptimizationRule for WindowMerge {
    fn name(&self) -> &'static str {
        "WindowMerge"
    }

    fn apply(&self, stream: &mut LogicalStream) -> bool {
        let ops = &mut stream.operations;
        if ops.len() < 2 {
            return false;
        }

        let mut changed = false;
        let mut i = 0;

        while i + 1 < ops.len() {
            let mergeable =
                if let (LogicalOp::Window(a), LogicalOp::Window(b)) = (&ops[i], &ops[i + 1]) {
                    // Same duration and sliding config → mergeable
                    a.duration == b.duration
                        && a.sliding == b.sliding
                        && a.session_gap == b.session_gap
                        && a.policy == b.policy
                } else {
                    false
                };

            if mergeable {
                ops.remove(i + 1);
                changed = true;
                // Don't increment — check if another window follows
            } else {
                i += 1;
            }
        }

        changed
    }
}

// =============================================================================
// Rule: ProjectionPruning
// =============================================================================

/// Remove redundant projections: if two adjacent Project ops exist,
/// keep only the second (it overwrites the first).
struct ProjectionPruning;

impl OptimizationRule for ProjectionPruning {
    fn name(&self) -> &'static str {
        "ProjectionPruning"
    }

    fn apply(&self, stream: &mut LogicalStream) -> bool {
        let ops = &mut stream.operations;
        if ops.len() < 2 {
            return false;
        }

        let mut changed = false;
        let mut i = 0;

        while i + 1 < ops.len() {
            let both_project = matches!(ops[i], LogicalOp::Project(_))
                && matches!(ops[i + 1], LogicalOp::Project(_));

            if both_project {
                ops.remove(i);
                changed = true;
                // Don't increment — the next pair might also be projections
            } else {
                i += 1;
            }
        }

        changed
    }
}

// =============================================================================
// Helper functions
// =============================================================================

/// Check if an expression references aggregation output aliases.
///
/// Simple heuristic: aggregation outputs typically use function call results
/// (count, sum, avg, etc.). If the filter only uses simple field accesses
/// and comparisons, it's safe to push before aggregation.
fn references_aggregation_output(expr: &Expr) -> bool {
    match expr {
        Expr::Call { .. } => true,
        Expr::Binary { left, right, .. } => {
            references_aggregation_output(left) || references_aggregation_output(right)
        }
        Expr::Unary { expr, .. } => references_aggregation_output(expr),
        Expr::Member { expr, .. } => references_aggregation_output(expr),
        _ => false,
    }
}

/// Check if a filter expression is temporal (references timestamp-like fields).
fn is_temporal_filter(expr: &Expr) -> bool {
    match expr {
        Expr::Binary { left, right, .. } => {
            is_timestamp_reference(left) || is_timestamp_reference(right)
        }
        _ => false,
    }
}

fn is_timestamp_reference(expr: &Expr) -> bool {
    match expr {
        Expr::Ident(name) => {
            let lower = name.to_lowercase();
            lower.contains("timestamp")
                || lower.contains("time")
                || lower == "ts"
                || lower == "event_time"
        }
        Expr::Member { member, .. } => {
            let lower = member.to_lowercase();
            lower.contains("timestamp")
                || lower.contains("time")
                || lower == "ts"
                || lower == "event_time"
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use varpulis_core::ast::{BinOp, Expr, SelectItem, WindowArgs};
    use varpulis_core::plan::{LogicalOp, LogicalPlan, LogicalSource, LogicalStream};

    fn make_plan(ops: Vec<LogicalOp>) -> LogicalPlan {
        LogicalPlan {
            streams: vec![LogicalStream {
                id: 0,
                name: "Test".to_string(),
                source: LogicalSource::EventType("E".to_string()),
                operations: ops,
                estimated_cardinality: None,
            }],
            functions: vec![],
            variables: vec![],
            connectors: vec![],
            patterns: vec![],
            events: vec![],
        }
    }

    fn window_op() -> LogicalOp {
        LogicalOp::Window(WindowArgs {
            duration: Expr::Duration(60_000_000_000), // 60s
            sliding: None,
            policy: None,
            session_gap: None,
        })
    }

    fn filter_op(name: &str) -> LogicalOp {
        LogicalOp::Filter(Expr::Binary {
            op: BinOp::Gt,
            left: Box::new(Expr::Ident(name.to_string())),
            right: Box::new(Expr::Int(100)),
        })
    }

    #[test]
    fn test_filter_pushdown_before_window() {
        let ops = vec![window_op(), filter_op("temperature")];
        let plan = make_plan(ops);

        let optimizer = Optimizer::default_rules();
        let (optimized, passes) = optimizer.optimize(plan);

        let ops = &optimized.streams[0].operations;
        assert!(
            matches!(ops[0], LogicalOp::Filter(_)),
            "Filter should be pushed before Window"
        );
        assert!(
            matches!(ops[1], LogicalOp::Window(_)),
            "Window should come after Filter"
        );
        assert!(passes >= 1);
    }

    #[test]
    fn test_filter_not_pushed_past_non_window() {
        let ops = vec![LogicalOp::Aggregate(vec![]), filter_op("temperature")];
        let plan = make_plan(ops);

        let optimizer = Optimizer::default_rules();
        let (optimized, _) = optimizer.optimize(plan);

        let ops = &optimized.streams[0].operations;
        // Filter should NOT be pushed past Aggregate (not window)
        assert!(matches!(ops[0], LogicalOp::Aggregate(_)));
        assert!(matches!(ops[1], LogicalOp::Filter(_)));
    }

    #[test]
    fn test_window_merge() {
        let ops = vec![
            LogicalOp::Window(WindowArgs {
                duration: Expr::Duration(60_000_000_000),
                sliding: None,
                policy: None,
                session_gap: None,
            }),
            LogicalOp::Window(WindowArgs {
                duration: Expr::Duration(60_000_000_000),
                sliding: None,
                policy: None,
                session_gap: None,
            }),
        ];
        let plan = make_plan(ops);

        let optimizer = Optimizer::default_rules();
        let (optimized, _) = optimizer.optimize(plan);

        assert_eq!(
            optimized.streams[0].operations.len(),
            1,
            "Two identical windows should merge into one"
        );
    }

    #[test]
    fn test_window_no_merge_different_duration() {
        let ops = vec![
            LogicalOp::Window(WindowArgs {
                duration: Expr::Duration(60_000_000_000),
                sliding: None,
                policy: None,
                session_gap: None,
            }),
            LogicalOp::Window(WindowArgs {
                duration: Expr::Duration(120_000_000_000),
                sliding: None,
                policy: None,
                session_gap: None,
            }),
        ];
        let plan = make_plan(ops);

        let optimizer = Optimizer::default_rules();
        let (optimized, _) = optimizer.optimize(plan);

        assert_eq!(
            optimized.streams[0].operations.len(),
            2,
            "Windows with different durations should not merge"
        );
    }

    #[test]
    fn test_projection_pruning() {
        let ops = vec![
            LogicalOp::Project(vec![SelectItem::Field("a".to_string())]),
            LogicalOp::Project(vec![
                SelectItem::Field("a".to_string()),
                SelectItem::Field("b".to_string()),
            ]),
        ];
        let plan = make_plan(ops);

        let optimizer = Optimizer::default_rules();
        let (optimized, _) = optimizer.optimize(plan);

        assert_eq!(
            optimized.streams[0].operations.len(),
            1,
            "Adjacent projections should be pruned to one"
        );
        if let LogicalOp::Project(items) = &optimized.streams[0].operations[0] {
            assert_eq!(items.len(), 2, "Should keep the second (final) projection");
        } else {
            panic!("Expected Project op");
        }
    }

    #[test]
    fn test_temporal_filter_pushdown() {
        let ops = vec![
            window_op(),
            LogicalOp::Filter(Expr::Binary {
                op: BinOp::Gt,
                left: Box::new(Expr::Ident("timestamp".to_string())),
                right: Box::new(Expr::Int(1000)),
            }),
        ];
        let plan = make_plan(ops);

        let optimizer = Optimizer::default_rules();
        let (optimized, _) = optimizer.optimize(plan);

        let ops = &optimized.streams[0].operations;
        assert!(
            matches!(ops[0], LogicalOp::Filter(_)),
            "Temporal filter should be pushed before Window"
        );
    }

    #[test]
    fn test_no_changes_returns_single_pass() {
        let ops = vec![filter_op("temperature"), window_op()];
        let plan = make_plan(ops);

        let optimizer = Optimizer::default_rules();
        let (_, passes) = optimizer.optimize(plan);

        assert_eq!(passes, 1, "No changes should result in a single pass");
    }

    #[test]
    fn test_optimize_plan_convenience() {
        let ops = vec![window_op(), filter_op("temperature")];
        let plan = make_plan(ops);

        let optimized = optimize_plan(plan);
        assert!(matches!(
            optimized.streams[0].operations[0],
            LogicalOp::Filter(_)
        ));
    }
}
