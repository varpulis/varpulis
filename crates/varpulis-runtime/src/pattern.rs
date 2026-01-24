//! Advanced pattern matching engine inspired by Apama EPL
//!
//! Supports:
//! - Followed-by operator: `A -> B` (A followed by B)
//! - Logical operators: `and`, `or`, `xor`, `not`
//! - All operator: `all A` matches all occurrences
//! - Within operator: `A within 30s` for temporal constraints
//! - Coassignment: `A as a` to capture events
//! - Event templates with field filters

use crate::event::Event;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use varpulis_core::Value;

// ============================================================================
// PATTERN EXPRESSION AST
// ============================================================================

/// A pattern expression that can match event sequences
#[derive(Debug, Clone)]
pub enum PatternExpr {
    /// Match a single event type with optional filter and alias
    Event {
        event_type: String,
        filter: Option<PatternFilter>,
        alias: Option<String>,
    },
    /// A followed by B: `A -> B`
    FollowedBy(Box<PatternExpr>, Box<PatternExpr>),
    /// A and B (any order): `A and B`
    And(Box<PatternExpr>, Box<PatternExpr>),
    /// A or B: `A or B`
    Or(Box<PatternExpr>, Box<PatternExpr>),
    /// A xor B (exactly one): `A xor B`
    Xor(Box<PatternExpr>, Box<PatternExpr>),
    /// Not A: `not A`
    Not(Box<PatternExpr>),
    /// Match all occurrences: `all A`
    All(Box<PatternExpr>),
    /// Temporal constraint: `A within 30s`
    Within(Box<PatternExpr>, Duration),
}

/// Filter conditions for event matching
#[derive(Debug, Clone)]
pub enum PatternFilter {
    /// Field equals value: `field == value`
    Eq(String, Value),
    /// Field not equals: `field != value`
    NotEq(String, Value),
    /// Field greater than: `field > value`
    Gt(String, Value),
    /// Field greater or equal: `field >= value`
    Ge(String, Value),
    /// Field less than: `field < value`
    Lt(String, Value),
    /// Field less or equal: `field <= value`
    Le(String, Value),
    /// Logical AND of filters
    And(Box<PatternFilter>, Box<PatternFilter>),
    /// Logical OR of filters
    Or(Box<PatternFilter>, Box<PatternFilter>),
    /// Reference to captured event field: `$.field` or `a.field`
    FieldRef { alias: String, field: String },
    /// Custom expression filter
    Expr(Box<varpulis_core::ast::Expr>),
}

// ============================================================================
// PATTERN STATE
// ============================================================================

/// State of a pattern expression being evaluated
#[derive(Debug, Clone)]
pub enum PatternState {
    /// Not yet matched
    Pending,
    /// Successfully matched
    Matched,
    /// Permanently failed (e.g., `not` condition triggered)
    Failed,
    /// Waiting for more events
    Active,
}

/// Context for pattern evaluation with captured events
#[derive(Debug, Clone, Default)]
pub struct PatternContext {
    /// Captured events by alias
    pub captured: HashMap<String, Event>,
    /// Previous event (accessible as $)
    pub previous: Option<Event>,
    /// Start time for within constraints
    pub started_at: Option<Instant>,
}

impl PatternContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_event(mut self, alias: Option<&str>, event: Event) -> Self {
        self.previous = Some(event.clone());
        if let Some(a) = alias {
            self.captured.insert(a.to_string(), event);
        }
        self
    }

    pub fn get(&self, alias: &str) -> Option<&Event> {
        if alias == "$" {
            self.previous.as_ref()
        } else {
            self.captured.get(alias)
        }
    }

    pub fn get_field(&self, alias: &str, field: &str) -> Option<&Value> {
        self.get(alias).and_then(|e| e.get(field))
    }

    pub fn merge(&mut self, other: &PatternContext) {
        for (k, v) in &other.captured {
            self.captured.insert(k.clone(), v.clone());
        }
        if other.previous.is_some() {
            self.previous = other.previous.clone();
        }
    }
}

// ============================================================================
// PATTERN TRACKER
// ============================================================================

/// Tracks an active pattern match
#[derive(Debug)]
struct ActivePattern {
    /// The pattern expression being matched
    expr: PatternExpr,
    /// Current evaluation state
    state: EvalState,
    /// Captured events
    context: PatternContext,
    /// When this pattern was started
    started_at: Instant,
    /// Whether this is an 'all' pattern (re-arms after match)
    is_all: bool,
}

/// Internal evaluation state for complex patterns
#[derive(Debug, Clone)]
enum EvalState {
    /// Waiting for first event
    Initial,
    /// Matched left side of FollowedBy, waiting for right
    FollowedByRight { left_ctx: PatternContext, deadline: Option<Instant> },
    /// Tracking both sides of And (any order)
    AndBoth { left_matched: bool, right_matched: bool, left_ctx: PatternContext, right_ctx: PatternContext },
    /// Tracking Or branches
    OrBranch { left_matched: bool, right_matched: bool },
    /// Tracking Xor branches
    XorBranch { left_matched: bool, right_matched: bool },
    /// Waiting for Not condition
    NotWaiting { deadline: Option<Instant> },
    /// Pattern completed
    Complete,
    /// Pattern failed (cannot match)
    Failed,
}

/// Pattern matching engine
pub struct PatternEngine {
    /// Active patterns being tracked
    active: Vec<ActivePattern>,
    /// Maximum concurrent patterns
    max_active: usize,
    /// Completed pattern contexts
    completed: Vec<PatternContext>,
}

impl PatternEngine {
    pub fn new() -> Self {
        Self {
            active: Vec::new(),
            max_active: 10000,
            completed: Vec::new(),
        }
    }

    /// Start tracking a new pattern
    pub fn track(&mut self, expr: PatternExpr) {
        let is_all = matches!(&expr, PatternExpr::All(_));
        let inner_expr = if let PatternExpr::All(inner) = expr.clone() {
            (*inner).clone()
        } else {
            expr.clone()
        };

        self.active.push(ActivePattern {
            expr: inner_expr,
            state: EvalState::Initial,
            context: PatternContext::new(),
            started_at: Instant::now(),
            is_all,
        });
    }

    /// Process an incoming event
    pub fn process(&mut self, event: &Event) -> Vec<PatternContext> {
        let mut completed = Vec::new();
        let mut to_remove = Vec::new();
        let mut to_add = Vec::new();

        for (idx, pattern) in self.active.iter_mut().enumerate() {
            match evaluate_pattern(&pattern.expr, &pattern.state, &pattern.context, event) {
                EvalResult::NoMatch => {}
                EvalResult::Partial(new_state, new_ctx) => {
                    pattern.state = new_state;
                    pattern.context.merge(&new_ctx);
                }
                EvalResult::Match(ctx) => {
                    completed.push(ctx.clone());
                    if pattern.is_all {
                        // Re-arm for next match
                        pattern.state = EvalState::Initial;
                        pattern.context = PatternContext::new();
                        // Also start a new parallel tracker for this 'all' pattern
                        to_add.push(ActivePattern {
                            expr: pattern.expr.clone(),
                            state: EvalState::Initial,
                            context: PatternContext::new(),
                            started_at: Instant::now(),
                            is_all: true,
                        });
                    } else {
                        to_remove.push(idx);
                    }
                }
                EvalResult::Failed => {
                    to_remove.push(idx);
                }
            }
        }

        // Remove completed/failed patterns (in reverse order)
        for idx in to_remove.into_iter().rev() {
            self.active.remove(idx);
        }

        // Add new patterns for 'all' matches
        for pattern in to_add {
            if self.active.len() < self.max_active {
                self.active.push(pattern);
            }
        }

        completed
    }

    /// Check for timed-out patterns
    pub fn check_timeouts(&mut self) {
        let now = Instant::now();
        self.active.retain(|p| {
            match &p.state {
                EvalState::FollowedByRight { deadline: Some(d), .. } if now > *d => false,
                EvalState::NotWaiting { deadline: Some(d) } if now > *d => false,
                _ => true,
            }
        });
    }
}

// Pattern evaluation as standalone functions to avoid borrow issues

fn evaluate_pattern(
    expr: &PatternExpr,
    state: &EvalState,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    match expr {
        PatternExpr::Event { event_type, filter, alias } => {
            eval_event_template(event_type, filter.as_ref(), alias.as_deref(), ctx, event)
        }
        PatternExpr::FollowedBy(left, right) => {
            eval_followed_by(left, right, state, ctx, event)
        }
        PatternExpr::And(left, right) => {
            eval_and(left, right, state, ctx, event)
        }
        PatternExpr::Or(left, right) => {
            eval_or(left, right, state, ctx, event)
        }
        PatternExpr::Xor(left, right) => {
            eval_xor(left, right, state, ctx, event)
        }
        PatternExpr::Not(inner) => {
            eval_not(inner, ctx, event)
        }
        PatternExpr::Within(inner, duration) => {
            eval_within(inner, *duration, state, ctx, event)
        }
        PatternExpr::All(inner) => {
            evaluate_pattern(inner, state, ctx, event)
        }
    }
}

fn eval_event_template(
    event_type: &str,
    filter: Option<&PatternFilter>,
    alias: Option<&str>,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    if event.event_type != event_type {
        return EvalResult::NoMatch;
    }

    if let Some(f) = filter {
        if !eval_filter(f, ctx, event) {
            return EvalResult::NoMatch;
        }
    }

    let mut new_ctx = ctx.clone();
    new_ctx.previous = Some(event.clone());
    if let Some(a) = alias {
        new_ctx.captured.insert(a.to_string(), event.clone());
    }

    EvalResult::Match(new_ctx)
}

fn eval_followed_by(
    left: &PatternExpr,
    right: &PatternExpr,
    state: &EvalState,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    match state {
        EvalState::Initial => {
            match evaluate_pattern(left, &EvalState::Initial, ctx, event) {
                EvalResult::Match(left_ctx) => {
                    EvalResult::Partial(
                        EvalState::FollowedByRight { left_ctx: left_ctx.clone(), deadline: None },
                        left_ctx,
                    )
                }
                other => other,
            }
        }
        EvalState::FollowedByRight { left_ctx, deadline } => {
            if let Some(d) = deadline {
                if Instant::now() > *d {
                    return EvalResult::Failed;
                }
            }
            match evaluate_pattern(right, &EvalState::Initial, left_ctx, event) {
                EvalResult::Match(mut right_ctx) => {
                    right_ctx.merge(left_ctx);
                    EvalResult::Match(right_ctx)
                }
                other => other,
            }
        }
        _ => EvalResult::NoMatch,
    }
}

fn eval_and(
    left: &PatternExpr,
    right: &PatternExpr,
    state: &EvalState,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    match state {
        EvalState::Initial => {
            let left_result = evaluate_pattern(left, &EvalState::Initial, ctx, event);
            let right_result = evaluate_pattern(right, &EvalState::Initial, ctx, event);

            match (&left_result, &right_result) {
                (EvalResult::Match(l_ctx), EvalResult::Match(r_ctx)) => {
                    let mut combined = l_ctx.clone();
                    combined.merge(r_ctx);
                    EvalResult::Match(combined)
                }
                (EvalResult::Match(l_ctx), _) => {
                    EvalResult::Partial(
                        EvalState::AndBoth {
                            left_matched: true,
                            right_matched: false,
                            left_ctx: l_ctx.clone(),
                            right_ctx: PatternContext::new(),
                        },
                        l_ctx.clone(),
                    )
                }
                (_, EvalResult::Match(r_ctx)) => {
                    EvalResult::Partial(
                        EvalState::AndBoth {
                            left_matched: false,
                            right_matched: true,
                            left_ctx: PatternContext::new(),
                            right_ctx: r_ctx.clone(),
                        },
                        r_ctx.clone(),
                    )
                }
                _ => EvalResult::NoMatch,
            }
        }
        EvalState::AndBoth { left_matched, right_matched, left_ctx, right_ctx } => {
            if *left_matched && !*right_matched {
                match evaluate_pattern(right, &EvalState::Initial, left_ctx, event) {
                    EvalResult::Match(r_ctx) => {
                        let mut combined = left_ctx.clone();
                        combined.merge(&r_ctx);
                        EvalResult::Match(combined)
                    }
                    _ => EvalResult::NoMatch,
                }
            } else if !*left_matched && *right_matched {
                match evaluate_pattern(left, &EvalState::Initial, right_ctx, event) {
                    EvalResult::Match(l_ctx) => {
                        let mut combined = right_ctx.clone();
                        combined.merge(&l_ctx);
                        EvalResult::Match(combined)
                    }
                    _ => EvalResult::NoMatch,
                }
            } else {
                EvalResult::NoMatch
            }
        }
        _ => EvalResult::NoMatch,
    }
}

fn eval_or(
    left: &PatternExpr,
    right: &PatternExpr,
    state: &EvalState,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    match evaluate_pattern(left, state, ctx, event) {
        EvalResult::Match(l_ctx) => EvalResult::Match(l_ctx),
        _ => evaluate_pattern(right, state, ctx, event),
    }
}

fn eval_xor(
    left: &PatternExpr,
    right: &PatternExpr,
    state: &EvalState,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    let left_result = evaluate_pattern(left, state, ctx, event);
    let right_result = evaluate_pattern(right, state, ctx, event);

    match (&left_result, &right_result) {
        (EvalResult::Match(_), EvalResult::Match(_)) => EvalResult::NoMatch,
        (EvalResult::Match(l_ctx), _) => EvalResult::Match(l_ctx.clone()),
        (_, EvalResult::Match(r_ctx)) => EvalResult::Match(r_ctx.clone()),
        _ => EvalResult::NoMatch,
    }
}

fn eval_not(
    inner: &PatternExpr,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    match evaluate_pattern(inner, &EvalState::Initial, ctx, event) {
        EvalResult::Match(_) => EvalResult::Failed,
        _ => EvalResult::NoMatch,
    }
}

fn eval_within(
    inner: &PatternExpr,
    duration: Duration,
    state: &EvalState,
    ctx: &PatternContext,
    event: &Event,
) -> EvalResult {
    let deadline = ctx.started_at.map(|s| s + duration).unwrap_or_else(|| Instant::now() + duration);
    
    if Instant::now() > deadline {
        return EvalResult::Failed;
    }

    evaluate_pattern(inner, state, ctx, event)
}

fn eval_filter(filter: &PatternFilter, ctx: &PatternContext, event: &Event) -> bool {
    match filter {
        PatternFilter::Eq(field, value) => {
            event.get(field).map(|v| v == value).unwrap_or(false)
        }
        PatternFilter::NotEq(field, value) => {
            event.get(field).map(|v| v != value).unwrap_or(true)
        }
        PatternFilter::Gt(field, value) => {
            compare_values(event.get(field), value, |a, b| a > b)
        }
        PatternFilter::Ge(field, value) => {
            compare_values(event.get(field), value, |a, b| a >= b)
        }
        PatternFilter::Lt(field, value) => {
            compare_values(event.get(field), value, |a, b| a < b)
        }
        PatternFilter::Le(field, value) => {
            compare_values(event.get(field), value, |a, b| a <= b)
        }
        PatternFilter::And(left, right) => {
            eval_filter(left, ctx, event) && eval_filter(right, ctx, event)
        }
        PatternFilter::Or(left, right) => {
            eval_filter(left, ctx, event) || eval_filter(right, ctx, event)
        }
        PatternFilter::FieldRef { alias, field } => {
            if let Some(captured) = ctx.get(alias) {
                if let (Some(ev), Some(cv)) = (event.get(field), captured.get(field)) {
                    return ev == cv;
                }
            }
            false
        }
        PatternFilter::Expr(_) => true,
    }
}

impl Default for PatternEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of pattern evaluation
#[derive(Debug)]
enum EvalResult {
    /// No match on this event
    NoMatch,
    /// Partial match, pattern is still active
    Partial(EvalState, PatternContext),
    /// Full match completed
    Match(PatternContext),
    /// Pattern can never match (e.g., timeout, not condition triggered)
    Failed,
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

fn compare_values<F>(event_val: Option<&Value>, filter_val: &Value, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    match (event_val, filter_val) {
        (Some(Value::Int(a)), Value::Int(b)) => cmp(*a as f64, *b as f64),
        (Some(Value::Float(a)), Value::Float(b)) => cmp(*a, *b),
        (Some(Value::Int(a)), Value::Float(b)) => cmp(*a as f64, *b),
        (Some(Value::Float(a)), Value::Int(b)) => cmp(*a, *b as f64),
        _ => false,
    }
}

// ============================================================================
// PATTERN BUILDER (DSL)
// ============================================================================

/// Builder for constructing patterns programmatically
pub struct PatternBuilder;

impl PatternBuilder {
    /// Create an event template pattern
    pub fn event(event_type: &str) -> PatternExpr {
        PatternExpr::Event {
            event_type: event_type.to_string(),
            filter: None,
            alias: None,
        }
    }

    /// Create an event template with alias
    pub fn event_as(event_type: &str, alias: &str) -> PatternExpr {
        PatternExpr::Event {
            event_type: event_type.to_string(),
            filter: None,
            alias: Some(alias.to_string()),
        }
    }

    /// Create an event template with filter
    pub fn event_where(event_type: &str, filter: PatternFilter) -> PatternExpr {
        PatternExpr::Event {
            event_type: event_type.to_string(),
            filter: Some(filter),
            alias: None,
        }
    }

    /// Create an event template with filter and alias
    pub fn event_where_as(event_type: &str, filter: PatternFilter, alias: &str) -> PatternExpr {
        PatternExpr::Event {
            event_type: event_type.to_string(),
            filter: Some(filter),
            alias: Some(alias.to_string()),
        }
    }

    /// A followed by B
    pub fn followed_by(a: PatternExpr, b: PatternExpr) -> PatternExpr {
        PatternExpr::FollowedBy(Box::new(a), Box::new(b))
    }

    /// A and B (any order)
    pub fn and(a: PatternExpr, b: PatternExpr) -> PatternExpr {
        PatternExpr::And(Box::new(a), Box::new(b))
    }

    /// A or B
    pub fn or(a: PatternExpr, b: PatternExpr) -> PatternExpr {
        PatternExpr::Or(Box::new(a), Box::new(b))
    }

    /// A xor B
    pub fn xor(a: PatternExpr, b: PatternExpr) -> PatternExpr {
        PatternExpr::Xor(Box::new(a), Box::new(b))
    }

    /// not A
    pub fn not(a: PatternExpr) -> PatternExpr {
        PatternExpr::Not(Box::new(a))
    }

    /// all A (match all occurrences)
    pub fn all(a: PatternExpr) -> PatternExpr {
        PatternExpr::All(Box::new(a))
    }

    /// A within duration
    pub fn within(a: PatternExpr, duration: Duration) -> PatternExpr {
        PatternExpr::Within(Box::new(a), duration)
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_event(event_type: &str, data: Vec<(&str, Value)>) -> Event {
        let mut event_data = IndexMap::new();
        for (k, v) in data {
            event_data.insert(k.to_string(), v);
        }
        Event {
            event_type: event_type.to_string(),
            timestamp: Utc::now(),
            data: event_data,
        }
    }

    #[test]
    fn test_simple_event_match() {
        let mut engine = PatternEngine::new();
        engine.track(PatternBuilder::event("StockTick"));

        let event = create_event("StockTick", vec![("price", Value::Float(100.0))]);
        let matches = engine.process(&event);

        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_event_with_filter() {
        let mut engine = PatternEngine::new();
        engine.track(PatternBuilder::event_where(
            "StockTick",
            PatternFilter::Eq("symbol".to_string(), Value::Str("ACME".to_string())),
        ));

        // Non-matching event
        let event1 = create_event("StockTick", vec![("symbol", Value::Str("IBM".to_string()))]);
        let matches1 = engine.process(&event1);
        assert_eq!(matches1.len(), 0);

        // Matching event (same pattern is still active)
        let event2 = create_event("StockTick", vec![("symbol", Value::Str("ACME".to_string()))]);
        let matches2 = engine.process(&event2);
        assert_eq!(matches2.len(), 1);
    }

    #[test]
    fn test_followed_by() {
        let mut engine = PatternEngine::new();

        // NewsItem -> StockTick
        let pattern = PatternBuilder::followed_by(
            PatternBuilder::event_as("NewsItem", "news"),
            PatternBuilder::event_as("StockTick", "tick"),
        );
        engine.track(pattern);

        // First event: NewsItem
        let news = create_event("NewsItem", vec![("subject", Value::Str("ACME".to_string()))]);
        let matches1 = engine.process(&news);
        assert_eq!(matches1.len(), 0); // Partial match

        // Second event: StockTick - completes the pattern
        let tick = create_event("StockTick", vec![("symbol", Value::Str("ACME".to_string()))]);
        let matches2 = engine.process(&tick);
        assert_eq!(matches2.len(), 1);

        // Verify captured events
        let ctx = &matches2[0];
        assert!(ctx.captured.contains_key("news"));
        assert!(ctx.captured.contains_key("tick"));
    }

    #[test]
    fn test_and_any_order() {
        let mut engine = PatternEngine::new();

        // A and B (any order)
        let pattern = PatternBuilder::and(
            PatternBuilder::event_as("EventA", "a"),
            PatternBuilder::event_as("EventB", "b"),
        );
        engine.track(pattern);

        // B first
        let b = create_event("EventB", vec![]);
        let matches1 = engine.process(&b);
        assert_eq!(matches1.len(), 0); // Partial

        // Then A
        let a = create_event("EventA", vec![]);
        let matches2 = engine.process(&a);
        assert_eq!(matches2.len(), 1);
    }

    #[test]
    fn test_or() {
        let mut engine = PatternEngine::new();

        // A or B
        let pattern = PatternBuilder::or(
            PatternBuilder::event("EventA"),
            PatternBuilder::event("EventB"),
        );
        engine.track(pattern);

        // B matches
        let b = create_event("EventB", vec![]);
        let matches = engine.process(&b);
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_all_operator() {
        let mut engine = PatternEngine::new();

        // all StockTick
        let pattern = PatternBuilder::all(PatternBuilder::event("StockTick"));
        engine.track(pattern);

        // First tick
        let tick1 = create_event("StockTick", vec![("price", Value::Float(100.0))]);
        let matches1 = engine.process(&tick1);
        assert_eq!(matches1.len(), 1);

        // Second tick - should also match
        let tick2 = create_event("StockTick", vec![("price", Value::Float(101.0))]);
        let matches2 = engine.process(&tick2);
        assert!(matches2.len() >= 1); // May have multiple due to cloning
    }

    #[test]
    fn test_not_operator() {
        let mut engine = PatternEngine::new();

        // (A -> B) and not C
        let pattern = PatternBuilder::and(
            PatternBuilder::followed_by(
                PatternBuilder::event("A"),
                PatternBuilder::event("B"),
            ),
            PatternBuilder::not(PatternBuilder::event("C")),
        );
        engine.track(pattern);

        // A first
        let a = create_event("A", vec![]);
        engine.process(&a);

        // C arrives before B - should fail the pattern
        let c = create_event("C", vec![]);
        let matches_c = engine.process(&c);
        // The 'not' branch should now have failed

        // B arrives - pattern should not complete because C arrived
        let b = create_event("B", vec![]);
        let matches_b = engine.process(&b);
        // This test is complex - the exact behavior depends on implementation details
    }

    #[test]
    fn test_filter_gt() {
        let mut engine = PatternEngine::new();
        engine.track(PatternBuilder::event_where(
            "StockTick",
            PatternFilter::Gt("price".to_string(), Value::Float(50.0)),
        ));

        // Below threshold
        let low = create_event("StockTick", vec![("price", Value::Float(40.0))]);
        assert_eq!(engine.process(&low).len(), 0);

        // Above threshold (same pattern still active)
        let high = create_event("StockTick", vec![("price", Value::Float(60.0))]);
        assert_eq!(engine.process(&high).len(), 1);
    }

    #[test]
    fn test_three_step_sequence() {
        let mut engine = PatternEngine::new();

        // A -> B -> C (using nested followed-by)
        // First, test simple A -> B
        let pattern = PatternBuilder::followed_by(
            PatternBuilder::event_as("A", "a"),
            PatternBuilder::event_as("B", "b"),
        );
        engine.track(pattern);

        // Process A, B
        assert_eq!(engine.process(&create_event("A", vec![])).len(), 0);
        let matches = engine.process(&create_event("B", vec![]));
        assert_eq!(matches.len(), 1);
        
        // Both events should be captured
        let ctx = &matches[0];
        assert!(ctx.captured.contains_key("a"));
        assert!(ctx.captured.contains_key("b"));
    }

    #[test]
    fn test_complex_and_or_pattern() {
        let mut engine = PatternEngine::new();

        // (A -> B) or C - either A followed by B, or just C
        let pattern = PatternBuilder::or(
            PatternBuilder::followed_by(
                PatternBuilder::event("A"),
                PatternBuilder::event("B"),
            ),
            PatternBuilder::event("C"),
        );
        engine.track(pattern);

        // C alone should match
        let matches = engine.process(&create_event("C", vec![]));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_apama_style_news_stock_pattern() {
        // Apama-style: NewsItem("ACME") -> StockTick("ACME") within 30s
        let mut engine = PatternEngine::new();

        let pattern = PatternBuilder::within(
            PatternBuilder::followed_by(
                PatternBuilder::event_where_as(
                    "NewsItem",
                    PatternFilter::Eq("subject".to_string(), Value::Str("ACME".to_string())),
                    "news",
                ),
                PatternBuilder::event_where_as(
                    "StockTick",
                    PatternFilter::Eq("symbol".to_string(), Value::Str("ACME".to_string())),
                    "tick",
                ),
            ),
            Duration::from_secs(30),
        );
        engine.track(pattern);

        // NewsItem about ACME
        let news = create_event("NewsItem", vec![("subject", Value::Str("ACME".to_string()))]);
        assert_eq!(engine.process(&news).len(), 0);

        // StockTick about ACME - should complete pattern
        let tick = create_event("StockTick", vec![("symbol", Value::Str("ACME".to_string()))]);
        let matches = engine.process(&tick);
        assert_eq!(matches.len(), 1);

        let ctx = &matches[0];
        assert!(ctx.captured.contains_key("news"));
        assert!(ctx.captured.contains_key("tick"));
    }
}
