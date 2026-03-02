//! Deterministic benchmarks for the SASE+ engine using iai-callgrind.
//!
//! These benchmarks use Valgrind's callgrind tool to measure instruction counts
//! rather than wall-clock time, producing perfectly reproducible results
//! regardless of system load, CPU frequency scaling, or background processes.
//!
//! Run with:
//!   cargo bench -p varpulis-sase --bench iai_sase
//!
//! Prerequisites:
//!   - `valgrind` must be installed on the system
//!   - `iai-callgrind-runner` must be installed:
//!     cargo install iai-callgrind-runner
//!
//! Benchmark groups:
//!   - nfa_construction: NFA compilation from SASE patterns
//!   - event_processing: Single-event processing through the engine
//!   - pattern_matching: Full pattern match evaluation over event sequences

#![allow(missing_docs)]

use std::hint::black_box;
use std::time::Duration;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use varpulis_core::Event;
use varpulis_sase::{
    CompareOp, NfaCompiler, Predicate, SaseEngine, SasePattern, SelectionStrategy,
};

// ============================================================================
// Setup helpers (called outside the measured region)
// ============================================================================

/// Build a simple SEQ(A, B) pattern.
fn make_seq_ab_pattern() -> SasePattern {
    SasePattern::Seq(vec![
        SasePattern::Event {
            event_type: "A".to_string(),
            predicate: None,
            alias: Some("a".to_string()),
        },
        SasePattern::Event {
            event_type: "B".to_string(),
            predicate: None,
            alias: Some("b".to_string()),
        },
    ])
}

/// Build a SEQ(A, B, C, D, E) pattern.
fn make_seq_5_pattern() -> SasePattern {
    SasePattern::Seq(
        ["A", "B", "C", "D", "E"]
            .iter()
            .map(|t| SasePattern::Event {
                event_type: t.to_string(),
                predicate: None,
                alias: Some(t.to_lowercase()),
            })
            .collect(),
    )
}

/// Build a SEQ(A, B+, C) pattern with Kleene+ on B.
fn make_kleene_pattern() -> SasePattern {
    SasePattern::Seq(vec![
        SasePattern::Event {
            event_type: "A".to_string(),
            predicate: None,
            alias: Some("a".to_string()),
        },
        SasePattern::KleenePlus(Box::new(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: None,
            alias: Some("b".to_string()),
        })),
        SasePattern::Event {
            event_type: "C".to_string(),
            predicate: None,
            alias: Some("c".to_string()),
        },
    ])
}

/// Build a SEQ(Order[value > 500], Payment) pattern with a predicate.
fn make_predicate_pattern() -> SasePattern {
    SasePattern::Seq(vec![
        SasePattern::Event {
            event_type: "Order".to_string(),
            predicate: Some(Predicate::Compare {
                field: "value".to_string(),
                op: CompareOp::Gt,
                value: varpulis_core::Value::Int(500),
            }),
            alias: Some("order".to_string()),
        },
        SasePattern::Event {
            event_type: "Payment".to_string(),
            predicate: None,
            alias: Some("payment".to_string()),
        },
    ])
}

/// Build a WITHIN(SEQ(A, B), 5min) pattern.
fn make_within_pattern() -> SasePattern {
    SasePattern::Within(
        Box::new(SasePattern::Seq(vec![
            SasePattern::Event {
                event_type: "A".to_string(),
                predicate: None,
                alias: Some("a".to_string()),
            },
            SasePattern::Event {
                event_type: "B".to_string(),
                predicate: None,
                alias: Some("b".to_string()),
            },
        ])),
        Duration::from_secs(300),
    )
}

/// Create a pre-built engine with a SEQ(A, B) pattern ready for event processing.
fn setup_engine_seq_ab() -> (SaseEngine, Event) {
    let engine =
        SaseEngine::new(make_seq_ab_pattern()).with_strategy(SelectionStrategy::SkipTillNextMatch);
    let event = Event::new("A").with_field("id", 1_i64);
    (engine, event)
}

/// Create a pre-built engine that already consumed an "A" event, ready for "B".
fn setup_engine_with_active_run() -> (SaseEngine, Event) {
    let mut engine =
        SaseEngine::new(make_seq_ab_pattern()).with_strategy(SelectionStrategy::SkipTillNextMatch);
    let event_a = Event::new("A").with_field("id", 1_i64);
    let _ = engine.process(&event_a);
    let event_b = Event::new("B").with_field("id", 2_i64);
    (engine, event_b)
}

/// Create a pre-built engine with Kleene+ pattern and an "A" already consumed.
fn setup_engine_kleene_active() -> (SaseEngine, Event) {
    let mut engine =
        SaseEngine::new(make_kleene_pattern()).with_strategy(SelectionStrategy::SkipTillNextMatch);
    let event_a = Event::new("A").with_field("id", 1_i64);
    let _ = engine.process(&event_a);
    let event_b = Event::new("B").with_field("id", 2_i64);
    (engine, event_b)
}

/// Create a pre-built engine with a predicate pattern ready for an Order event.
fn setup_engine_predicate() -> (SaseEngine, Event) {
    let engine = SaseEngine::new(make_predicate_pattern())
        .with_strategy(SelectionStrategy::StrictContiguous);
    let event = Event::new("Order")
        .with_field("value", 750_i64)
        .with_field("id", 1_i64);
    (engine, event)
}

/// Generate a small event sequence that produces a complete match: [A, B].
fn setup_full_match_seq() -> (SaseEngine, Vec<Event>) {
    let engine =
        SaseEngine::new(make_seq_ab_pattern()).with_strategy(SelectionStrategy::SkipTillNextMatch);
    let events = vec![
        Event::new("A").with_field("id", 1_i64),
        Event::new("B").with_field("id", 2_i64),
    ];
    (engine, events)
}

/// Generate a 10-event sequence [A, B, C, D, E, A, B, C, D, E] for a 5-step pattern.
fn setup_long_match_seq() -> (SaseEngine, Vec<Event>) {
    let engine =
        SaseEngine::new(make_seq_5_pattern()).with_strategy(SelectionStrategy::SkipTillNextMatch);
    let types = ["A", "B", "C", "D", "E", "A", "B", "C", "D", "E"];
    let events: Vec<Event> = types
        .iter()
        .enumerate()
        .map(|(i, t)| Event::new(*t).with_field("id", i as i64))
        .collect();
    (engine, events)
}

/// Generate a Kleene+ match sequence: [A, B, B, B, C].
fn setup_kleene_match_seq() -> (SaseEngine, Vec<Event>) {
    let engine =
        SaseEngine::new(make_kleene_pattern()).with_strategy(SelectionStrategy::SkipTillNextMatch);
    let events = vec![
        Event::new("A").with_field("id", 1_i64),
        Event::new("B").with_field("id", 2_i64),
        Event::new("B").with_field("id", 3_i64),
        Event::new("B").with_field("id", 4_i64),
        Event::new("C").with_field("id", 5_i64),
    ];
    (engine, events)
}

// ============================================================================
// Group 1: NFA Construction
// ============================================================================

#[library_benchmark]
fn nfa_compile_seq_ab() {
    let pattern = make_seq_ab_pattern();
    black_box(NfaCompiler::new().compile(black_box(&pattern)));
}

#[library_benchmark]
fn nfa_compile_seq_5() {
    let pattern = make_seq_5_pattern();
    black_box(NfaCompiler::new().compile(black_box(&pattern)));
}

#[library_benchmark]
fn nfa_compile_kleene() {
    let pattern = make_kleene_pattern();
    black_box(NfaCompiler::new().compile(black_box(&pattern)));
}

#[library_benchmark]
fn nfa_compile_predicate() {
    let pattern = make_predicate_pattern();
    black_box(NfaCompiler::new().compile(black_box(&pattern)));
}

#[library_benchmark]
fn nfa_compile_within() {
    let pattern = make_within_pattern();
    black_box(NfaCompiler::new().compile(black_box(&pattern)));
}

library_benchmark_group!(
    name = nfa_construction;
    benchmarks =
        nfa_compile_seq_ab,
        nfa_compile_seq_5,
        nfa_compile_kleene,
        nfa_compile_predicate,
        nfa_compile_within
);

// ============================================================================
// Group 2: Single-Event Processing
// ============================================================================

#[library_benchmark]
#[bench::start_run(setup_engine_seq_ab())]
fn process_single_event_start_run((mut engine, event): (SaseEngine, Event)) {
    // Processes an "A" event that starts a new run.
    black_box(engine.process(black_box(&event)));
}

#[library_benchmark]
#[bench::complete_run(setup_engine_with_active_run())]
fn process_single_event_complete_run((mut engine, event): (SaseEngine, Event)) {
    // Processes a "B" event that completes a partial run -> produces a match.
    black_box(engine.process(black_box(&event)));
}

#[library_benchmark]
#[bench::kleene_accumulate(setup_engine_kleene_active())]
fn process_single_event_kleene((mut engine, event): (SaseEngine, Event)) {
    // Processes a "B" event on a Kleene+ state (self-loop accumulation).
    black_box(engine.process(black_box(&event)));
}

#[library_benchmark]
#[bench::predicate_eval(setup_engine_predicate())]
fn process_single_event_predicate((mut engine, event): (SaseEngine, Event)) {
    // Processes an Order event with value > 500 predicate.
    black_box(engine.process(black_box(&event)));
}

library_benchmark_group!(
    name = event_processing;
    benchmarks =
        process_single_event_start_run,
        process_single_event_complete_run,
        process_single_event_kleene,
        process_single_event_predicate
);

// ============================================================================
// Group 3: Full Pattern Match Evaluation
// ============================================================================

#[library_benchmark]
#[bench::seq_ab(setup_full_match_seq())]
fn match_full_sequence_ab((mut engine, events): (SaseEngine, Vec<Event>)) {
    // Process [A, B] -> should produce 1 match.
    for event in &events {
        black_box(engine.process(black_box(event)));
    }
}

#[library_benchmark]
#[bench::seq_5(setup_long_match_seq())]
fn match_full_sequence_5((mut engine, events): (SaseEngine, Vec<Event>)) {
    // Process [A,B,C,D,E,A,B,C,D,E] through a 5-step pattern.
    for event in &events {
        black_box(engine.process(black_box(event)));
    }
}

#[library_benchmark]
#[bench::kleene_match(setup_kleene_match_seq())]
fn match_kleene_sequence((mut engine, events): (SaseEngine, Vec<Event>)) {
    // Process [A, B, B, B, C] through SEQ(A, B+, C).
    for event in &events {
        black_box(engine.process(black_box(event)));
    }
}

library_benchmark_group!(
    name = pattern_matching;
    benchmarks =
        match_full_sequence_ab,
        match_full_sequence_5,
        match_kleene_sequence
);

// ============================================================================
// Entrypoint
// ============================================================================

main!(
    library_benchmark_groups = nfa_construction,
    event_processing,
    pattern_matching
);
