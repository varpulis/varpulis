//! `.within()` is a bound on event time, not on arrival time.
//!
//! Before this fix `SaseEngine` defaulted to `TimeSemantics::ProcessingTime`
//! and nothing in the VPL compiler ever selected the event-time branch, so a
//! run's WITHIN deadline was `Timestamp::now() + timeout` and expiry was
//! decided by `Timestamp::now()`. Replaying a log therefore satisfied every
//! temporal bound trivially — the whole file arrives inside a millisecond —
//! which is both a false-positive source (a two-hour gap matches `.within(1h)`)
//! and, for a slow live reader, a false-negative source.
//!
//! Every test here fails on the pre-fix engine and passes after; none of them
//! depends on anything outside the repository, so a missing fixture panics
//! rather than quietly skipping.

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;
use varpulis_runtime::sase::{
    EmissionMode, PatternBuilder, SaseEngine, SasePattern, TimeSemantics,
};
use varpulis_runtime::sase_persistence::RunCheckpointExt;

// =========================================================================
// Helpers
// =========================================================================

fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 10, 18, h, m, s).unwrap()
}

fn stamped(event_type: &str, ts: DateTime<Utc>) -> Event {
    Event::new(event_type).with_timestamp(ts)
}

/// Read a repository file, panicking loudly if it has moved — a regression
/// test that cannot find its fixture must fail, not pass by abstaining.
fn repo_file(path: &str) -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let full = root.join(path);
    std::fs::read_to_string(&full)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", full.display()))
}

fn run_vpl(code: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    engine.process_batch_sync(events).expect("process");
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

fn parse_events(source: &str) -> Vec<Event> {
    EventFileParser::parse(source)
        .expect("parse events")
        .into_iter()
        .map(|te| te.event)
        .collect()
}

/// `SEQ(Login, Transaction) WITHIN timeout`.
fn login_then_transaction(timeout: Duration) -> SasePattern {
    SasePattern::Within(
        Box::new(PatternBuilder::seq(vec![
            PatternBuilder::event("Login"),
            PatternBuilder::event("Transaction"),
        ])),
        timeout,
    )
}

// =========================================================================
// Engine default
// =========================================================================

#[test]
fn engine_defaults_to_event_time() {
    let engine = SaseEngine::new(PatternBuilder::event("A"));
    assert_eq!(
        engine.time_semantics(),
        TimeSemantics::EventTime,
        "a freshly built engine must measure WITHIN against event timestamps"
    );
}

#[test]
fn within_rejects_a_gap_that_exceeds_it_in_event_time() {
    // No `.with_event_time()` here on purpose: this asserts the DEFAULT.
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_secs(5)))
        .with_emission_mode(EmissionMode::Longest);

    engine.process(&stamped("Login", at(10, 0, 0)));
    let results = engine.process(&stamped("Transaction", at(10, 0, 10)));

    assert_eq!(
        results.len(),
        0,
        "a 10s gap must not satisfy .within(5s) however fast the events arrive"
    );
}

#[test]
fn within_still_accepts_a_gap_inside_the_bound() {
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_secs(5)))
        .with_emission_mode(EmissionMode::Longest);

    engine.process(&stamped("Login", at(10, 0, 0)));
    let results = engine.process(&stamped("Transaction", at(10, 0, 3)));

    assert_eq!(results.len(), 1, "a 3s gap is inside .within(5s)");
}

#[test]
fn within_boundary_is_inclusive() {
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_secs(5)))
        .with_emission_mode(EmissionMode::Longest);

    engine.process(&stamped("Login", at(10, 0, 0)));
    let results = engine.process(&stamped("Transaction", at(10, 0, 5)));

    assert_eq!(
        results.len(),
        1,
        "an event exactly on the deadline is still within the window"
    );
}

#[test]
fn processing_time_remains_available_as_an_opt_out() {
    // The escape hatch behind `VARPULIS_SASE_TIME=processing` must keep the
    // old behaviour intact: wall-clock arrival, so a replayed 10s gap matches.
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_secs(5)))
        .with_emission_mode(EmissionMode::Longest)
        .with_processing_time();

    assert_eq!(engine.time_semantics(), TimeSemantics::ProcessingTime);

    engine.process(&stamped("Login", at(10, 0, 0)));
    let results = engine.process(&stamped("Transaction", at(10, 0, 10)));

    assert_eq!(
        results.len(),
        1,
        "processing time ignores event timestamps by design"
    );
}

// =========================================================================
// Out-of-orderness must not widen the bound
// =========================================================================

#[test]
fn out_of_orderness_tolerance_does_not_widen_within() {
    // The watermark deliberately lags by `max_out_of_orderness` so runs stay
    // alive for late arrivals. That must not turn into extra WITHIN budget:
    // culling is watermark-driven, but *matching* is decided by the completing
    // event's own timestamp.
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_secs(5)))
        .with_emission_mode(EmissionMode::Longest)
        .with_max_out_of_orderness(Duration::from_mins(1));

    engine.process(&stamped("Login", at(10, 0, 0)));
    let results = engine.process(&stamped("Transaction", at(10, 0, 30)));

    assert_eq!(
        results.len(),
        0,
        "a 30s gap breaks .within(5s) even under 60s of out-of-orderness tolerance"
    );
}

#[test]
fn out_of_orderness_still_lets_a_late_but_in_window_event_complete() {
    // The counterpart: the run must survive long enough for an event that
    // arrives late but whose own timestamp is inside the window.
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_mins(1)))
        .with_emission_mode(EmissionMode::Longest)
        .with_max_out_of_orderness(Duration::from_mins(2));

    engine.process(&stamped("Login", at(10, 0, 0)));
    // A far-future event drags max_timestamp forward; the watermark lags by
    // 120s and so must not yet have expired the run.
    engine.process(&stamped("Noise", at(10, 1, 30)));
    let results = engine.process(&stamped("Transaction", at(10, 0, 30)));

    assert_eq!(
        results.len(),
        1,
        "an out-of-order event inside the window must still complete the run"
    );
}

// =========================================================================
// WITHIN must reach every run-creation path
// =========================================================================

#[test]
fn within_applies_to_a_pattern_that_starts_with_and() {
    // `WITHIN(AND(A, B))` puts the timeout on the AND state, which is reached
    // through the AND branch of `try_start_run_shared`. That branch used to
    // build the run without ever consulting `state.timeout`, so the bound was
    // dropped on the floor regardless of time semantics.
    let pattern = SasePattern::Within(
        Box::new(SasePattern::And(
            Box::new(PatternBuilder::event("A")),
            Box::new(PatternBuilder::event("B")),
        )),
        Duration::from_secs(5),
    );

    let mut engine = SaseEngine::new(pattern).with_emission_mode(EmissionMode::Longest);

    engine.process(&stamped("A", at(10, 0, 0)));
    let results = engine.process(&stamped("B", at(10, 0, 30)));

    assert_eq!(
        results.len(),
        0,
        "a 30s gap must not satisfy .within(5s) for an AND pattern either"
    );
}

// =========================================================================
// Checkpoint / restore
// =========================================================================

#[test]
fn event_time_deadline_survives_checkpoint_restore() {
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_secs(5)))
        .with_emission_mode(EmissionMode::Longest);
    engine.process(&stamped("Login", at(10, 0, 0)));

    let run = engine
        .runs
        .first()
        .expect("Login should have started a run");
    let restored = varpulis_runtime::sase::Run::from_checkpoint(&run.checkpoint());

    assert_eq!(
        restored.event_time_deadline,
        Some(at(10, 0, 5)),
        "the WITHIN deadline must survive a checkpoint"
    );
    assert!(
        restored.event_time_excludes(at(10, 0, 10)),
        "a restored run must still reject an event past its deadline"
    );
}

#[test]
fn processing_time_deadline_survives_checkpoint_restore() {
    // Under processing time the deadline is a wall-clock instant, so it is
    // serializable. `from_checkpoint` used to hard-code `deadline: None`,
    // which made every restored partial match immortal: a restart silently
    // erased the WITHIN bound instead of preserving it.
    let mut engine = SaseEngine::new(login_then_transaction(Duration::from_secs(5)))
        .with_emission_mode(EmissionMode::Longest)
        .with_processing_time();
    engine.process(&stamped("Login", at(10, 0, 0)));

    let run = engine
        .runs
        .first()
        .expect("Login should have started a run");
    let deadline = run
        .deadline
        .expect("processing-time run carries a deadline");
    let restored = varpulis_runtime::sase::Run::from_checkpoint(&run.checkpoint());

    let restored_deadline = restored
        .deadline
        .expect("the wall-clock deadline must survive a checkpoint");
    assert!(
        restored_deadline
            .as_datetime()
            .signed_duration_since(deadline.as_datetime())
            .num_milliseconds()
            .abs()
            <= 1,
        "restored deadline {restored_deadline:?} should match the original {deadline:?}"
    );
    assert!(
        restored
            .started_at
            .as_datetime()
            .signed_duration_since(run.started_at.as_datetime())
            .num_milliseconds()
            .abs()
            <= 1,
        "the run's start instant must survive too, so match durations stay honest"
    );
}

// =========================================================================
// Shipped artefacts
// =========================================================================

#[test]
fn shipped_temporal_constraints_example_matches_its_documented_outcome() {
    // examples/vpl-by-example/15_temporal_constraints.evt documents:
    //   traveler1:     US -> Nigeria in 45 minutes  -> MATCH
    //   slow_traveler: US -> Japan   in  2 hours    -> NO MATCH (exceeds 1h)
    let vpl = repo_file("examples/vpl-by-example/15_temporal_constraints.vpl");
    let events = parse_events(&repo_file(
        "examples/vpl-by-example/15_temporal_constraints.evt",
    ));

    let alerts = run_vpl(&vpl, events);

    let users: Vec<Option<&Value>> = alerts.iter().map(|e| e.data.get("user")).collect();
    assert_eq!(
        users,
        vec![Some(&Value::str("traveler1"))],
        "only the 45-minute hop is inside .within(1h); slow_traveler's 2h hop is not"
    );
}

#[test]
fn shipped_lateral_movement_rule_still_fires_on_its_own_data() {
    // Guard against over-correction: the real APT29 capture puts the SMB
    // connection 15 seconds before the remote exec, well inside .within(2m).
    let vpl = repo_file("examples/security-demo/detect_lateral_movement.vpl");
    let events = parse_events(&repo_file(
        "examples/security-demo/data/apt29_lateral_movement.jsonl",
    ));

    let alerts = run_vpl(&vpl, events);

    assert_eq!(
        alerts.len(),
        1,
        "the shipped lateral-movement detection must still fire on its own dataset"
    );
    assert_eq!(
        alerts[0].data.get("rule"),
        Some(&Value::str("lateral_movement_smb"))
    );
}

#[test]
fn shipped_lateral_movement_rule_stops_firing_when_the_chain_spans_days() {
    // Same events, same order, same fields — only the timestamps move, one
    // day apart. A `.within(2m)` rule must not call that lateral movement.
    let vpl = repo_file("examples/security-demo/detect_lateral_movement.vpl");
    let mut events = parse_events(&repo_file(
        "examples/security-demo/data/apt29_lateral_movement.jsonl",
    ));
    assert!(
        events.len() >= 3,
        "fixture should carry the full capture, got {} events",
        events.len()
    );
    for (i, event) in events.iter_mut().enumerate() {
        event.timestamp = at(10, 0, 0) + chrono::Duration::days(i as i64);
    }

    let alerts = run_vpl(&vpl, events);

    assert_eq!(
        alerts.len(),
        0,
        "a 24-hour gap must not satisfy .within(2m), got {alerts:?}"
    );
}

// =========================================================================
// .evt timing directives
// =========================================================================

#[test]
fn batch_zero_stamps_jsonl_events_from_the_epoch() {
    // `BATCH 0` is an explicit statement about event time. Testing
    // `current_batch_time > 0` instead read it as "no timing given", so a
    // JSONL line carrying no embedded timestamp kept the `Utc::now()` that
    // `parse_jsonl_line` had put there — landing decades *after* its
    // `BATCH 60000` sibling, which is stamped from the epoch. Event time is
    // meaningless on a file whose first event is newer than its last.
    // (`.evt`-format lines were already stamped from the epoch by
    // `parse_event_line`, so only the JSONL path exposes this.)
    let source = concat!(
        "BATCH 0\n",
        r#"{"event_type": "Login", "data": {"user_id": "a"}}"#,
        "\nBATCH 60000\n",
        r#"{"event_type": "Login", "data": {"user_id": "b"}}"#,
        "\n"
    );
    let events = EventFileParser::parse(source).expect("parse");

    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].event.timestamp,
        DateTime::UNIX_EPOCH,
        "the BATCH 0 event belongs at offset zero, not at wall-clock now"
    );
    assert_eq!(
        events[1].event.timestamp,
        DateTime::UNIX_EPOCH + chrono::Duration::seconds(60)
    );
    assert!(
        events[0].event.timestamp < events[1].event.timestamp,
        "a file's events must not run backwards in event time"
    );
}

#[test]
fn events_before_any_batch_directive_keep_their_own_timestamp() {
    // Regression guard for the fix above: a JSONL stream with embedded
    // timestamps and no BATCH directive must keep them untouched.
    let events = EventFileParser::parse(
        r#"{"EventID": 1, "@timestamp": "2020-10-18T07:50:05.917Z", "Image": "cmd.exe"}"#,
    )
    .expect("parse");

    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].event.timestamp,
        Utc.with_ymd_and_hms(2020, 10, 18, 7, 50, 5).unwrap() + chrono::Duration::milliseconds(917),
        "an embedded @timestamp must survive when the file declares no BATCH"
    );
}
