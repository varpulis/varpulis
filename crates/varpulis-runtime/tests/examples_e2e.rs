//! Execution gate for the shipped `examples/` and `demos/` VPL files.
//!
//! Modelled on `security_demo_e2e.rs`: load the file we actually ship, feed it
//! the data we actually ship, and assert on the output. `varpulis check` only
//! *parses* — it cannot tell a working example from one that emits nothing, and
//! three shipped examples were silently emitting nothing.
//!
//! Three layers, all mandatory:
//!
//! 1. [`EXAMPLE_CASES`] — every `.vpl` that has a sibling `.evt` is run and its
//!    exact output count asserted. Exact, not `>= 1`: an example whose output
//!    count moves is a regression worth looking at.
//! 2. [`every_vpl_evt_pair_is_covered`] — walks the tree and fails if a pair
//!    exists that the table does not mention, so a new example cannot be added
//!    without an assertion.
//! 3. [`every_datafree_vpl_loads`] — every `.vpl` with no `.evt` (most of
//!    `demos/`) must still parse AND compile into the engine.
//!
//! Known-failing engine behaviour is recorded in [`KNOWN_FAILING_EXAMPLES`],
//! [`LOAD_EXEMPT`] and [`known_failing`] as assertions on the *broken*
//! behaviour, so the day the engine is fixed they go red and force the record
//! to be updated. Nothing is silently tolerated, and nothing here fixes the
//! engine — engine semantics are owned elsewhere.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

// ===========================================================================
// Harness
// ===========================================================================

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn load_file(rel: &str) -> String {
    let full = workspace_root().join(rel);
    std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("read {}: {e}", full.display()))
}

/// Run a VPL program over an `.evt` file exactly the way each example's own
/// header says to (`varpulis simulate -p X -e Y -v -w 1`), and collect what it
/// emits.
///
/// The finalisation below is not decoration: it mirrors
/// `crates/varpulis-cli/src/commands/simulate.rs` end for end. Drop the
/// watermark drain and 25_watermarks emits nothing; drop the session flush and
/// 08_session_window emits 1 instead of 3; drain with `try_recv` instead of a
/// bounded wait and 19_join emits nothing, because a join publishes from a
/// spawned task and the channel is still empty the instant
/// `process_batch_sync` returns. A harness missing these would report three
/// working examples as broken — the mirror image of the bug this file exists
/// to catch.
async fn run_example(vpl_rel: &str, evt_rel: &str) -> Vec<Event> {
    use std::time::Duration;

    let code = load_file(vpl_rel);
    let program = parse(&code).unwrap_or_else(|e| panic!("{vpl_rel}: parse failed: {e:?}"));

    let events: Vec<Event> = EventFileParser::parse(&load_file(evt_rel))
        .unwrap_or_else(|e| panic!("{evt_rel}: parse failed: {e:?}"))
        .into_iter()
        .map(|te| te.event)
        .collect();
    assert!(!events.is_empty(), "{evt_rel} contains no events");

    let (tx, mut rx) = mpsc::channel(8192);
    let mut engine = Engine::new(tx);
    engine
        .load(&program)
        .unwrap_or_else(|e| panic!("{vpl_rel}: engine load failed: {e:?}"));
    engine
        .process_batch_sync(events)
        .unwrap_or_else(|e| panic!("{vpl_rel}: processing failed: {e:?}"));

    // End-of-input drain: graduate still-open event-time windows.
    engine
        .flush_final_watermark()
        .await
        .unwrap_or_else(|e| panic!("{vpl_rel}: watermark drain failed: {e:?}"));
    if engine.has_session_windows() {
        engine
            .flush_expired_sessions()
            .await
            .unwrap_or_else(|e| panic!("{vpl_rel}: session flush failed: {e:?}"));
    }

    // Collect until the engine goes quiet. `rx.recv()` never returns None here
    // (the engine still holds a sender), so the idle timeout terminates.
    let mut out = Vec::new();
    while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
        out.push(e);
    }
    out
}

fn field<'a>(e: &'a Event, name: &str) -> Option<&'a Value> {
    e.data.get(name)
}

// ===========================================================================
// 1. Every shipped .vpl/.evt pair, with its exact output count
// ===========================================================================

/// `(vpl, evt, expected output count)`.
///
/// Every count here was measured against the shipped files. A zero is NOT
/// allowed: an example that emits nothing does not demonstrate anything, which
/// is exactly the failure this file exists to prevent.
const EXAMPLE_CASES: &[(&str, &str, usize)] = &[
    (
        "examples/transaction_monitoring.vpl",
        "examples/transaction_monitoring.evt",
        5,
    ),
    (
        "examples/vpl-by-example/01_hello_world.vpl",
        "examples/vpl-by-example/01_hello_world.evt",
        2,
    ),
    (
        "examples/vpl-by-example/02_filtering.vpl",
        "examples/vpl-by-example/02_filtering.evt",
        9,
    ),
    (
        "examples/vpl-by-example/03_event_declarations.vpl",
        "examples/vpl-by-example/03_event_declarations.evt",
        3,
    ),
    (
        "examples/vpl-by-example/04_field_selection.vpl",
        "examples/vpl-by-example/04_field_selection.evt",
        8,
    ),
    (
        "examples/vpl-by-example/05_tumbling_window.vpl",
        "examples/vpl-by-example/05_tumbling_window.evt",
        2,
    ),
    (
        "examples/vpl-by-example/06_count_window.vpl",
        "examples/vpl-by-example/06_count_window.evt",
        2,
    ),
    (
        "examples/vpl-by-example/07_sliding_window.vpl",
        "examples/vpl-by-example/07_sliding_window.evt",
        3,
    ),
    (
        "examples/vpl-by-example/08_session_window.vpl",
        "examples/vpl-by-example/08_session_window.evt",
        3,
    ),
    (
        "examples/vpl-by-example/09_basic_aggregations.vpl",
        "examples/vpl-by-example/09_basic_aggregations.evt",
        1,
    ),
    (
        "examples/vpl-by-example/10_partitioned_aggregations.vpl",
        "examples/vpl-by-example/10_partitioned_aggregations.evt",
        2,
    ),
    (
        "examples/vpl-by-example/11_simple_sequence.vpl",
        "examples/vpl-by-example/11_simple_sequence.evt",
        2,
    ),
    (
        "examples/vpl-by-example/12_multi_step_sequence.vpl",
        "examples/vpl-by-example/12_multi_step_sequence.evt",
        1,
    ),
    (
        "examples/vpl-by-example/13_kleene_plus.vpl",
        "examples/vpl-by-example/13_kleene_plus.evt",
        3,
    ),
    (
        "examples/vpl-by-example/14_negation.vpl",
        "examples/vpl-by-example/14_negation.evt",
        1,
    ),
    (
        "examples/vpl-by-example/15_temporal_constraints.vpl",
        "examples/vpl-by-example/15_temporal_constraints.evt",
        2,
    ),
    (
        "examples/vpl-by-example/16_partition_by_patterns.vpl",
        "examples/vpl-by-example/16_partition_by_patterns.evt",
        4,
    ),
    (
        "examples/vpl-by-example/17_reusable_patterns.vpl",
        "examples/vpl-by-example/17_reusable_patterns.evt",
        4,
    ),
    (
        "examples/vpl-by-example/18_match_all.vpl",
        "examples/vpl-by-example/18_match_all.evt",
        3,
    ),
    (
        "examples/vpl-by-example/20_merge.vpl",
        "examples/vpl-by-example/20_merge.evt",
        6,
    ),
    (
        "examples/vpl-by-example/21_functions.vpl",
        "examples/vpl-by-example/21_functions.evt",
        3,
    ),
    (
        "examples/vpl-by-example/22_conditional_expressions.vpl",
        "examples/vpl-by-example/22_conditional_expressions.evt",
        3,
    ),
    (
        "examples/vpl-by-example/23_forecasting.vpl",
        "examples/vpl-by-example/23_forecasting.evt",
        6,
    ),
    (
        "examples/vpl-by-example/24_trend_aggregation.vpl",
        "examples/vpl-by-example/24_trend_aggregation.evt",
        1,
    ),
    (
        "examples/vpl-by-example/25_watermarks.vpl",
        "examples/vpl-by-example/25_watermarks.evt",
        1,
    ),
    (
        "examples/vpl-by-example/27_strictly_increasing.vpl",
        "examples/vpl-by-example/27_strictly_increasing.evt",
        1,
    ),
    (
        "examples/vpl-by-example/28_strictly_decreasing.vpl",
        "examples/vpl-by-example/28_strictly_decreasing.evt",
        1,
    ),
    (
        "examples/vpl-by-example/29_seq_vs_arrow.vpl",
        "examples/vpl-by-example/29_seq_vs_arrow.evt",
        2,
    ),
    (
        "examples/vpl-by-example/30_emission_modes.vpl",
        "examples/vpl-by-example/30_emission_modes.evt",
        11,
    ),
    (
        "examples/vpl-by-example/31_array_semantics.vpl",
        "examples/vpl-by-example/31_array_semantics.evt",
        1,
    ),
];

/// Examples whose VPL is correct but which the ENGINE currently drops on the
/// floor. `(vpl, evt, observed_now, intended, why)`.
///
/// These assert the BROKEN count. When the engine is fixed the assertion trips,
/// which is the point — the entry then moves into [`EXAMPLE_CASES`] with its
/// intended count. Nothing here is a licence to leave an example unexercised.
const KNOWN_FAILING_EXAMPLES: &[(&str, &str, usize, usize, &str)] = &[(
    "examples/vpl-by-example/19_join.vpl",
    "examples/vpl-by-example/19_join.evt",
    0,
    4,
    "ENGINE: the synchronous dispatch path bails out of joins.      crates/varpulis-runtime/src/engine/dispatch.rs,      `process_stream_with_functions_sync`: `if matches!(stream.source,      RuntimeSource::Join(_)) { return ...empty... }` (// join requires async in      some paths). `varpulis simulate -w 1` takes that path whenever the program      has no `.to()` sink, so a join example can never emit. The VPL itself is      correct: run the same file against a build whose sync path handles joins      and it emits the 4 rows below.",
)];

#[tokio::test]
async fn known_failing_examples_still_fail() {
    for (vpl, evt, observed, intended, why) in KNOWN_FAILING_EXAMPLES {
        let out = run_example(vpl, evt).await;
        assert_eq!(
            out.len(),
            *observed,
            "KNOWN-FAILING CASE MOVED: {vpl} now emits {} event(s), not the              recorded {observed}. If it emits the intended {intended}, delete              its KNOWN_FAILING_EXAMPLES entry and add it to EXAMPLE_CASES.\n\n             Recorded cause: {why}",
            out.len()
        );
    }
}

#[tokio::test]
async fn every_example_emits_its_expected_output() {
    let mut failures: Vec<String> = Vec::new();

    for (vpl, evt, expected) in EXAMPLE_CASES {
        assert_ne!(
            *expected, 0,
            "{vpl}: an expectation of 0 outputs is never acceptable — an example \
             that emits nothing demonstrates nothing. Fix the example."
        );
        let out = run_example(vpl, evt).await;
        if out.len() != *expected {
            failures.push(format!(
                "  {vpl}: expected {expected} output event(s), got {}",
                out.len()
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "shipped examples no longer produce their documented output:\n{}",
        failures.join("\n")
    );
}

// ===========================================================================
// 2. Coverage — no example may exist without an assertion
// ===========================================================================

fn collect_vpl_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                // Vendored/derived trees carry no shipped examples.
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if name == "node_modules" || name == "target" || name == "dist" {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("vpl") {
                out.push(path);
            }
        }
    }

    let root = workspace_root();
    let mut out = Vec::new();
    walk(&root.join("examples"), &mut out);
    walk(&root.join("demos"), &mut out);
    out.sort();
    out
}

fn rel(path: &Path) -> String {
    path.strip_prefix(workspace_root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn every_vpl_evt_pair_is_covered() {
    let declared: BTreeSet<&str> = EXAMPLE_CASES
        .iter()
        .map(|(vpl, _, _)| *vpl)
        .chain(KNOWN_FAILING_EXAMPLES.iter().map(|(vpl, _, _, _, _)| *vpl))
        .collect();

    let mut uncovered = Vec::new();
    for vpl in collect_vpl_files() {
        if vpl.with_extension("evt").exists() {
            let r = rel(&vpl);
            if !declared.contains(r.as_str()) {
                uncovered.push(r);
            }
        }
    }

    assert!(
        uncovered.is_empty(),
        "these examples ship an .evt but no output assertion — add them to \
         EXAMPLE_CASES so they are actually executed:\n  {}",
        uncovered.join("\n  ")
    );

    // The reverse direction: a table entry whose files vanished would otherwise
    // pass silently by never being reached.
    let listed = EXAMPLE_CASES
        .iter()
        .map(|(vpl, evt, _)| (*vpl, *evt))
        .chain(
            KNOWN_FAILING_EXAMPLES
                .iter()
                .map(|(vpl, evt, _, _, _)| (*vpl, *evt)),
        );
    for (vpl, evt) in listed {
        assert!(
            workspace_root().join(vpl).exists(),
            "the example tables list {vpl}, which does not exist"
        );
        assert!(
            workspace_root().join(evt).exists(),
            "the example tables list {evt}, which does not exist"
        );
    }
}

// ===========================================================================
// 3. Data-free examples must still compile into the engine
// ===========================================================================

/// `.vpl` files that cannot be loaded into a bare engine here, each with its
/// reason. Kept explicit so "it does not load" is always a recorded decision,
/// never an omission — and [`every_datafree_vpl_loads`] fails if one of these
/// starts loading, so the list cannot rot.
const LOAD_EXEMPT: &[(&str, &str)] = &[
    (
        "examples/enrich_refdata.vpl",
        "needs the `database` cargo feature for its .enrich() provider; this          test crate is built without it. Not a defect in the example.",
    ),
    // ENGINE GAP: `.tap()` is not implemented — the engine rejects it at load
    // with "`.tap()` is not yet implemented — use .print() or .log()". It
    // appears in no page under docs/language/, so these five shipped examples
    // demonstrate an operator that does not exist. Left as-is rather than
    // rewritten: turning `.tap(counter: ..., labels: [...])` into `.print()`
    // would change what each example claims to show, and the operator is an
    // engine decision, not a docs one.
    (
        "examples/financial_markets.vpl",
        "ENGINE: uses `.tap()`, which is not implemented (line 617)",
    ),
    (
        "examples/forecast_cybersecurity.vpl",
        "ENGINE: uses `.tap()`, which is not implemented",
    ),
    (
        "examples/forecast_fraud.vpl",
        "ENGINE: uses `.tap()`, which is not implemented (line 249)",
    ),
    (
        "examples/forecast_iot.vpl",
        "ENGINE: uses `.tap()`, which is not implemented",
    ),
    (
        "examples/hvac_demo.vpl",
        "ENGINE: uses `.tap()`, which is not implemented (line 400)",
    ),
];

#[test]
fn every_datafree_vpl_loads() {
    let exempt: BTreeSet<&str> = LOAD_EXEMPT.iter().map(|(p, _)| *p).collect();
    let mut failures = Vec::new();

    for vpl in collect_vpl_files() {
        if vpl.with_extension("evt").exists() {
            continue; // covered by EXAMPLE_CASES, which loads *and* runs it
        }
        let r = rel(&vpl);
        let source = std::fs::read_to_string(&vpl).expect("read vpl");
        let loaded = match parse(&source) {
            Ok(program) => {
                let (tx, _rx) = mpsc::channel(16);
                let mut engine = Engine::new(tx);
                engine.load(&program).map_err(|e| format!("{e:?}"))
            }
            Err(e) => Err(format!("parse failed: {e:?}")),
        };

        match (exempt.contains(r.as_str()), loaded) {
            (false, Err(e)) => failures.push(format!("  {r}: {e}")),
            (true, Ok(())) => failures.push(format!(
                "  {r}: LISTED IN LOAD_EXEMPT BUT NOW LOADS — remove the entry                  (recorded reason: {})",
                LOAD_EXEMPT
                    .iter()
                    .find(|(p, _)| *p == r)
                    .map_or("", |(_, why)| why)
            )),
            _ => {}
        }
    }

    assert!(
        failures.is_empty(),
        "shipped VPL files that do not compile (or exemptions that are now \
         stale):\n{}\n\nExempt today:\n{}",
        failures.join("\n"),
        LOAD_EXEMPT
            .iter()
            .map(|(p, why)| format!("  {p}: {why}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ===========================================================================
// Content assertions for the three examples that used to emit nothing
// ===========================================================================

#[tokio::test]
async fn negation_example_cancels_the_run_the_cancellation_touched() {
    // Was: `-> Payment ... .not(Payment ...)`, self-contradictory, 0 outputs.
    let out = run_example(
        "examples/vpl-by-example/14_negation.vpl",
        "examples/vpl-by-example/14_negation.evt",
    )
    .await;
    assert_eq!(out.len(), 1, "exactly the uncancelled order should confirm");
    let e = &out[0];
    assert_eq!(field(e, "alert"), Some(&Value::str("order_confirmed")));
    assert_eq!(field(e, "order_id"), Some(&Value::Int(1)));
    assert_eq!(field(e, "customer"), Some(&Value::str("Alice")));
    assert_eq!(
        field(e, "amount"),
        Some(&Value::Float(100.0)),
        "the payment amount must be carried through"
    );
    // Order 2 was cancelled before its payment arrived.
    assert!(
        out.iter()
            .all(|e| field(e, "order_id") != Some(&Value::Int(2))),
        "the cancelled order must not be confirmed"
    );
}

#[tokio::test]
async fn forecasting_example_predicts_before_the_pattern_completes() {
    // Was: `warmup: 50` against a 9-event file — structurally unreachable, so
    // the PST never left warmup and the stream emitted nothing.
    let out = run_example(
        "examples/vpl-by-example/23_forecasting.vpl",
        "examples/vpl-by-example/23_forecasting.evt",
    )
    .await;
    assert_eq!(out.len(), 6, "8 training runs, warmup 3 => 6 predictions");
    for e in &out {
        assert_eq!(field(e, "alert"), Some(&Value::str("PREDICTED_TAKEOVER")));
        let Some(Value::Float(p)) = field(e, "probability") else {
            panic!("forecast alert without a probability: {:?}", e.data);
        };
        assert!(
            *p > 0.6,
            "the .where(forecast_probability > 0.6) filter must hold, got {p}"
        );
        assert!(
            matches!(field(e, "severity"), Some(Value::Str(s)) if &**s == "CRITICAL" || &**s == "HIGH"),
            "severity must be derived from the probability: {:?}",
            e.data
        );
    }
}

// ===========================================================================
// Known-failing engine behaviour
//
// These assert the CURRENTLY BROKEN behaviour. When the engine is fixed they
// go red, which is the point: the record has to be updated deliberately rather
// than the gap being forgotten. Engine semantics are owned elsewhere — this
// file only records what is observed.
// ===========================================================================

mod known_failing {
    use super::*;

    /// KNOWN FAILING (varpulis-runtime, observed on v0.11.0): the synchronous
    /// dispatch path drops joins, so 19_join emits nothing through
    /// `varpulis simulate -w 1`.
    ///
    /// The example itself was ALSO broken and has been repaired: it ended at
    /// `.select()` with no `.emit()` (a join computes the row but publishes
    /// nothing without one), and its two source streams were windowed
    /// aggregates with no `.emit()` over an input too short for their 5s
    /// windows to close. With those fixed it produces the 4 rows asserted
    /// below on a build whose sync path handles joins.
    ///
    /// What remains is the engine bail-out at
    /// `engine/dispatch.rs::process_stream_with_functions_sync` —
    /// `if matches!(stream.source, RuntimeSource::Join(_)) { return empty }`.
    /// Not fixed here: engine semantics are owned elsewhere.
    #[tokio::test]
    async fn sync_dispatch_drops_joins() {
        let out = run_example(
            "examples/vpl-by-example/19_join.vpl",
            "examples/vpl-by-example/19_join.evt",
        )
        .await;
        assert!(
            out.is_empty(),
            "KNOWN-FAILING CASE FIXED: the sync dispatch path now emits joins \
             ({} event(s)). Move 19_join from KNOWN_FAILING_EXAMPLES into \
             EXAMPLE_CASES with its intended count of 4, and replace this test \
             with the positive assertions (zone/temperature/humidity/comfort on \
             every row, and both zones represented).",
            out.len()
        );
    }

    /// KNOWN FAILING (varpulis-runtime, observed on v0.11.0): on a stream that
    /// uses `.forecast()`, `.emit()` resolves literals and the `forecast_*`
    /// variables but NOT the sequence aliases, so `user: login.user_id`,
    /// `ip: login.ip` and `amount: tx.amount` are dropped from the alert
    /// instead of being filled in.
    ///
    /// The example is written the way a user would reasonably write it and is
    /// left that way on purpose; this test pins the gap. When the engine
    /// resolves aliases through `.forecast()`, this test fails — delete it and
    /// tighten `forecasting_example_predicts_before_the_pattern_completes`
    /// to assert the alias fields are present.
    #[tokio::test]
    async fn forecast_emit_drops_pattern_aliases() {
        let out = run_example(
            "examples/vpl-by-example/23_forecasting.vpl",
            "examples/vpl-by-example/23_forecasting.evt",
        )
        .await;
        assert!(!out.is_empty(), "forecast example must emit something");
        for e in &out {
            assert!(
                field(e, "user").is_none()
                    && field(e, "ip").is_none()
                    && field(e, "amount").is_none(),
                "KNOWN-FAILING CASE FIXED: `.forecast()` now resolves pattern aliases in \
                 `.emit()` ({:?}). Remove this test and assert the fields positively in \
                 forecasting_example_predicts_before_the_pattern_completes, and drop the \
                 KNOWN LIMITATION note from examples/vpl-by-example/23_forecasting.vpl.",
                e.data
            );
        }
    }
}
