//! `.trend_aggregate()` counts the trends the pattern matches.
//!
//! It used to feed the events to an aggregator that knew the pattern's
//! event types and nothing else: the closure's condition, the start's
//! condition and the partition were ignored, the count came out after every
//! event as a running total, `min`/`max` returned the trend count,
//! `count_events()` the number of events, and the result carried no field
//! of the events. On the tutorial's five ticks it printed 15 where the
//! definitions give 24.

mod common;

use common::{at, end, feed, only};
use varpulis_engine::Program;

const TICKS: &str = r"
event StockTick:
    symbol: str
    price: float
";

fn tick(time: &str, symbol: &str, price: f64) -> String {
    at(
        time,
        &format!(r#""symbol": "{symbol}", "price": {price:?}"#),
    )
}

/// The tutorial's stream, with every aggregate and what `.emit()` can read.
fn rising(closure: &str) -> String {
    format!(
        r"{TICKS}
stream RisingTrends = StockTick as first
    -> all StockTick {closure} as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(
        trends: count_trends(),
        events: count_events(rising),
        every: count_events(),
        total: sum_trends(rising.price),
        mean: avg_trends(rising.price),
        low: min_trends(rising.price),
        high: max_trends(rising.price)
    )
    .emit(
        symbol: symbol,
        start: first.price,
        trends: trends,
        events: events,
        every: every,
        total: total,
        mean: mean,
        low: low,
        high: high,
        distinct: count(rising)
    )
"
    )
}

const FIVE: [f64; 5] = [100.0, 105.0, 110.0, 108.0, 115.0];

fn feed_five(p: &mut Program) {
    for (i, price) in FIVE.into_iter().enumerate() {
        let out = feed(
            &mut *p,
            "StockTick",
            &tick(&format!("10:00:0{i}"), "AAPL", price),
        );
        assert!(
            out.is_empty(),
            "nothing comes out before the window closes: {out:#?}"
        );
    }
}

/// `price > first.price`: from 100, every non-empty subset of the four
/// later ticks, 15; from 105, the subsets of {110, 108, 115}, 7; from 110
/// and from 108, {115}, one each. 24 trends. 105 takes part in 8 of them,
/// 110 and 108 in 12 each, 115 in 14: 46 participations of `rising`, plus
/// one start per trend, 70 in all. The sum is 8·105 + 12·110 + 12·108 +
/// 14·115 = 5066.
#[test]
fn every_start_counts_and_every_subset_of_the_closure_is_a_trend() {
    let mut p = Program::compile(&rising("where price > first.price")).unwrap();
    feed_five(&mut p);
    let out = only(end(&mut p));
    assert_eq!(out["event_type"], "RisingTrends");
    assert_eq!(out["symbol"], "AAPL");
    assert_eq!(out["start"], 100.0, "the earliest start");
    assert_eq!(out["trends"], 24);
    assert_eq!(out["events"], 46);
    assert_eq!(out["every"], 70);
    assert_eq!(out["total"], 5066.0);
    let mean = out["mean"].as_f64().unwrap();
    assert!((mean - 5066.0 / 24.0).abs() < 1e-9, "{mean}");
    assert_eq!(out["low"], 105.0);
    assert_eq!(out["high"], 115.0);
    assert_eq!(out["distinct"], 4, "four ticks take part in a trend");
    assert_eq!(
        out["timestamp"], "2026-10-14T10:01:00Z",
        "stamped with the window's end"
    );
}

/// `price > rising.price` compares each tick with the one before it in
/// the trend, the start included: 108 cannot follow 110. From 100 the
/// rising chains in {105, 110, 108, 115} are 11; from 105, 5; from 110 and
/// 108, one each: 18.
#[test]
fn a_self_reference_compares_with_the_previous_event_of_the_trend() {
    let mut p = Program::compile(&rising("where price > rising.price")).unwrap();
    feed_five(&mut p);
    let out = only(end(&mut p));
    assert_eq!(out["trends"], 18);
    assert_eq!(out["high"], 115.0);
}

/// `.increasing(price)` is that same condition.
#[test]
fn increasing_is_the_same_condition() {
    let program = rising("").replace(
        "-> all StockTick  as rising",
        "-> all StockTick.increasing(price) as rising",
    );
    assert!(program.contains(".increasing(price)"), "{program}");
    let mut p = Program::compile(&program).unwrap();
    feed_five(&mut p);
    assert_eq!(only(end(&mut p))["trends"], 18);
}

/// Without a condition every later tick extends every trend: from 100 the
/// 15 subsets, from 105 7, from 110 3, from 108 1: 26.
#[test]
fn without_a_condition_every_later_event_qualifies() {
    let mut p = Program::compile(&rising("")).unwrap();
    feed_five(&mut p);
    assert_eq!(only(end(&mut p))["trends"], 26);
}

/// Falling prices give a window with events and no trend: it still
/// reports, with zero counts and null extremes.
#[test]
fn a_window_without_a_trend_reports_zero() {
    let mut p = Program::compile(&rising("where price > first.price")).unwrap();
    for (i, price) in [100.0, 90.0, 80.0].into_iter().enumerate() {
        feed(
            &mut p,
            "StockTick",
            &tick(&format!("10:00:0{i}"), "AAPL", price),
        );
    }
    let out = only(end(&mut p));
    assert_eq!(out["trends"], 0);
    assert_eq!(out["events"], 0);
    assert_eq!(out["total"], 0.0);
    assert!(out["mean"].is_null(), "{out}");
    assert!(out["low"].is_null(), "{out}");
    assert!(out["high"].is_null(), "{out}");
    assert_eq!(out["distinct"], 0);
}

/// Partitions are separate windows. AAPL's closes when its own tick
/// arrives a minute on; MSFT's, whose ticks stopped, closes on the event
/// clock in the same feed; the window the late AAPL tick opened closes at
/// the end of the input.
#[test]
fn partitions_close_on_arrival_on_the_clock_and_at_the_end() {
    let mut p = Program::compile(&rising("where price > first.price")).unwrap();
    assert!(feed(&mut p, "StockTick", &tick("10:00:00", "AAPL", 100.0)).is_empty());
    assert!(feed(&mut p, "StockTick", &tick("10:00:01", "MSFT", 10.0)).is_empty());
    assert!(feed(&mut p, "StockTick", &tick("10:00:02", "AAPL", 105.0)).is_empty());
    assert!(feed(&mut p, "StockTick", &tick("10:00:03", "MSFT", 12.0)).is_empty());
    assert!(feed(&mut p, "StockTick", &tick("10:00:04", "MSFT", 11.0)).is_empty());
    let out = feed(&mut p, "StockTick", &tick("10:01:30", "AAPL", 1.0));
    assert_eq!(out.len(), 2, "{out:#?}");
    assert_eq!(out[0]["symbol"], "AAPL");
    assert_eq!(out[0]["trends"], 1);
    assert_eq!(out[0]["timestamp"], "2026-10-14T10:01:00Z");
    assert_eq!(out[1]["symbol"], "MSFT");
    assert_eq!(out[1]["trends"], 3, "from 10: {{12}}, {{11}}, {{12, 11}}");
    assert_eq!(out[1]["timestamp"], "2026-10-14T10:01:01Z");
    assert!(feed(&mut p, "StockTick", &tick("10:01:40", "AAPL", 2.0)).is_empty());
    let last = only(end(&mut p));
    assert_eq!(last["symbol"], "AAPL");
    assert_eq!(last["trends"], 1);
    assert_eq!(last["timestamp"], "2026-10-14T10:02:30Z");
}

/// The result goes on down the pipeline: `.where()` reads the aggregates
/// and `.emit()` the start's fields.
#[test]
fn the_result_is_filtered_and_projected_downstream() {
    let program = format!(
        r"{TICKS}
stream BusySymbols = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .where(count > 20)
    .emit(symbol: first.symbol, trends: count)
"
    );
    let mut p = Program::compile(&program).unwrap();
    feed_five(&mut p);
    for (i, price) in [10.0, 11.0].into_iter().enumerate() {
        feed(
            &mut p,
            "StockTick",
            &tick(&format!("10:00:0{i}"), "MSFT", price),
        );
    }
    let out = only(end(&mut p));
    assert_eq!(out["symbol"], "AAPL", "MSFT's one trend is under 20");
    assert_eq!(out["trends"], 24);
}

/// The shipped batch example: three steps between a start and an end give
/// 7 trends, each step in 4 of them.
#[test]
fn a_start_a_closure_and_an_end_step() {
    let program = r"
event BatchStart:
    batch_id: str
    source: str
event BatchStep:
    batch_id: str
    step: str
    duration: float
    errors: int
event BatchEnd:
    batch_id: str
    status: str

stream BatchAnalytics = BatchStart as start
    -> all BatchStep where batch_id == start.batch_id as steps
    -> BatchEnd where batch_id == start.batch_id as done
    .within(30m)
    .partition_by(batch_id)
    .trend_aggregate(
        avg_duration: avg_trends(steps.duration),
        max_errors: max_trends(steps.errors),
        total_duration: sum_trends(steps.duration),
        step_count: count_events(steps),
        trend_count: count_trends()
    )
    .where(total_duration > 1000)
    .emit(
        batch_id: start.batch_id,
        source: start.source,
        status: done.status,
        avg_step_duration: avg_duration,
        max_step_errors: max_errors,
        total_duration: total_duration,
        steps: step_count,
        distinct_steps: count(steps),
        trends: trend_count
    )
";
    let mut p = Program::compile(program).unwrap();
    let b = r#""batch_id": "BATCH-001""#;
    feed(
        &mut p,
        "BatchStart",
        &at("10:00:00", &format!(r#"{b}, "source": "etl""#)),
    );
    for (i, (step, duration, errors)) in [
        ("extract", 1200.0, 0),
        ("transform", 3400.0, 2),
        ("load", 800.0, 0),
    ]
    .into_iter()
    .enumerate()
    {
        feed(
            &mut p,
            "BatchStep",
            &at(
                &format!("10:00:0{}", i + 1),
                &format!(r#"{b}, "step": "{step}", "duration": {duration:?}, "errors": {errors}"#),
            ),
        );
    }
    feed(
        &mut p,
        "BatchEnd",
        &at("10:00:05", &format!(r#"{b}, "status": "success""#)),
    );
    let out = only(end(&mut p));
    assert_eq!(out["batch_id"], "BATCH-001");
    assert_eq!(out["source"], "etl");
    assert_eq!(out["status"], "success");
    assert_eq!(out["trends"], 7);
    assert_eq!(out["steps"], 12);
    assert_eq!(out["distinct_steps"], 3);
    assert_eq!(out["total_duration"], 21600.0);
    assert_eq!(out["avg_step_duration"], 21600.0 / 7.0);
    assert_eq!(out["max_step_errors"], 2.0);
}

/// An end step's condition reads the closure's last event.
#[test]
fn an_end_step_reads_the_last_event_of_the_closure() {
    let program = format!(
        r"{TICKS}
stream Reversals = StockTick as first
    -> all StockTick where price > first.price as rising
    -> StockTick where price < rising.price as reversal
    .within(1m)
    .partition_by(symbol)
    .trend_aggregate(trends: count_trends(), ends: count_events(reversal))
    .emit(trends: trends, ends: ends, last: reversal.price)
"
    );
    // From 100: rising subsets of {105, 110} then 108 below the last one:
    // {110} and {105, 110} end at 108; {105}, {110}, {105, 110}, {108}, ...
    // then 90 below any: 7 subsets of {105, 110, 108}; 9 in all. From 105:
    // {110} ends at 108; the 3 subsets of {110, 108} end at 90: 4. From 110:
    // {115}? absent; 90 ends nothing without a rising tick. 13 in all.
    let mut p = Program::compile(&program).unwrap();
    for (i, price) in [100.0, 105.0, 110.0, 108.0, 90.0].into_iter().enumerate() {
        feed(
            &mut p,
            "StockTick",
            &tick(&format!("10:00:0{i}"), "AAPL", price),
        );
    }
    let out = only(end(&mut p));
    assert_eq!(out["trends"], 13);
    assert_eq!(out["ends"], 13, "one end per trend");
    assert_eq!(out["last"], 90.0, "the latest end that closes a trend");
}

/// The open windows are in the snapshot: a program restored mid-window
/// finishes it as the original would have.
#[test]
fn the_open_windows_survive_a_snapshot() {
    let source = rising("where price > first.price");
    let mut p = Program::compile(&source).unwrap();
    for (i, price) in FIVE[..3].iter().enumerate() {
        feed(
            &mut p,
            "StockTick",
            &tick(&format!("10:00:0{i}"), "AAPL", *price),
        );
    }
    let bytes = p.snapshot().unwrap();
    let mut p = Program::compile(&source).unwrap();
    p.restore(&bytes).unwrap();
    for (i, price) in FIVE[3..].iter().enumerate() {
        feed(
            &mut p,
            "StockTick",
            &tick(&format!("10:00:0{}", i + 3), "AAPL", *price),
        );
    }
    let out = only(end(&mut p));
    assert_eq!(out["trends"], 24);
    assert_eq!(out["start"], 100.0);
}

/// Two streams over the same ticks with different start conditions count
/// their own trends.
#[test]
fn two_streams_count_independently() {
    let program = format!(
        r"{TICKS}
stream LowStart = StockTick where price < 50.0 as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .trend_aggregate(count: count_trends())
    .emit(low: count)

stream HighStart = StockTick where price >= 50.0 as first
    -> all StockTick where price > first.price as rising
    .within(1m)
    .trend_aggregate(count: count_trends())
    .emit(high: count)
"
    );
    let mut p = Program::compile(&program).unwrap();
    for (i, price) in [40.0, 60.0, 70.0].into_iter().enumerate() {
        feed(
            &mut p,
            "StockTick",
            &tick(&format!("10:00:0{i}"), "X", price),
        );
    }
    let out = end(&mut p);
    assert_eq!(out.len(), 2, "{out:#?}");
    assert_eq!(out[0]["event_type"], "LowStart");
    assert_eq!(out[0]["low"], 3, "from 40: the subsets of {{60, 70}}");
    assert_eq!(out[1]["event_type"], "HighStart");
    assert_eq!(out[1]["high"], 1, "from 60: {{70}}; from 70: nothing");
}

/// A named pattern aggregates like an inline one.
#[test]
fn a_named_pattern_is_aggregated() {
    let program = format!(
        r"{TICKS}
pattern Rising = StockTick as first
    -> all StockTick where price > first.price as rising
    within 1m partition by symbol

stream RisingTrends = Rising
    .trend_aggregate(trends: count_trends())
    .emit(symbol: symbol, trends: trends)
"
    );
    let mut p = Program::compile(&program).unwrap_or_else(|e| panic!("{e}"));
    feed_five(&mut p);
    let out = only(end(&mut p));
    assert_eq!(out["symbol"], "AAPL");
    assert_eq!(out["trends"], 24);
}

fn refused(program: &str) -> String {
    match Program::compile(program) {
        Ok(_) => panic!("compiled, must be refused:\n{program}"),
        Err(e) => e.to_string(),
    }
}

/// What the counting cannot honour is refused when the program loads,
/// naming the form it honours.
#[test]
fn what_the_counting_cannot_honour_is_refused() {
    let base = rising("where price > first.price");
    let cases: [(&str, &str, &str); 9] = [
        (
            "an emission mode",
            ".within(1m)\n    .each()",
            ".each() does not apply to .trend_aggregate()",
        ),
        (
            "the longest emission mode",
            ".within(1m)\n    .longest()",
            ".longest() does not apply",
        ),
        (
            "strict contiguity",
            ".within(1m)\n    .strict()",
            ".strict() does not apply",
        ),
        (
            "an unknown aggregate",
            "low: min_trends(rising.price),",
            "'min' is not a trend aggregate",
        ),
        (
            "a bare field",
            "total: sum_trends(rising.price),",
            "sum_trends() takes an alias-qualified field",
        ),
        (
            "an alias that is not a step",
            "total: sum_trends(rising.price),",
            "'falling' is not a step of the pattern (the steps are first, rising)",
        ),
        (
            "an argument to count_trends",
            "trends: count_trends(),",
            "count_trends() takes no argument",
        ),
        (
            "a closure as the start",
            "StockTick as first\n    -> all StockTick where price > first.price as rising",
            "start with a single event",
        ),
        (
            "a pattern without a closure",
            "-> all StockTick where price > first.price as rising",
            "needs a closure in the pattern",
        ),
    ];
    let replacements: [&str; 9] = [
        ".within(1m)\n    .each()",
        ".within(1m)\n    .longest()",
        ".within(1m)\n    .strict()",
        "low: min(rising.price),",
        "total: sum_trends(price),",
        "total: sum_trends(falling.price),",
        "trends: count_trends(rising),",
        "all StockTick as first\n    -> all StockTick where price > first.price as rising",
        "-> StockTick where price > first.price as rising",
    ];
    for ((what, from, expected), to) in cases.iter().zip(replacements) {
        let program = if from.starts_with(".within") {
            base.replace(".within(1m)", to)
        } else {
            base.replace(from, to)
        };
        assert_ne!(program, base, "{what}: the replacement did not apply");
        let err = refused(&program);
        assert!(err.contains(expected), "{what}: {err}");
    }
}

#[test]
fn a_missing_within_is_refused() {
    let program = rising("where price > first.price").replace("    .within(1m)\n", "");
    let err = refused(&program);
    assert!(err.contains("needs .within(d)"), "{err}");
}

#[test]
fn a_negated_step_is_refused() {
    let program = format!(
        r"{TICKS}
event Halt:
    symbol: str
stream S = StockTick as first
    -> all StockTick where price > first.price as rising
    .not(Halt)
    .within(1m)
    .trend_aggregate(trends: count_trends())
    .emit(trends: trends)
"
    );
    let err = refused(&program);
    assert!(err.contains("NOT step"), "{err}");
}

/// A condition can read the start and, in a closure, the closure's own
/// previous event; a single step's, the step before it. Anything else the
/// counting cannot bind, and says so.
#[test]
fn a_condition_reading_an_unbound_alias_is_refused() {
    let program = r"
event A:
    v: float
event B:
    v: float
event C:
    v: float
stream S = A as a
    -> B as b
    -> all C where v > b.v as c
    .within(1m)
    .trend_aggregate(trends: count_trends())
    .emit(trends: trends)
";
    let err = refused(program);
    assert!(
        err.contains("step 'c' reads 'b'") && err.contains("closure's own previous event"),
        "{err}"
    );
}
