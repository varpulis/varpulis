//! `and` / `or` patterns in the runtime: what the compiler builds from the
//! AST, and a run inside an `and` across a checkpoint and restore.

use varpulis_core::ast::{SasePatternExpr, Stmt};
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::compiler::compile_sase_pattern_expr;
use varpulis_runtime::event::Event;
use varpulis_runtime::persistence::SaseCheckpoint;
use varpulis_runtime::sase::{MatchResult, SaseEngine, SasePattern};
use varpulis_runtime::sase_persistence::SaseCheckpointExt;

fn pattern_of(src: &str) -> SasePatternExpr {
    let program = parse(src).unwrap_or_else(|e| panic!("{src}: {e}"));
    program
        .statements
        .into_iter()
        .find_map(|s| match s.node {
            Stmt::PatternDecl { expr, .. } => Some(expr),
            _ => None,
        })
        .expect("a pattern declaration")
}

fn compiled(src: &str) -> Option<SasePattern> {
    compile_sase_pattern_expr(&pattern_of(src), None)
}

/// The pattern as a compact string, to compare shapes.
fn shape(p: &SasePattern) -> String {
    match p {
        SasePattern::Event { event_type, .. } => event_type.clone(),
        SasePattern::Seq(steps) => {
            format!(
                "seq({})",
                steps.iter().map(shape).collect::<Vec<_>>().join(", ")
            )
        }
        SasePattern::And(l, r) => format!("and({}, {})", shape(l), shape(r)),
        SasePattern::Or(l, r) => format!("or({}, {})", shape(l), shape(r)),
        SasePattern::Not(inner) => format!("not({})", shape(inner)),
        SasePattern::KleenePlus(inner) => format!("all({})", shape(inner)),
        SasePattern::KleeneStar(inner) => format!("star({})", shape(inner)),
        SasePattern::Within(inner, d) => format!("within({}, {}s)", shape(inner), d.as_secs()),
    }
}

#[test]
fn compiles_and_or_and_followed_by() {
    let p = compiled("pattern P = X -> (A and B and C) -> D or E").expect("compiles");
    assert_eq!(shape(&p), "seq(X, and(A, and(B, C)), or(D, E))");
}

#[test]
fn an_and_over_ors_is_one_and() {
    // One state whose first operand is either event: no expansion.
    let p = compiled("pattern P = (A or B) and C").expect("compiles");
    assert_eq!(shape(&p), "and(or(A, B), C)");
    let wide = ["(A or B)"; 7].join(" and ");
    let p = compiled(&format!("pattern P = {wide}")).expect("compiles");
    assert_eq!(shape(&p).matches("and(").count(), 6);
}

#[test]
fn what_the_engine_does_not_run_does_not_compile() {
    // The validator refuses these first, with E035.
    assert!(compiled("pattern P = (A -> B) and C").is_none());
    assert!(compiled("pattern P = all A as xs and B").is_none());
    assert!(compiled("pattern P = A and NOT B").is_none());
    assert!(compiled("pattern P = ((A as a -> B as b) or C as c) and D as d").is_none());
    assert!(compiled("pattern P = ((A and B) or C) and D").is_none());
}

#[test]
fn a_named_pattern_the_engine_cannot_run_fails_the_load() {
    let src = "
event A:
    id: int
event B:
    id: int
event C:
    id: int
event D:
    id: int
pattern P = ((A as a -> B as b) or C as c) and D as d within 1m
stream S = P
    .emit(d: d.id)
";
    let program = parse(src).expect("parses");
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let mut engine = varpulis_runtime::engine::Engine::new(tx);
    let err = engine
        .load(&program)
        .expect_err("an uncompilable pattern fails the load");
    assert!(err.to_string().contains("pattern 'P'"), "{err}");
}

#[test]
fn constants_reach_the_filters_inside_and_and_or() {
    let program = parse(
        "
const LIMIT = 100
pattern P = (A where id > LIMIT as a) or (B as b and C where id < LIMIT as c)
",
    )
    .expect("parses");
    let Some(expr) = program.statements.into_iter().find_map(|s| match s.node {
        Stmt::PatternDecl { expr, .. } => Some(expr),
        _ => None,
    }) else {
        panic!("a pattern declaration");
    };
    let filters: Vec<String> = expr
        .items()
        .iter()
        .filter_map(|i| i.filter.as_ref().map(|f| format!("{f:?}")))
        .collect();
    assert_eq!(filters.len(), 2);
    for f in filters {
        assert!(!f.contains("LIMIT"), "the constant is left as a name: {f}");
        assert!(f.contains("100"), "{f}");
    }
}

#[test]
fn a_pattern_without_and_or_compiles_as_before() {
    let p = compiled("pattern P = A as a -> all B as bs -> C").expect("compiles");
    assert_eq!(shape(&p), "seq(A, all(B), C)");
}

// ---------------------------------------------------------------------------
// Checkpoint and restore in the middle of an `and`
// ---------------------------------------------------------------------------

fn ev(event_type: &str, id: i64, key: i64) -> Event {
    Event::new(event_type)
        .with_field("id", Value::Int(id))
        .with_field("key", Value::Int(key))
}

fn describe(m: &MatchResult) -> String {
    let mut parts: Vec<String> = m
        .captured
        .iter()
        .map(|(alias, e)| format!("{alias}={:?}", e.get("id")))
        .collect();
    parts.sort();
    parts.join(",")
}

fn engine_for(src: &str) -> SaseEngine {
    SaseEngine::new(compiled(src).expect("compiles"))
}

/// Checkpoint `engine`, through JSON as a snapshot is stored, and restore
/// the result into a fresh engine for the same pattern.
fn restored(engine: &SaseEngine, src: &str) -> SaseEngine {
    let json = serde_json::to_string(&engine.checkpoint()).expect("serializes");
    let cp: SaseCheckpoint = serde_json::from_str(&json).expect("deserializes");
    let mut fresh = engine_for(src);
    fresh.restore(&cp);
    fresh
}

#[test]
fn a_run_restored_mid_and_keeps_the_operands_it_matched() {
    let src = "pattern P = A as a and B as b and C as c";
    let mut engine = engine_for(src);
    assert!(engine.process(&ev("A", 1, 0)).is_empty());
    assert!(engine.process(&ev("B", 2, 0)).is_empty());

    let mut engine = restored(&engine, src);
    let got: Vec<String> = engine
        .process(&ev("C", 3, 0))
        .iter()
        .map(describe)
        .collect();
    // The run A opened took B; it completes with C. (B's own run still
    // needs an A.)
    assert_eq!(got, ["a=Some(Int(1)),b=Some(Int(2)),c=Some(Int(3))"]);
}

#[test]
fn a_run_restored_mid_and_keeps_what_waits() {
    // B comes first and waits for the A whose key it compares with.
    let src = "pattern P = A as a and B where key == a.key as b";
    let mut engine = engine_for(src);
    assert!(engine.process(&ev("B", 1, 7)).is_empty());

    let mut engine = restored(&engine, src);
    assert!(engine.process(&ev("A", 2, 8)).is_empty(), "not B's partner");
    let got: Vec<String> = engine
        .process(&ev("A", 3, 7))
        .iter()
        .map(describe)
        .collect();
    assert_eq!(got, ["a=Some(Int(3)),b=Some(Int(1))"]);
}

#[test]
fn a_checkpoint_from_before_and_existed_still_restores() {
    // The fields an `and` adds are absent from older snapshots.
    let src = "pattern P = A as a -> B as b";
    let mut engine = engine_for(src);
    engine.process(&ev("A", 1, 0));
    let mut json: serde_json::Value =
        serde_json::to_value(engine.checkpoint()).expect("serializes");
    for run in json["active_runs"].as_array_mut().expect("runs") {
        let run = run.as_object_mut().expect("run");
        run.remove("and_completed");
        run.remove("and_waiting");
        run.remove("and_anchored");
    }
    let cp: SaseCheckpoint = serde_json::from_value(json).expect("old shape deserializes");
    let mut fresh = engine_for(src);
    fresh.restore(&cp);
    let got: Vec<String> = fresh.process(&ev("B", 2, 0)).iter().map(describe).collect();
    assert_eq!(got, ["a=Some(Int(1)),b=Some(Int(2))"]);
}

#[test]
fn runs_restored_mid_or_still_end_each_other() {
    // X, B: the run waits for D while its copy, past B, waits for C.
    let src = "pattern P = X as x -> ((B as b -> C as c) or D as d)";
    let mut engine = engine_for(src);
    assert!(engine.process(&ev("X", 1, 0)).is_empty());
    assert!(engine.process(&ev("B", 2, 0)).is_empty());

    let mut engine = restored(&engine, src);
    let got: Vec<String> = engine
        .process(&ev("D", 3, 0))
        .iter()
        .map(describe)
        .collect();
    assert_eq!(got, ["d=Some(Int(3)),x=Some(Int(1))"]);
    // The copy ended with the match: C completes nothing.
    assert!(engine.process(&ev("C", 4, 0)).is_empty());
}

#[test]
fn a_restore_keeps_the_winner_of_a_tie_in_a_partition() {
    // Two runs in one partition, each of which forks; then both
    // alternatives of each wait for one C. Live and restored must agree.
    let src = "pattern P = A as a -> ((B where v == a.v as p -> C where v == a.v as c) \
               or (D where v == a.v as p -> C where v == a.v as c))";
    let at = |t: &str, v: i64, secs: i64| {
        let mut e = Event::new(t)
            .with_field("id", Value::Int(secs))
            .with_field("v", Value::Int(v))
            .with_field("pk", Value::Int(0));
        e.timestamp = chrono::DateTime::from_timestamp(1_700_000_000 + secs, 0).expect("ts");
        e
    };
    let first = [
        at("A", 0, 0),
        at("B", 0, 1),
        at("A", 1, 2),
        at("B", 1, 3),
        at("D", 0, 4),
        at("C", 0, 5),
    ];
    let second = [at("D", 1, 6), at("C", 1, 7)];
    let make = || {
        SaseEngine::new(compiled(src).expect("compiles"))
            .with_event_time()
            .with_partition_by("pk".into())
    };
    let feed = |e: &mut SaseEngine, evs: &[Event]| -> Vec<String> {
        evs.iter()
            .flat_map(|x| e.process(x))
            .map(|m| describe(&m))
            .collect()
    };

    let mut live = make();
    let mut want = feed(&mut live, &first);
    want.extend(feed(&mut live, &second));

    let mut before = make();
    let mut got = feed(&mut before, &first);
    let json = serde_json::to_string(&before.checkpoint()).expect("serializes");
    let cp: SaseCheckpoint = serde_json::from_str(&json).expect("deserializes");
    let mut after = make();
    after.restore(&cp);
    got.extend(feed(&mut after, &second));

    assert_eq!(got, want);
    // The leftmost alternative, through B, wins both ties.
    assert_eq!(
        want,
        [
            "a=Some(Int(0)),c=Some(Int(5)),p=Some(Int(1))",
            "a=Some(Int(2)),c=Some(Int(7)),p=Some(Int(3))"
        ]
    );
}

#[test]
fn a_restore_keeps_which_run_is_oldest_below_the_millisecond() {
    // A copy forked into an `or` keeps the start of the run it was copied
    // from, so a run written later in a checkpoint can be older, by a few
    // microseconds, than one written before it. `EvictOldest` must evict the
    // same run after a restore as before it. Here the second run is made
    // 0.4 ms older than the first, in the same millisecond.
    use varpulis_runtime::sase::BackpressureStrategy;
    let src = "pattern P = A as a -> B as b";
    let capped = || {
        engine_for(src)
            .with_max_runs(2)
            .with_backpressure(BackpressureStrategy::EvictOldest)
    };
    let mut engine = capped();
    engine.process(&ev("A", 1, 0));
    engine.process(&ev("A", 2, 0));
    let mut json: serde_json::Value =
        serde_json::to_value(engine.checkpoint()).expect("serializes");
    let runs = json["active_runs"].as_array_mut().expect("runs");
    assert_eq!(runs.len(), 2);
    let ms = runs[0]["started_at_ms"].as_i64().expect("started_at_ms");
    for (run, offset_ns) in runs.iter_mut().zip([600_000, 200_000]) {
        run["started_at_ms"] = ms.into();
        run["started_at_ns"] = (ms * 1_000_000 + offset_ns).into();
    }
    let cp: SaseCheckpoint = serde_json::from_value(json).expect("deserializes");
    let mut fresh = capped();
    fresh.restore(&cp);
    // A third run: the cap evicts the oldest, the one A2 opened.
    fresh.process(&ev("A", 3, 0));
    let got: Vec<String> = fresh.process(&ev("B", 4, 0)).iter().map(describe).collect();
    assert_eq!(
        got,
        [
            "a=Some(Int(1)),b=Some(Int(4))",
            "a=Some(Int(3)),b=Some(Int(4))"
        ]
    );
}
