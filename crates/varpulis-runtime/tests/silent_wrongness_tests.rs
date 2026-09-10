//! Constructs that used to parse, validate, execute, and return a silently
//! wrong answer.
//!
//! Every test here was written against the *unfixed* engine and observed to
//! fail there. For a detection product a wrong answer is a missed or fabricated
//! alert, and silence is what makes it lethal — so the rule applied throughout
//! is: refuse to compile rather than compute a wrong answer, and where refusing
//! would break a legitimate program, implement the construct properly.
//!
//! Cases, in the order of the audit:
//!
//! 1. `sum(<expression>)` aggregated a phantom field named `"value"`.
//! 2. `.partition_by(<anything but a bare identifier>)` was discarded.
//! 3. `==` was type-strict while `<`/`>` coerced, and SASE disagreed with
//!    `.where()`.
//! 4. `.emit(field: "literal")` could be replaced by a same-named field value.
//! 5. `.not(EventType)` invalidated runs in every partition.
//! 6. Kleene closures truncated at 20 events in silence.
//! 7. `.concurrent()` was a no-op that still allocated a thread pool.

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

// ===========================================================================
// Harness
// ===========================================================================

async fn run(program_source: &str, events_source: &str) -> Vec<Event> {
    let (tx, mut rx) = mpsc::channel::<Event>(4096);
    let program = parse(program_source).expect("parse program");
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load program");

    let events = EventFileParser::parse(events_source).expect("parse events");
    for te in events {
        engine.process(te.event).await.expect("process");
    }
    engine.flush_end_of_input().await.expect("end-of-input");

    let mut out = Vec::new();
    while let Ok(Some(e)) =
        tokio::time::timeout(std::time::Duration::from_millis(150), rx.recv()).await
    {
        out.push(e);
    }
    out
}

/// Load a program into a bare engine and return the load error, if any.
fn load_error(program_source: &str) -> Option<String> {
    let program = parse(program_source).expect("parse program");
    let (tx, _rx) = mpsc::channel::<Event>(16);
    let mut engine = Engine::new(tx);
    engine.load(&program).err().map(|e| format!("{e}"))
}

fn f(e: &Event, name: &str) -> Option<Value> {
    e.data.get(name).cloned()
}

// ===========================================================================
// 1. sum(<expression>) — aggregated a field named "value" that does not exist
// ===========================================================================

#[tokio::test]
async fn sum_of_an_expression_sums_the_expression() {
    // sum(price * quantity) = 10*2 + 20*3 = 80. The unfixed compiler yielded
    // `None` for the field name and `aggregation.rs` then read a phantom field
    // called "value", returning 0.0.
    let program = r"
        event Trade:
            price: float
            quantity: int

        stream Totals = Trade
            .window(2)
            .aggregate(notional: sum(price * quantity))
            .emit(notional: notional)
    ";
    let events = r"
        Trade { price: 10.0, quantity: 2 }
        Trade { price: 20.0, quantity: 3 }
    ";

    let out = run(program, events).await;
    assert_eq!(out.len(), 1, "one window should fire");
    assert_eq!(
        f(&out[0], "notional"),
        Some(Value::Float(80.0)),
        "sum(price * quantity) must sum the product, not a phantom \"value\" field"
    );
}

#[tokio::test]
async fn avg_and_max_of_an_expression_are_computed() {
    let program = r"
        event R:
            bytes_in: int
            bytes_out: int

        stream Agg = R
            .window(3)
            .aggregate(
                mean_total: avg(bytes_in + bytes_out),
                peak: max(bytes_in * 2)
            )
            .emit(mean_total: mean_total, peak: peak)
    ";
    let events = r"
        R { bytes_in: 10, bytes_out: 1 }
        R { bytes_in: 20, bytes_out: 2 }
        R { bytes_in: 30, bytes_out: 3 }
    ";

    let out = run(program, events).await;
    assert_eq!(out.len(), 1);
    // (11 + 22 + 33) / 3 = 22
    assert_eq!(f(&out[0], "mean_total"), Some(Value::Float(22.0)));
    // max(2*bytes_in) = 60
    assert_eq!(f(&out[0], "peak"), Some(Value::Float(60.0)));
}

#[tokio::test]
async fn a_rule_shaped_sum_of_a_sum_actually_fires() {
    // The audit's headline shape: `sum(bytes_in + bytes_out) > threshold`
    // never fired, because the sum was always 0.0.
    let program = r#"
        event Flow:
            bytes_in: int
            bytes_out: int

        stream Burst = Flow
            .window(2)
            .aggregate(total: sum(bytes_in + bytes_out))
            .having(total > 100)
            .emit(alert: "burst", total: total)
    "#;
    let events = r"
        Flow { bytes_in: 60, bytes_out: 10 }
        Flow { bytes_in: 50, bytes_out: 10 }
    ";

    let out = run(program, events).await;
    assert_eq!(
        out.len(),
        1,
        "sum(bytes_in + bytes_out) = 130 > 100 must fire the rule"
    );
    assert_eq!(f(&out[0], "total"), Some(Value::Float(130.0)));
}

#[tokio::test]
async fn shipped_example_10_computes_a_real_vwap() {
    // examples/vpl-by-example/10_partitioned_aggregations.vpl, run over the
    // .evt we ship with it. AAPL vwap = 148300/800 = 185.375,
    // GOOG vwap = 140375/1000 = 140.375. Both came out 0.0.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let vpl = std::fs::read_to_string(
        root.join("examples/vpl-by-example/10_partitioned_aggregations.vpl"),
    )
    .expect("read vpl");
    let evt = std::fs::read_to_string(
        root.join("examples/vpl-by-example/10_partitioned_aggregations.evt"),
    )
    .expect("read evt");

    let out = run(&vpl, &evt).await;
    assert_eq!(out.len(), 2, "one window per symbol");

    let mut by_symbol: Vec<(String, f64)> = out
        .iter()
        .map(|e| {
            let sym = match f(e, "symbol") {
                Some(Value::Str(s)) => s.to_string(),
                other => panic!("symbol was {other:?}"),
            };
            let vwap = match f(e, "vwap") {
                Some(Value::Float(v)) => v,
                other => panic!("vwap was {other:?}"),
            };
            (sym, vwap)
        })
        .collect();
    by_symbol.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(by_symbol[0].0, "AAPL");
    assert!(
        (by_symbol[0].1 - 185.375).abs() < 1e-9,
        "AAPL vwap should be 185.375, got {}",
        by_symbol[0].1
    );
    assert_eq!(by_symbol[1].0, "GOOG");
    assert!(
        (by_symbol[1].1 - 140.375).abs() < 1e-9,
        "GOOG vwap should be 140.375, got {}",
        by_symbol[1].1
    );
}

// ===========================================================================
// 2. .partition_by(<non-identifier>) was silently discarded
// ===========================================================================

#[tokio::test]
async fn partition_by_alias_qualified_field_actually_partitions() {
    // The form published in docs/usecases-webmethods.md and
    // docs/comparisons/varpulis-vs-arroyo.md. Discarding it gives an
    // unpartitioned SASE engine, so a Login for alice correlates with a
    // Transfer for bob — for a detection rule, kill-chain steps crossing
    // between users and hosts.
    let program = r"
        event Login:
            user_id: str

        event Transfer:
            user_id: str

        stream CrossUser = Login as login
            -> Transfer as xfer
            .within(5m)
            .partition_by(login.user_id)
            .emit(who: login.user_id)
    ";
    let events = r#"
        Login { user_id: "alice" }
        Transfer { user_id: "bob" }
    "#;

    let out = run(program, events).await;
    assert_eq!(
        out.len(),
        0,
        "alice's login must not correlate with bob's transfer"
    );
}

#[tokio::test]
async fn partition_by_alias_qualified_field_still_matches_within_a_partition() {
    let program = r"
        event Login:
            user_id: str

        event Transfer:
            user_id: str

        stream SameUser = Login as login
            -> Transfer as xfer
            .within(5m)
            .partition_by(login.user_id)
            .emit(who: login.user_id)
    ";
    let events = r#"
        Login { user_id: "alice" }
        Transfer { user_id: "alice" }
    "#;

    let out = run(program, events).await;
    assert_eq!(out.len(), 1, "same-user sequence must still match");
    assert_eq!(f(&out[0], "who"), Some(Value::str("alice")));
}

#[test]
fn partition_by_an_arbitrary_expression_is_refused() {
    // Not silently ignored: refused, with the alternative named.
    let err = load_error(
        r"
        event E:
            a: str
            b: str

        stream S = E
            .partition_by(a + b)
            .window(2)
            .aggregate(n: count())
            .emit(n: n)
        ",
    )
    .expect(".partition_by(a + b) must not compile");
    assert!(
        err.contains(".partition_by()"),
        "error must name the operator, got: {err}"
    );
}

#[test]
fn partition_by_an_unknown_alias_is_refused() {
    let err = load_error(
        r"
        event Login:
            user_id: str

        event Transfer:
            user_id: str

        stream S = Login as login
            -> Transfer as xfer
            .within(5m)
            .partition_by(nosuch.user_id)
            .emit(who: login.user_id)
        ",
    )
    .expect(".partition_by(nosuch.user_id) must not compile");
    assert!(
        err.contains("nosuch"),
        "error must name the unknown alias, got: {err}"
    );
}

// ===========================================================================
// 3. `==` was type-strict where every other comparison coerced
// ===========================================================================

#[tokio::test]
async fn equality_coerces_int_and_float_like_every_other_comparison() {
    // DestinationPort arriving as 445.0 (a JSON number that happened to carry a
    // decimal point) was compared against the literal 445 and missed.
    let program = r#"
        event Net:
            DestinationPort: float

        stream SMB = Net
            .where(DestinationPort == 445)
            .emit(hit: "smb")
    "#;
    let events = r"
        Net { DestinationPort: 445.0 }
    ";

    let out = run(program, events).await;
    assert_eq!(
        out.len(),
        1,
        "445.0 == 445 must hold, exactly as 445.0 >= 445 already did"
    );
}

#[tokio::test]
async fn inequality_coerces_too() {
    let program = r#"
        event Net:
            DestinationPort: float

        stream NotSMB = Net
            .where(DestinationPort != 445)
            .emit(hit: "other")
    "#;
    let events = r"
        Net { DestinationPort: 445.0 }
        Net { DestinationPort: 80.0 }
    ";

    let out = run(program, events).await;
    assert_eq!(out.len(), 1, "only port 80 is != 445");
}

#[tokio::test]
async fn where_and_sequence_steps_agree_on_equality() {
    // The identical predicate inside a `->` step matched where `.where()` did
    // not: SASE coerced, the expression evaluator did not.
    let filtered = r"
        event Net:
            DestinationPort: float
            Hostname: str

        event Proc:
            Hostname: str

        stream A = Net
            .where(DestinationPort == 445)
            .emit(Hostname: Hostname)
    ";
    let sequenced = r"
        event Net:
            DestinationPort: float
            Hostname: str

        event Proc:
            Hostname: str

        stream B = Net where DestinationPort == 445 as n
            -> Proc as p
            .within(5m)
            .emit(host: n.Hostname)
    ";
    let events = r#"
        Net { DestinationPort: 445.0, Hostname: "WS01" }
        Proc { Hostname: "WS01" }
    "#;

    let a = run(filtered, events).await;
    let b = run(sequenced, events).await;
    assert_eq!(
        a.len(),
        b.len(),
        ".where() and a sequence step must agree on the same predicate \
         (.where() emitted {}, sequence emitted {})",
        a.len(),
        b.len()
    );
    assert_eq!(a.len(), 1);
}

#[tokio::test]
async fn having_can_compare_an_aggregate_against_an_int_literal() {
    // sum/avg/min/max always return Float, so `total == 100` could never fire.
    let program = r"
        event M:
            v: int

        stream Exact = M
            .window(2)
            .aggregate(total: sum(v))
            .having(total == 100)
            .emit(total: total)
    ";
    let events = r"
        M { v: 40 }
        M { v: 60 }
    ";

    let out = run(program, events).await;
    assert_eq!(out.len(), 1, "sum = 100.0 must satisfy `total == 100`");
}

// ===========================================================================
// 4. .emit(field: "literal") could be replaced by a field value
// ===========================================================================

#[tokio::test]
async fn emit_string_literal_is_a_literal_even_when_a_field_shares_its_name() {
    let program = r#"
        event Alert:
            critical: str

        stream Out = Alert
            .emit(severity: "critical")
    "#;
    let events = r#"
        Alert { critical: "THIS IS A FIELD VALUE" }
    "#;

    let out = run(program, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(
        f(&out[0], "severity"),
        Some(Value::str("critical")),
        "a quoted literal must never be replaced by a same-named field"
    );
}

#[tokio::test]
async fn emit_of_a_missing_field_does_not_fabricate_its_own_name() {
    // `.emit(k: k)` emitted the literal string "k" when `k` was absent.
    let program = r"
        event E:
            present: str

        stream Out = E
            .emit(present: present, k: k)
    ";
    let events = r#"
        E { present: "yes" }
    "#;

    let out = run(program, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(f(&out[0], "present"), Some(Value::str("yes")));
    assert_eq!(
        f(&out[0], "k"),
        None,
        "a missing field must be absent, not the literal string \"k\""
    );
}

#[tokio::test]
async fn adding_an_arithmetic_item_does_not_change_the_other_items() {
    // The same emit, once with and once without an unrelated arithmetic item.
    // The presence of the arithmetic flipped the whole emit onto a different
    // code path, and the other items changed meaning.
    let without = r#"
        event Alert:
            critical: str
            n: int

        stream Out = Alert
            .emit(severity: "critical")
    "#;
    let with = r#"
        event Alert:
            critical: str
            n: int

        stream Out = Alert
            .emit(severity: "critical", doubled: n * 2)
    "#;
    let events = r#"
        Alert { critical: "THIS IS A FIELD VALUE", n: 3 }
    "#;

    let a = run(without, events).await;
    let b = run(with, events).await;
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_eq!(
        f(&a[0], "severity"),
        f(&b[0], "severity"),
        "adding an unrelated arithmetic item must not change `severity`"
    );
    assert_eq!(f(&b[0], "doubled"), Some(Value::Int(6)));
}

// ===========================================================================
// 5. .not(EventType) cancelled every partition
// ===========================================================================

#[tokio::test]
async fn bare_negation_only_cancels_its_own_partition() {
    // One device acknowledging its own alarm cancelled every other device's
    // pending match.
    let program = r"
        event Alarm:
            device: str

        event Ack:
            device: str

        event Escalate:
            device: str

        stream Unacked = Alarm as a
            -> Escalate as e
            .within(10m)
            .partition_by(device)
            .not(Ack)
            .emit(device: a.device)
    ";
    let events = r#"
        Alarm { device: "d1" }
        Alarm { device: "d2" }
        Ack { device: "d1" }
        Escalate { device: "d2" }
    "#;

    let out = run(program, events).await;
    assert_eq!(
        out.len(),
        1,
        "d1's ack must not cancel d2's pending escalation"
    );
    assert_eq!(f(&out[0], "device"), Some(Value::str("d2")));
}

#[tokio::test]
async fn bare_negation_still_cancels_its_own_partition() {
    let program = r"
        event Alarm:
            device: str

        event Ack:
            device: str

        event Escalate:
            device: str

        stream Unacked = Alarm as a
            -> Escalate as e
            .within(10m)
            .partition_by(device)
            .not(Ack)
            .emit(device: a.device)
    ";
    let events = r#"
        Alarm { device: "d1" }
        Ack { device: "d1" }
        Escalate { device: "d1" }
    "#;

    let out = run(program, events).await;
    assert_eq!(out.len(), 0, "d1's own ack must cancel d1's escalation");
}

// ===========================================================================
// 6. Kleene closures truncate at 20 events — now they say so
// ===========================================================================

/// `detect_brute_force_takeover.vpl`'s shape, in `.longest()` mode so the
/// terminator emits one consolidated match whose failure count is the thing an
/// analyst reads.
fn brute_force_program() -> &'static str {
    r#"
        event AuthEvent:
            source_ip: str
            status: str

        stream BruteForce = AuthEvent where status == "failed" as first
            -> all AuthEvent where status == "failed" as fails
            -> AuthEvent where status == "success" as success
            .within(30m)
            .partition_by(source_ip)
            .longest()
            .emit(
                failed_count: count(fails) + 1,
                truncated: _kleene_truncated
            )
    "#
}

fn brute_force_events(failures: usize) -> String {
    let mut events = String::new();
    for _ in 0..failures {
        events.push_str("AuthEvent { source_ip: \"10.0.0.1\", status: \"failed\" }\n");
    }
    events.push_str("AuthEvent { source_ip: \"10.0.0.1\", status: \"success\" }\n");
    events
}

#[tokio::test]
async fn a_truncated_kleene_closure_says_so_on_the_match() {
    // Fed more failures than the closure can hold. The cap is defensible;
    // dropping the 21st event and every one after it in silence is not — the
    // alert reported a failure count that had quietly stopped rising, from a
    // rule whose own header promised "3, 15, or 1000 failures, identically".
    let out = run(brute_force_program(), &brute_force_events(40)).await;
    assert!(!out.is_empty(), "the brute-force chain must still match");

    let truncated: Vec<i64> = out
        .iter()
        .filter_map(|e| match f(e, "truncated") {
            Some(Value::Int(n)) => Some(n),
            _ => None,
        })
        .collect();
    assert!(
        truncated.iter().any(|n| *n > 0),
        "a match whose closure dropped events at the cap must report them; got {:?}",
        out.iter().map(|e| f(e, "truncated")).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn an_untruncated_kleene_closure_stays_silent() {
    // The mark must be absent when nothing was dropped, so its presence means
    // something.
    let out = run(brute_force_program(), &brute_force_events(3)).await;
    assert!(!out.is_empty(), "the brute-force chain must still match");
    for e in &out {
        assert_eq!(
            f(e, "truncated"),
            None,
            "nothing was dropped, so nothing should be reported"
        );
    }
}

// ===========================================================================
// 8. Found while fixing the above: a keyword ate the head of an identifier
// ===========================================================================

#[tokio::test]
async fn an_identifier_that_starts_with_not_is_an_identifier() {
    // `not` was matched without a word boundary, so `notional` parsed as
    // `not ional` and `not_after` as `not _after`: a different program,
    // accepted silently, computing a different answer. `not_before`/`not_after`
    // are the standard names of a certificate's validity window, so this is a
    // shape a real detection rule takes.
    let program = r"
        event Cert:
            not_after: int
            notional: float

        stream Expiring = Cert
            .where(not_after < 100)
            .emit(when: not_after, size: notional)
    ";
    let events = r"
        Cert { not_after: 50, notional: 7.5 }
        Cert { not_after: 500, notional: 1.0 }
    ";

    let out = run(program, events).await;
    assert_eq!(out.len(), 1, "only the cert expiring before 100 matches");
    assert_eq!(f(&out[0], "when"), Some(Value::Int(50)));
    assert_eq!(f(&out[0], "size"), Some(Value::Float(7.5)));
}

#[tokio::test]
async fn a_real_not_still_negates() {
    let program = r"
        event T:
            x: int

        stream S = T
            .where(not (x > 10))
            .emit(x: x)
    ";
    let events = r"
        T { x: 5 }
        T { x: 50 }
    ";

    let out = run(program, events).await;
    assert_eq!(out.len(), 1, "`not (x > 10)` must still negate");
    assert_eq!(f(&out[0], "x"), Some(Value::Int(5)));
}

// ===========================================================================
// 7. .concurrent() was a no-op
// ===========================================================================

#[test]
fn concurrent_is_refused_with_the_alternative_named() {
    let err = load_error(
        r"
        event E:
            x: int

        stream S = E
            .concurrent(workers: 4)
            .where(x > 0)
            .emit(x: x)
        ",
    )
    .expect(".concurrent() must not compile");
    assert!(
        err.contains(".concurrent()") && err.contains("not yet implemented"),
        "error must say .concurrent() is not implemented, got: {err}"
    );
    assert!(
        err.contains("partition_by") || err.contains("workers"),
        "error must name the alternative, got: {err}"
    );
}

// ===========================================================================
// 9. Found while fixing case 4: an unresolvable trend aggregate was dropped
// ===========================================================================

#[test]
fn a_bare_field_trend_aggregate_is_refused() {
    // `sum_trends(price)` used to be dropped by a `filter_map`, producing no
    // output field at all — and `.emit(sum: total)` then fabricated the literal
    // string "total" for it, which is how the engine's own test suite came to
    // assert that `sum_trends` "worked". The documented signature, and the one
    // every shipped example uses, is `sum_trends(alias.field)`.
    let err = load_error(
        r"
        event StockTick:
            symbol: str
            price: float

        stream DualTrend = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .trend_aggregate(
                cnt: count_trends(),
                total: sum_trends(price)
            )
            .emit(count: cnt, sum: total)
        ",
    )
    .expect("sum_trends(price) must not compile");
    assert!(
        err.contains("sum_trends") && err.contains("alias"),
        "error must name the operator and the alias-qualified form, got: {err}"
    );
}

#[test]
fn an_alias_qualified_trend_aggregate_still_compiles() {
    assert_eq!(
        load_error(
            r"
        event StockTick:
            symbol: str
            price: float

        stream DualTrend = StockTick as first
            -> all StockTick as rising
            .within(60s)
            .trend_aggregate(
                cnt: count_trends(),
                total: sum_trends(rising.price)
            )
            .emit(count: cnt, sum: total)
        "
        ),
        None,
        "the documented form must still compile"
    );
}
