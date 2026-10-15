//! `Program::compile` gives the verdict `Program::check` gives.
//!
//! `compile` skipped the semantic validator, so `varpulis simulate` and any
//! host that compiled without checking ran a program with a misspelled event
//! type as a program that never fires. And three constructs the engine
//! cannot act on — a lookup, a webhook, a timer — loaded without a word and
//! did nothing.

mod common;

use common::{at, feed};
use varpulis_engine::{Error, Program};

fn refused(vpl: &str) -> String {
    match Program::compile(vpl) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("compiled, and it must not:\n{vpl}"),
    }
}

#[test]
fn compile_refuses_a_misspelled_event_type_as_check_does() {
    let program = r"
event Order:
    id: str
stream S = Ordr
    .emit(id: id)
";
    let check = Program::check(program).unwrap_err();
    assert!(
        matches!(check, Error::Invalid(ref d) if d.contains("E033")),
        "{check}"
    );
    let compile = Program::compile(program).unwrap_err();
    assert!(
        matches!(compile, Error::Invalid(ref d) if d.contains("E033")),
        "compile must refuse what check refuses: {compile}"
    );
}

#[test]
fn compile_still_accepts_a_program_that_declares_no_event_type() {
    let mut p = Program::compile("stream S = Anything .emit(x: x)").unwrap();
    let out = feed(&mut p, "Anything", &at("10:00:00", r#""x": 1"#));
    assert_eq!(out.len(), 1);
}

#[test]
fn an_enrichment_is_refused_by_an_engine_with_no_lookup_provider() {
    let message = refused(
        r#"
connector Weather = http(url: "http://example.invalid/v1")
event Temperature:
    city: str
stream S = Temperature as t
    .enrich(Weather, key: t.city, fields: [forecast])
    .emit(city: t.city, forecast: forecast)
"#,
    );
    assert!(message.contains(".enrich()"), "{message}");
}

#[test]
fn a_webhook_alert_is_refused_by_an_engine_that_opens_no_socket() {
    let message = refused(
        r#"
event Login:
    user: str
stream S = Login
    .alert(webhook: "http://example.invalid/hook", message: "login {user}")
    .emit(user: user)
"#,
    );
    assert!(message.contains("webhook"), "{message}");
}

#[test]
fn an_alert_without_a_webhook_still_loads_and_passes_events_on() {
    let mut p = Program::compile(
        r#"
event Login:
    user: str
stream S = Login
    .alert(message: "login {user}")
    .emit(user: user)
"#,
    )
    .unwrap();
    let out = feed(&mut p, "Login", &at("10:00:00", r#""user": "bob""#));
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["user"], "bob");
}

#[test]
fn a_timer_source_is_refused_by_an_engine_that_has_no_clock_of_its_own() {
    let message = refused("stream Beat = timer(1s) .emit(tick: 1)");
    assert!(message.contains("timer"), "{message}");
}

#[test]
fn distinct_and_limit_check_ok_and_do_what_they_say() {
    let program = r"
event Login:
    user: str
stream D = Login
    .distinct(user)
    .emit(user: user)
stream L = Login
    .limit(2)
    .emit(user: user)
";
    Program::check(program).unwrap_or_else(|e| panic!("{e}"));
    let mut p = Program::compile(program).unwrap();
    let mut seen: Vec<(String, String)> = Vec::new();
    for (i, user) in ["a", "b", "a", "c"].iter().enumerate() {
        for emit in feed(
            &mut p,
            "Login",
            &at(&format!("10:00:0{i}"), &format!(r#""user": "{user}""#)),
        ) {
            seen.push((
                emit["event_type"].as_str().unwrap().to_string(),
                emit["user"].as_str().unwrap().to_string(),
            ));
        }
    }
    let of = |stream: &str| -> Vec<&str> {
        seen.iter()
            .filter(|(s, _)| s == stream)
            .map(|(_, u)| u.as_str())
            .collect()
    };
    assert_eq!(of("D"), vec!["a", "b", "c"]);
    assert_eq!(of("L"), vec!["a", "b"]);
}

#[test]
fn a_negated_step_that_is_not_last_is_refused() {
    // `A -> NOT B -> C`: the absence is decided when the pattern's deadline
    // passes, and the run expires with it, so C could never be taken. It
    // checked "ok" and never fired.
    let program = r"
event A:
    k: str
event B:
    k: str
event C:
    k: str
pattern P = A as a -> NOT B where k == a.k -> C where k == a.k as c within 1h
stream S = P
    .emit(k: a.k)
";
    let err = Program::check(program).unwrap_err();
    assert!(
        matches!(err, Error::Invalid(ref d) if d.contains("E093")),
        "{err}"
    );
    assert!(refused(program).contains("last step"));
}

#[test]
fn a_typed_emit_is_refused_rather_than_ignored() {
    let program = r"
event T:
    v: int
event Alert:
    v: int
stream S = T
    .emit as Alert (v: v)
";
    let message = refused(program);
    assert!(message.contains(".emit as Alert"), "{message}");
}

#[test]
fn an_import_is_refused_rather_than_ignored() {
    let message = refused(
        r#"
import "lib/common.vpl" as common
event T:
    v: int
stream S = T
    .emit(v: v)
"#,
    );
    assert!(message.contains("import"), "{message}");
}

#[test]
fn a_context_is_refused_by_an_engine_with_no_threads_of_its_own() {
    let message = refused(
        r"
context fast (cores: [0])
event T:
    v: int
stream S = T
    .context(fast)
    .emit(v: v)
",
    );
    assert!(message.contains("context"), "{message}");
}

#[test]
fn a_window_policy_is_refused_instead_of_ignored() {
    // `.window(5m, policy: "watermark")` is in the joins reference; the
    // argument was parsed and did nothing.
    let message = refused(
        r#"
event Order:
    id: str
stream S = Order
    .window(5m, policy: "watermark")
    .aggregate(n: count())
"#,
    );
    assert!(
        message.contains("policy") && message.contains(".watermark("),
        "{message}"
    );
}

#[test]
fn an_aggregate_directly_on_a_join_is_refused_instead_of_emitting_nothing() {
    // The tutorial's "Join with Aggregation" loaded and never emitted: the
    // join's window is not run on the pairs, so the aggregate never flushed.
    let message = refused(
        r"
event OrderEvent:
    customer_id: str
    amount: float
event CustomerEvent:
    id: str
    name: str
stream CustomerStats = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent.on(Orders.customer_id == Customers.id)
)
.window(1h)
.aggregate(order_count: count(), total_spent: sum(Orders.amount))
",
    );
    assert!(
        message.contains("join") && message.contains(".aggregate()"),
        "{message}"
    );
}

#[test]
fn a_slide_of_the_wrong_kind_is_refused_instead_of_defaulting() {
    // A time window with a count for a slide ran with a slide of one
    // minute; a count window with a duration for a slide, with a slide of
    // one event. Neither said so.
    let message = refused(
        r"
event Order:
    id: str
stream S = Order
    .window(5m, sliding: 10)
    .aggregate(n: count())
",
    );
    assert!(message.contains("slid"), "{message}");
    let message = refused(
        r"
event Order:
    id: str
stream S = Order
    .window(100, sliding: 5s)
    .aggregate(n: count())
",
    );
    assert!(message.contains("slid"), "{message}");
    let message = refused(
        r"
event Order:
    id: str
stream S = Order
    .window(5m, sliding: 0s)
    .aggregate(n: count())
",
    );
    assert!(message.contains("positive"), "{message}");
}
