//! The engine as an embedding host sees it: a VPL string in, emits out, no
//! runtime, no socket. Each test is a property Vejas ADR-0031 relies on.

use varpulis_engine::{Program, Value};

/// The head-to-head workload of the 2026-09-21 merge evaluation, verbatim:
/// a lookup with fallback, two conversions, a threshold, one emit routed
/// with `.to()`.
const ORDERS: &str = r##"
connector Bus = nats (
    url: "nats://127.0.0.1:4224"
)

type Address:
    country: str

event Order:
    id: str
    email: str
    total_price: str
    t: int
    shipping_address: Address

stream Orders = Order
    .from(Bus, topic: "vx.bench.orders")

stream Out = Order
    .where(to_float(total_price) >= 0.0)
    .emit(
        order_ref: split(id, "#").last(),
        customer_email: lower(email),
        total_eur: to_float(total_price),
        country: if shipping_address.country == "France" then "FR"
                 else if shipping_address.country == "Germany" then "DE"
                 else shipping_address.country,
        t: t
    )
    .to(Bus, topic: "vx.bench.out")
"##;

const ORDER: &[u8] =
    br#"{"id": "SO#1001", "email": "Jane.Doe@ACME.com", "total_price": "347.00", "t": 0,
 "shipping_address": {"country": "France"},
 "line_items": [{"sku": "A-12", "quantity": "2", "unit_price_cents": 9900}], "type": "Order"}"#;

#[test]
fn an_emit_carries_the_sink_its_program_routed_it_to() {
    let mut program = Program::compile(ORDERS).expect("the evaluation's own program compiles");

    let sources = program.sources();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].connector_name, "Bus");
    assert_eq!(sources[0].event_type, "Order");
    assert_eq!(
        sources[0].topic_override.as_deref(),
        Some("vx.bench.orders")
    );

    let sinks = program.sinks();
    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].stream, "Out");
    assert_eq!(sinks[0].connector_name, "Bus");
    assert_eq!(sinks[0].topic.as_deref(), Some("vx.bench.out"));
    assert!(!sinks[0].dynamic_topic);

    let emits = program.feed_json("Order", ORDER).unwrap();
    assert_eq!(emits.len(), 1, "one order above the threshold, one emit");
    let emit = &emits[0];
    assert_eq!(emit.stream, "Out");
    assert_eq!(
        emit.sink.as_ref().map(|s| s.topic.as_deref()),
        Some(Some("vx.bench.out")),
        "the host learns where to publish from the emit itself"
    );

    let json = emit.to_json();
    assert_eq!(json["event_type"], "Out");
    assert_eq!(json["order_ref"], "1001");
    assert_eq!(json["customer_email"], "jane.doe@acme.com");
    assert_eq!(json["total_eur"], 347.0);
    assert_eq!(json["country"], "FR");
    assert!(
        json["timestamp"].is_string(),
        "the NATS sink's payload shape, timestamp included"
    );
}

#[test]
fn a_sequence_within_is_judged_in_event_time_not_arrival_time() {
    const LATERAL: &str = r#"
event SmbConnect:
    host: str
    target: str

event ServiceStart:
    host: str
    image: str

stream LateralMovement = SmbConnect as smb
    -> ServiceStart where host == smb.target as svc
    .within(2m)
    .emit(rule: "lateral_movement", from: smb.host, to: svc.host, image: svc.image)
"#;

    // Two events one minute apart in event time: a match.
    let mut program = Program::compile(LATERAL).unwrap();
    let first = program
        .feed_json(
            "SmbConnect",
            br#"{"event_type": "SmbConnect", "@timestamp": "2026-09-21T10:00:00Z", "host": "ws-1", "target": "srv-9"}"#,
        )
        .unwrap();
    assert!(first.is_empty(), "an open sequence emits nothing");
    let second = program
        .feed_json(
            "ServiceStart",
            br#"{"event_type": "ServiceStart", "@timestamp": "2026-09-21T10:01:00Z", "host": "srv-9", "image": "psexesvc.exe"}"#,
        )
        .unwrap();
    assert_eq!(
        second.len(),
        1,
        "SMB then a service start within two minutes"
    );
    let json = second[0].to_json();
    assert_eq!(json["rule"], "lateral_movement");
    assert_eq!(json["from"], "ws-1");
    assert_eq!(json["to"], "srv-9");
    assert!(second[0].sink.is_none(), "no .to(): the host decides");

    // The same two events three minutes apart in event time, fed back to
    // back in arrival time: no match. Arrival order and wall clock do not
    // count; the events' own timestamps do.
    let mut program = Program::compile(LATERAL).unwrap();
    program
        .feed_json(
            "SmbConnect",
            br#"{"event_type": "SmbConnect", "@timestamp": "2026-09-21T10:00:00Z", "host": "ws-1", "target": "srv-9"}"#,
        )
        .unwrap();
    let late = program
        .feed_json(
            "ServiceStart",
            br#"{"event_type": "ServiceStart", "@timestamp": "2026-09-21T10:03:00Z", "host": "srv-9", "image": "psexesvc.exe"}"#,
        )
        .unwrap();
    assert!(late.is_empty(), "three minutes is outside .within(2m)");
}

const LATERAL_RULE: &str = r#"
event SmbConnect:
    host: str
    target: str

event ServiceStart:
    host: str
    image: str

stream LateralMovement = SmbConnect as smb
    -> ServiceStart where host == smb.target as svc
    .within(2m)
    .emit(rule: "lateral_movement", from: smb.host, to: svc.host, image: svc.image)
"#;

fn smb(i: usize) -> Vec<u8> {
    format!(
        r#"{{"event_type":"SmbConnect","@timestamp":"2026-09-21T10:00:00Z","host":"ws-{i}","target":"srv-{i}"}}"#
    )
    .into_bytes()
}

fn service(i: usize, at: &str) -> Vec<u8> {
    format!(
        r#"{{"event_type":"ServiceStart","@timestamp":"{at}","host":"srv-{i}","image":"psexesvc.exe"}}"#
    )
    .into_bytes()
}

#[test]
fn a_snapshot_restored_into_a_fresh_program_closes_the_sequences_it_held_open() {
    let mut before = Program::compile(LATERAL_RULE).unwrap();
    for i in 0..20 {
        assert!(before.feed_json("SmbConnect", &smb(i)).unwrap().is_empty());
    }
    let bytes = before.snapshot().unwrap();
    assert!(!bytes.is_empty());
    drop(before); // the crash

    // Without the snapshot, a fresh program knows nothing of the twenty.
    let mut naive = Program::compile(LATERAL_RULE).unwrap();
    let lost: usize = (0..20)
        .map(|i| {
            naive
                .feed_json("ServiceStart", &service(i, "2026-09-21T10:01:00Z"))
                .unwrap()
                .len()
        })
        .sum();
    assert_eq!(lost, 0, "a fresh program cannot close what it never opened");

    // With it, every sequence opened before the snapshot closes after it.
    let mut after = Program::compile(LATERAL_RULE).unwrap();
    after.restore(&bytes).unwrap();
    let matched: usize = (0..20)
        .map(|i| {
            after
                .feed_json("ServiceStart", &service(i, "2026-09-21T10:01:00Z"))
                .unwrap()
                .len()
        })
        .sum();
    assert_eq!(matched, 20);

    // The deadlines travelled with them: three minutes on, in event time,
    // the restored sequence is closed, however soon the event arrives.
    let mut later = Program::compile(LATERAL_RULE).unwrap();
    later.restore(&bytes).unwrap();
    let out = later
        .feed_json("ServiceStart", &service(3, "2026-09-21T10:03:00Z"))
        .unwrap();
    assert!(
        out.is_empty(),
        ".within(2m) counts from the restored sequence's own start"
    );
}

#[test]
fn taking_a_snapshot_changes_nothing_for_the_program_that_took_it() {
    let mut program = Program::compile(LATERAL_RULE).unwrap();
    program.feed_json("SmbConnect", &smb(7)).unwrap();
    let first: serde_json::Value = serde_json::from_slice(&program.snapshot().unwrap()).unwrap();
    let second: serde_json::Value = serde_json::from_slice(&program.snapshot().unwrap()).unwrap();
    assert_eq!(first, second, "a snapshot is a copy, not a move");
    let out = program
        .feed_json("ServiceStart", &service(7, "2026-09-21T10:01:00Z"))
        .unwrap();
    assert_eq!(
        out.len(),
        1,
        "the sequence is still open in the program that was snapshotted"
    );
}

#[test]
fn restore_refuses_bytes_that_are_not_a_snapshot() {
    let mut program = Program::compile(LATERAL_RULE).unwrap();
    assert!(matches!(
        program.restore(b"not a snapshot"),
        Err(varpulis_engine::Error::Snapshot(_))
    ));
    assert!(matches!(
        program.restore(b""),
        Err(varpulis_engine::Error::Snapshot(_))
    ));
    assert!(matches!(
        program.restore(b"{}"),
        Err(varpulis_engine::Error::Snapshot(_))
    ));
}

#[test]
fn end_of_input_closes_the_last_window() {
    const COUNTS: &str = r"
event Ping:
    host: str

stream PerMinute = Ping
    .window(1m)
    .aggregate(n: count())
    .emit(pings: n)
";
    let mut program = Program::compile(COUNTS).unwrap();
    for second in [0, 10, 20] {
        let payload = format!(
            r#"{{"event_type": "Ping", "@timestamp": "2026-09-21T10:00:{second:02}Z", "host": "a"}}"#
        );
        let emits = program.feed_json("Ping", payload.as_bytes()).unwrap();
        assert!(emits.is_empty(), "the window is still open");
    }
    let flushed = program.end_of_input().unwrap();
    assert_eq!(
        flushed.len(),
        1,
        "bounded input: the open window closes once"
    );
    assert_eq!(flushed[0].to_json()["pings"], 3);
    assert_eq!(flushed[0].event.data.get("pings"), Some(&Value::Int(3)));
}

#[test]
fn check_refuses_what_the_engine_refuses() {
    assert!(Program::check(ORDERS).is_ok());
    // `.filter()` parses but is not an operator the engine runs; the check
    // refuses it (the validator does, before the engine would), so a host can
    // validate before deploying.
    let err = Program::check(
        "event Order:\n    id: str\n\nstream Bad = Order\n    .filter(id == \"x\")\n    .emit(x: 1)\n",
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            varpulis_engine::Error::Invalid(_) | varpulis_engine::Error::Engine(_)
        ),
        "a refusal of the program, not a parse error: {err}"
    );
}

/// Not a test: a probe for where the microseconds go, run by hand.
/// `cargo test -p varpulis-engine --release --test embed -- --ignored --nocapture throughput_probe`
#[test]
#[ignore]
fn throughput_probe() {
    use std::time::Instant;
    const N: usize = 64_000;
    let mut program = Program::compile(ORDERS).unwrap();

    let t = Instant::now();
    let mut events = Vec::with_capacity(N);
    for _ in 0..N {
        events.push(varpulis_engine::event_from_json("Order", ORDER).unwrap());
    }
    let decode_us = t.elapsed().as_secs_f64() * 1e6 / N as f64;

    let t = Instant::now();
    let mut emitted = 0usize;
    let mut bytes = 0usize;
    for ev in events {
        let emits = program.feed(ev).unwrap();
        for e in &emits {
            bytes += e.to_json().to_string().len();
        }
        emitted += emits.len();
    }
    let feed_us = t.elapsed().as_secs_f64() * 1e6 / N as f64;

    let mut program = Program::compile(ORDERS).unwrap();
    let t = Instant::now();
    for _ in 0..N {
        let ev = varpulis_engine::event_from_json("Order", ORDER).unwrap();
        let emits = program.feed(ev).unwrap();
        for e in &emits {
            bytes += e.event.to_sink_payload().len();
        }
    }
    let feed_only_us = t.elapsed().as_secs_f64() * 1e6 / N as f64 - decode_us;

    let t = Instant::now();
    let batch: Vec<varpulis_engine::Event> = (0..N)
        .map(|_| varpulis_engine::event_from_json("Order", ORDER).unwrap())
        .collect();
    let decode2 = t.elapsed();
    let mut program = Program::compile(ORDERS).unwrap();
    let t = Instant::now();
    let emits = program.feed_batch(batch).unwrap();
    let batch_us = t.elapsed().as_secs_f64() * 1e6 / N as f64;

    eprintln!(
        "probe: event_from_json {decode_us:.1} us | feed + to_json {feed_us:.1} us | feed + to_sink_payload {feed_only_us:.1} us | feed_batch(64k) {batch_us:.1} us/event (decode {:.1} us) | emits {emitted} + {} | {bytes} bytes",
        decode2.as_secs_f64() * 1e6 / N as f64,
        emits.len()
    );
}
