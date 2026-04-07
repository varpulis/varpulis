//! Array semantics tests for Kleene-bound aliases.
//!
//! Tests that `b.LEN`, `b[i]`, `b[i].field`, `first(b)`, `last(b)`, and
//! `collect(b.field)` all work as documented in the SASE+ paper, where
//! Kleene-bound variables are sequences/arrays.
#![allow(clippy::needless_raw_string_hashes)]
#![allow(non_snake_case)]

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

fn run_sync(code: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    engine.process_batch_sync(events).unwrap();
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

fn reading(id: &str, val: i64) -> Event {
    let mut e = Event::new("Reading");
    e.data.insert("id".into(), Value::str(id));
    e.data.insert("val".into(), Value::Int(val));
    e
}

#[test]
fn test_array_len_via_LEN_field() {
    // b.LEN should return the count of captured Kleene events.
    let vpl = r#"
event Start: id: str
event Reading:
    id: str
    val: int
event End: id: str

stream X = Start -> all Reading as b -> End
    .longest()
    .emit(count: b.LEN)
"#;

    let mut events = vec![Event::new("Start")];
    events.push(reading("R1", 10));
    events.push(reading("R2", 20));
    events.push(reading("R3", 30));
    events.push(Event::new("End"));

    let outputs = run_sync(vpl, events);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].get("count"), Some(&Value::Int(3)));
}

#[test]
fn test_array_index_zero_first_element() {
    // b[0].field should return the first captured event's field.
    let vpl = r#"
event Start: id: str
event Reading:
    id: str
    val: int
event End: id: str

stream X = Start -> all Reading as b -> End
    .longest()
    .emit(first_id: b[0].id, first_val: b[0].val)
"#;

    let events = vec![
        Event::new("Start"),
        reading("R1", 10),
        reading("R2", 20),
        reading("R3", 30),
        Event::new("End"),
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].get("first_id"), Some(&Value::str("R1")));
    assert_eq!(outputs[0].get("first_val"), Some(&Value::Int(10)));
}

#[test]
fn test_array_index_last_via_len_minus_one() {
    // b[b.LEN - 1].field should equal b.field (last element shortcut)
    let vpl = r#"
event Start: id: str
event Reading:
    id: str
    val: int
event End: id: str

stream X = Start -> all Reading as b -> End
    .longest()
    .emit(last_via_index: b[b.LEN - 1].id, last_via_shortcut: b.id)
"#;

    let events = vec![
        Event::new("Start"),
        reading("R1", 10),
        reading("R2", 20),
        reading("R3", 30),
        Event::new("End"),
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].get("last_via_index"), Some(&Value::str("R3")));
    assert_eq!(outputs[0].get("last_via_shortcut"), Some(&Value::str("R3")));
}

#[test]
fn test_array_index_middle_element() {
    // b[1].field should return the second captured event's field.
    let vpl = r#"
event Start: id: str
event Reading:
    id: str
    val: int
event End: id: str

stream X = Start -> all Reading as b -> End
    .longest()
    .emit(middle_id: b[1].id)
"#;

    let events = vec![
        Event::new("Start"),
        reading("R1", 10),
        reading("R2", 20),
        reading("R3", 30),
        Event::new("End"),
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].get("middle_id"), Some(&Value::str("R2")));
}

#[test]
fn test_collect_field_returns_array() {
    // collect(b.field) should return an array of all field values.
    let vpl = r#"
event Start: id: str
event Reading:
    id: str
    val: int
event End: id: str

stream X = Start -> all Reading as b -> End
    .longest()
    .emit(all_ids: collect(b.id), all_vals: collect(b.val))
"#;

    let events = vec![
        Event::new("Start"),
        reading("R1", 10),
        reading("R2", 20),
        reading("R3", 30),
        Event::new("End"),
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(outputs.len(), 1);

    let all_ids = outputs[0].get("all_ids").expect("all_ids present");
    if let Value::Array(arr) = all_ids {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Value::str("R1"));
        assert_eq!(arr[1], Value::str("R2"));
        assert_eq!(arr[2], Value::str("R3"));
    } else {
        panic!("all_ids should be Value::Array, got {all_ids:?}");
    }

    let all_vals = outputs[0].get("all_vals").expect("all_vals present");
    if let Value::Array(arr) = all_vals {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Value::Int(10));
        assert_eq!(arr[1], Value::Int(20));
        assert_eq!(arr[2], Value::Int(30));
    } else {
        panic!("all_vals should be Value::Array");
    }
}

#[test]
fn test_count_function_matches_LEN() {
    // count(b) and b.LEN should return the same value.
    let vpl = r#"
event Start: id: str
event Reading:
    id: str
    val: int
event End: id: str

stream X = Start -> all Reading as b -> End
    .longest()
    .emit(via_count: count(b), via_len: b.LEN)
"#;

    let events = vec![
        Event::new("Start"),
        reading("R1", 10),
        reading("R2", 20),
        Event::new("End"),
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].get("via_count"), Some(&Value::Int(2)));
    assert_eq!(outputs[0].get("via_len"), Some(&Value::Int(2)));
}
