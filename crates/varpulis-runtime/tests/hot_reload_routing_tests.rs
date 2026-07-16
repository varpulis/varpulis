//! Regression test for the hot-reload routing bug (audit HIGH).
//!
//! `Engine::reload` used to rebuild the router from each stream's
//! `RuntimeSource`, which only exposes ONE event type for a multi-event
//! Sequence and NONE for a Join. So after any hot reload — an interactive edit
//! or a tenant update — Sequence and Join streams silently stopped receiving
//! their other inputs and never matched again. The fix adopts the freshly
//! compiled router (which registers every event type per stream).

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

/// A sequence stream that matches TWO event types (Order then Payment). Before
/// the fix, reload kept only the `Order` route and dropped `Payment`.
const SEQ_VPL: &str = "\
event Order:
    id: str
event Payment:
    order_id: str
stream OrderPaymentMatch = Order as order
    -> Payment where order_id == order.id as payment
    .within(5m)
    .emit(matched_order: order.id)
";

fn order(id: &str) -> Event {
    Event::new("Order").with_field("id", id)
}
fn payment(order_id: &str) -> Event {
    Event::new("Payment").with_field("order_id", order_id)
}

/// The sequence matches Order→Payment before reload, and must STILL match after
/// reloading the same program. Fail-before: reverting `reload`'s router rebuild
/// drops the `Payment` route, so the post-reload sequence never completes and
/// the second assertion fails.
#[tokio::test]
async fn sequence_stream_still_matches_after_hot_reload() {
    let program = parse(SEQ_VPL).expect("parse SEQ_VPL");
    let (tx, mut rx) = mpsc::channel::<Event>(64);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Baseline: Order then Payment completes the sequence and emits.
    engine.process(order("o1")).await.expect("process order o1");
    engine
        .process(payment("o1"))
        .await
        .expect("process payment o1");
    assert!(
        rx.try_recv().is_ok(),
        "sequence should match BEFORE reload (sanity check on the test itself)"
    );

    // Hot reload the identical program.
    engine.reload(&program).expect("reload");

    // The sequence must still match — the `Payment` route has to survive reload.
    engine.process(order("o2")).await.expect("process order o2");
    engine
        .process(payment("o2"))
        .await
        .expect("process payment o2");
    assert!(
        rx.try_recv().is_ok(),
        "sequence must STILL match after hot reload — the Payment route must survive"
    );
}
