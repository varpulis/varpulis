//! Broker-backed gates for the JetStream cluster substrate.
//!
//! These tests are the fail-before/pass-after evidence for the data-loss fix:
//! an inter-pipeline routed event, and a worker heartbeat, published while the
//! consumer is restarting. On the core NATS substrate both are lost — that is
//! the defect, and it is asserted here so it can never be quietly reintroduced.
//! On JetStream both survive.
//!
//! # Requires a real broker
//!
//! A real `nats-server` **with JetStream enabled** (`nats-server -js`). Point
//! elsewhere with `VARPULIS_TEST_NATS_JS_URL` (default
//! `nats://127.0.0.1:4224`).
//!
//! There is no mock and no in-process fake. A missing broker never reports
//! success: with `VARPULIS_REQUIRE_BROKERS=1` (every CI job that provisions a
//! broker) an unreachable server is a hard failure; without it the abstention
//! is reported loudly on stderr and in the GitHub job summary.

#![cfg(feature = "nats-transport")]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use uuid::Uuid;
use varpulis_cluster::coordinator::Coordinator;
use varpulis_cluster::nats_coordinator::run_coordinator_nats_handler_with;
use varpulis_cluster::nats_jetstream::{
    dlq_subject, ClusterTransport, Substrate, DLQ_STREAM, HEARTBEAT_STREAM, MAX_DELIVER,
    ROUTING_STREAM,
};
use varpulis_cluster::nats_transport::{subject_heartbeat, subject_pipeline};
use varpulis_cluster::nats_worker::run_route_ingress;
use varpulis_cluster::routing::build_routing_table;
use varpulis_cluster::{
    HeartbeatRequest, InterPipelineRoute, SharedCoordinator, WorkerCapacity, WorkerId, WorkerNode,
    WorkerStatus,
};

// ---------------------------------------------------------------------------
// Broker gate (same policy as the redis / mqtt / pulsar / cdc connectors —
// test-only, so it is duplicated rather than pulling a new dependency into a
// published crate).
//
// A broker-backed test must NEVER report success because the broker was absent.
// ---------------------------------------------------------------------------

/// Record an abstention. Panics when `VARPULIS_REQUIRE_BROKERS` is set.
#[track_caller]
fn report_skip(test: &str, reason: &str) {
    assert!(
        std::env::var_os("VARPULIS_REQUIRE_BROKERS").is_none(),
        "VARPULIS_REQUIRE_BROKERS=1 but {test} could not reach its broker: {reason}. \
         This test is the fail-before/pass-after gate for a merged fix — it must not \
         pass by abstaining. Fix the broker fixture instead of relaxing the gate."
    );
    eprintln!("SKIPPED(no-broker) {test}: {reason}");
    if let Ok(summary) = std::env::var("GITHUB_STEP_SUMMARY") {
        use std::io::Write as _;
        if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(summary) {
            let _ = writeln!(f, "- :warning: **SKIPPED (no broker)** `{test}` — {reason}");
        }
    }
}

fn js_url() -> String {
    std::env::var("VARPULIS_TEST_NATS_JS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:4224".to_string())
}

/// Connect on the given substrate, or report an abstention and return `None`.
async fn transport_or_skip(test: &str, substrate: Substrate) -> Option<ClusterTransport> {
    let url = js_url();
    match ClusterTransport::connect(&url, substrate).await {
        Ok(t) => Some(t),
        Err(e) => {
            report_skip(test, &format!("{url}: {e}"));
            None
        }
    }
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

/// A `Coordinator` with one registered worker, ready to receive heartbeats.
async fn coordinator_with_worker(worker_id: &str, timeout: Duration) -> SharedCoordinator {
    let coord: SharedCoordinator = Arc::new(RwLock::new(Coordinator::new()));
    {
        let mut c = coord.write().await;
        c.heartbeat_timeout = timeout;
        c.register_worker(WorkerNode {
            id: WorkerId(worker_id.to_string()),
            address: String::new(),
            api_key: varpulis_core::security::SecretString::new("hb-key".to_string()),
            status: WorkerStatus::Ready,
            capacity: WorkerCapacity::default(),
            last_heartbeat: std::time::Instant::now(),
            assigned_pipelines: Vec::new(),
            events_processed: 0,
            heartbeat_seq: 0,
            last_seen_hb_seq: 0,
        });
    }
    coord
}

// ===========================================================================
// GATE 1 — a routed event across a consumer restart
// ===========================================================================

/// **The defect.** Core NATS is fire-and-forget: an event routed to a pipeline
/// that is restarting is gone, and nothing records that it happened.
///
/// This asserts the loss on purpose. It is the "before" half of the gate, kept
/// permanently so a future change that quietly makes core the durable path (or
/// makes this test pass for the wrong reason) is caught.
#[tokio::test]
async fn core_pubsub_loses_routed_event_across_consumer_restart() {
    const TEST: &str = "core_pubsub_loses_routed_event_across_consumer_restart";
    let Some(transport) = transport_or_skip(TEST, Substrate::Core).await else {
        return;
    };
    assert_eq!(transport.substrate(), Substrate::Core);

    let group = unique("grp");
    let subject = subject_pipeline(&group, "ingress", "detect");
    let event = serde_json::json!({
        "event_type": "SuspiciousLogin",
        "fields": { "user": "svc-backup", "src_ip": "10.0.0.9" }
    });

    // The pipeline is up: it holds a subscription.
    let consumer = transport.consume_route("detect", &subject).await.unwrap();
    assert!(!consumer.is_durable(), "core NATS has no durable consumers");

    // The pipeline restarts — the subscription goes away with the process.
    drop(consumer);
    transport.client().flush().await.unwrap();

    // An event is routed to it while it is down.
    transport
        .publish_event(&subject, &event, None)
        .await
        .unwrap();
    transport.client().flush().await.unwrap();

    // The pipeline comes back.
    let mut consumer = transport.consume_route("detect", &subject).await.unwrap();
    transport.client().flush().await.unwrap();

    let delivered = tokio::time::timeout(Duration::from_secs(2), consumer.next()).await;
    assert!(
        delivered.is_err(),
        "core NATS unexpectedly delivered an event published while nobody was \
         subscribed — this test documents the LOSS; if it now survives, the \
         substrate default changed and the gate below is what should be asserting it"
    );
    eprintln!("[{TEST}] core NATS: event published during the restart was LOST (as expected)");
}

/// **The fix.** On JetStream the same event survives the same restart: the
/// server stored it, and the durable consumer resumes from its last
/// acknowledged position when the pipeline comes back.
#[tokio::test]
async fn jetstream_routed_event_survives_consumer_restart() {
    const TEST: &str = "jetstream_routed_event_survives_consumer_restart";
    let Some(transport) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };
    assert_eq!(transport.substrate(), Substrate::JetStream);

    let group = unique("grp");
    let pipeline = unique("detect");
    let subject = subject_pipeline(&group, "ingress", "detect");
    let event = serde_json::json!({
        "event_type": "SuspiciousLogin",
        "fields": { "user": "svc-backup", "src_ip": "10.0.0.9" }
    });

    // The pipeline is up: it holds a durable consumer.
    let consumer = transport.consume_route(&pipeline, &subject).await.unwrap();

    // The pipeline restarts. The client-side puller goes away; the durable
    // consumer stays on the server.
    drop(consumer);

    // An event is routed to it while it is down.
    transport
        .publish_event(&subject, &event, None)
        .await
        .unwrap();

    // The pipeline comes back and finds its own event waiting.
    let mut consumer = transport.consume_route(&pipeline, &subject).await.unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(10), consumer.next())
        .await
        .expect(
            "the event published while the consumer was restarting was LOST — a durable \
             JetStream consumer must resume from its last acknowledged position",
        )
        .expect("consumer closed");
    assert!(consumer.is_durable(), "JetStream consumer must be durable");

    let received: serde_json::Value = delivery.json().unwrap();
    assert_eq!(received["event_type"], "SuspiciousLogin");
    assert_eq!(received["fields"]["src_ip"], "10.0.0.9");
    assert_eq!(delivery.delivery_attempt(), 1);
    eprintln!(
        "[{TEST}] JetStream: event published during the restart SURVIVED \
         (subject {subject}, attempt {})",
        delivery.delivery_attempt()
    );

    // Apply, then ack — in that order.
    delivery.ack().await.unwrap();

    // And it is not redelivered once acked.
    let mut consumer = transport.consume_route(&pipeline, &subject).await.unwrap();
    let again = tokio::time::timeout(Duration::from_secs(2), consumer.next()).await;
    assert!(
        again.is_err(),
        "an acked event must not be redelivered — that would double-apply it"
    );
}

// ===========================================================================
// GATE 2 — the ack discipline: ack after apply, never on receipt
// ===========================================================================

/// A crash between delivery and ack must redeliver, not lose.
///
/// This is what "ack after the event is applied to engine state" buys, and the
/// reason `Delivery::ack` takes `self` by value: a delivery dropped without an
/// ack (the process died mid-apply) comes back.
#[tokio::test]
async fn unacked_event_is_redelivered_after_a_crash_mid_apply() {
    const TEST: &str = "unacked_event_is_redelivered_after_a_crash_mid_apply";
    let Some(transport) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };

    let group = unique("grp");
    let pipeline = unique("detect");
    let subject = subject_pipeline(&group, "ingress", "detect");
    transport
        .publish_event(
            &subject,
            &serde_json::json!({ "event_type": "Beacon", "fields": { "n": 1 } }),
            None,
        )
        .await
        .unwrap();

    // Delivery 1: received, then the process "crashes" before acking.
    let mut consumer = transport.consume_route(&pipeline, &subject).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), consumer.next())
        .await
        .expect("timed out on first delivery")
        .expect("consumer closed");
    assert_eq!(first.delivery_attempt(), 1);
    // Nak rather than waiting out `ack_wait` — the same "not applied" signal the
    // ingress loop sends when `process_event` fails, and it keeps the test fast.
    first.nak().await.unwrap();
    drop(consumer);

    // Delivery 2: the event is still there.
    let mut consumer = transport.consume_route(&pipeline, &subject).await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(10), consumer.next())
        .await
        .expect("event was LOST after an un-acked delivery")
        .expect("consumer closed");
    assert_eq!(
        second.delivery_attempt(),
        2,
        "redelivery must be visible to the consumer so it can count attempts"
    );
    eprintln!("[{TEST}] un-acked delivery came back as attempt 2");
    second.ack().await.unwrap();
}

/// A poison event is parked in the DLQ and acked — dropped *and traced*, never
/// looped forever and never silently vanished.
#[tokio::test]
async fn poison_event_is_dead_lettered_not_looped() {
    const TEST: &str = "poison_event_is_dead_lettered_not_looped";
    let Some(transport) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };

    let group = unique("grp");
    let pipeline = unique("poison");
    let subject = subject_pipeline(&group, "ingress", "detect");
    transport
        .publish_event(
            &subject,
            &serde_json::json!({ "event_type": "AlwaysFails", "fields": {} }),
            None,
        )
        .await
        .unwrap();

    let mut consumer = transport.consume_route(&pipeline, &subject).await.unwrap();

    loop {
        let delivery = tokio::time::timeout(Duration::from_secs(15), consumer.next())
            .await
            .expect("timed out waiting for a redelivery")
            .expect("consumer closed");
        if delivery.is_poison() {
            assert_eq!(
                delivery.delivery_attempt(),
                MAX_DELIVER,
                "poison is declared at max_deliver"
            );
            delivery
                .dead_letter("apply failed every time")
                .await
                .unwrap();
            break;
        }
        assert!(delivery.delivery_attempt() < MAX_DELIVER);
        delivery.nak().await.unwrap();
    }

    // Parked, and acked: nothing comes back.
    let further = tokio::time::timeout(Duration::from_secs(3), consumer.next()).await;
    assert!(
        further.is_err(),
        "a dead-lettered event must be acked, not left to loop"
    );

    // And the death envelope is really in the DLQ stream.
    let ctx = transport.context().expect("jetstream context");
    let dlq = ctx.get_stream(DLQ_STREAM).await.expect("DLQ stream");
    let parked = dlq_subject(&pipeline);
    let msg = tokio::time::timeout(
        Duration::from_secs(5),
        dlq.get_last_raw_message_by_subject(&parked),
    )
    .await
    .expect("timed out reading the DLQ")
    .expect("no death envelope in the DLQ");
    let envelope: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(envelope["original_subject"], subject);
    assert_eq!(envelope["attempts"], MAX_DELIVER);
    assert_eq!(envelope["last_error"], "apply failed every time");
    eprintln!("[{TEST}] poison parked in {DLQ_STREAM} after {MAX_DELIVER} deliveries");
}

// ===========================================================================
// GATE 3 — the end-to-end ingress: event reaches engine state
// ===========================================================================

/// The production ingress loop, against a real engine: an event routed while
/// the pipeline is down is applied to engine state once the pipeline is back.
///
/// Observed through `collect_pipeline_metrics`, which counts events the engine
/// actually processed — not through the transport, so this proves the ack
/// happens on the far side of a real apply.
#[tokio::test]
async fn route_ingress_applies_an_event_published_while_the_pipeline_was_down() {
    const TEST: &str = "route_ingress_applies_an_event_published_while_the_pipeline_was_down";
    let Some(transport) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };

    let api_key = unique("key");
    let pipeline_name = unique("detect");
    let tm: varpulis_runtime::SharedTenantManager =
        Arc::new(RwLock::new(varpulis_runtime::TenantManager::new()));
    let pipeline_id = {
        let mut mgr = tm.write().await;
        mgr.create_tenant(
            unique("tenant"),
            api_key.clone(),
            varpulis_runtime::TenantQuota::default(),
        )
        .unwrap();
        let tid = mgr.get_tenant_by_api_key(&api_key).cloned().unwrap();
        mgr.deploy_pipeline_on_tenant(
            &tid,
            pipeline_name.clone(),
            "stream A = SensorReading .where(temperature > 100)".to_string(),
        )
        .await
        .unwrap()
    };

    let group = unique("grp");
    let routes = vec![InterPipelineRoute {
        from_pipeline: "ingress".to_string(),
        to_pipeline: pipeline_name.clone(),
        event_types: vec!["Sensor*".to_string()],
        nats_subject: None,
    }];
    let table = build_routing_table(&group, &routes);
    let (subject, filter) = table.input_subscriptions[&pipeline_name][0].clone();

    // Create the durable consumer, then take the pipeline down. (Binding once
    // first is what makes this a *restart* rather than a cold start.)
    drop(
        transport
            .consume_route(&pipeline_name, &subject)
            .await
            .unwrap(),
    );

    // Route an event to the pipeline while nothing is consuming.
    transport
        .publish_event(
            &subject,
            &serde_json::json!({
                "event_type": "SensorReading",
                "fields": { "temperature": 150.0, "sensor_id": "s1" }
            }),
            None,
        )
        .await
        .unwrap();

    let before = events_in(&tm, &pipeline_name).await;
    assert_eq!(before, 0, "nothing applied yet");

    // Pipeline comes back and its ingress loop starts.
    let ingress = {
        let transport = transport.clone();
        let pipeline_name = pipeline_name.clone();
        let pipeline_id = pipeline_id.clone();
        let subject = subject.clone();
        let api_key = api_key.clone();
        let tm = tm.clone();
        tokio::spawn(async move {
            run_route_ingress(
                transport,
                &pipeline_name,
                &pipeline_id,
                &subject,
                &filter,
                &api_key,
                tm,
            )
            .await;
        })
    };

    let mut applied = 0;
    for _ in 0..100 {
        applied = events_in(&tm, &pipeline_name).await;
        if applied > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ingress.abort();

    assert_eq!(
        applied, 1,
        "the event routed while the pipeline was down never reached engine state"
    );
    eprintln!("[{TEST}] event routed during downtime reached engine state (events_in = {applied})");
}

async fn events_in(tm: &varpulis_runtime::SharedTenantManager, pipeline: &str) -> u64 {
    let mgr = tm.read().await;
    mgr.collect_pipeline_metrics()
        .await
        .into_iter()
        .find(|(name, _, _)| name == pipeline)
        .map(|(_, events_in, _)| events_in)
        .unwrap_or(0)
}

// ===========================================================================
// GATE 4 — heartbeats across a coordinator restart
// ===========================================================================

/// **The defect.** A heartbeat published while the coordinator is restarting is
/// lost on core NATS, and it feeds a liveness decision.
#[tokio::test]
async fn core_pubsub_loses_heartbeat_published_while_coordinator_is_down() {
    const TEST: &str = "core_pubsub_loses_heartbeat_published_while_coordinator_is_down";
    let Some(transport) = transport_or_skip(TEST, Substrate::Core).await else {
        return;
    };

    let worker_id = unique("w");
    let coord = coordinator_with_worker(&worker_id, Duration::from_mins(1)).await;

    // Coordinator is down. The worker heartbeats anyway.
    transport
        .publish_event(
            &subject_heartbeat(&worker_id),
            &HeartbeatRequest {
                events_processed: 4242,
                pipelines_running: 3,
                pipeline_metrics: vec![],
            },
            None,
        )
        .await
        .unwrap();
    transport.client().flush().await.unwrap();

    // Coordinator comes back.
    let handler = {
        let client = transport.client().clone();
        let coord = coord.clone();
        let id = unique("coord");
        tokio::spawn(async move {
            run_coordinator_nats_handler_with(client, coord, Substrate::Core, &id).await;
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    handler.abort();

    let c = coord.read().await;
    let worker = c.workers.get(&WorkerId(worker_id.clone())).unwrap();
    assert_eq!(
        worker.events_processed, 0,
        "core NATS unexpectedly delivered a heartbeat published while the coordinator \
         was down — this test documents the LOSS"
    );
    eprintln!("[{TEST}] core NATS: heartbeat sent during the restart was LOST (as expected)");
}

/// **The fix.** The same heartbeat survives the same restart on JetStream, and
/// the liveness decision is made on real evidence.
#[tokio::test]
async fn jetstream_heartbeat_survives_coordinator_restart() {
    const TEST: &str = "jetstream_heartbeat_survives_coordinator_restart";
    let Some(transport) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };

    let worker_id = unique("w");
    let coord = coordinator_with_worker(&worker_id, Duration::from_mins(1)).await;

    // Coordinator is down. The worker heartbeats anyway.
    transport
        .publish_event(
            &subject_heartbeat(&worker_id),
            &HeartbeatRequest {
                events_processed: 4242,
                pipelines_running: 3,
                pipeline_metrics: vec![],
            },
            None,
        )
        .await
        .unwrap();

    // Coordinator comes back with its durable consumer.
    let coordinator_id = unique("coord");
    let handler = {
        let client = transport.client().clone();
        let coord = coord.clone();
        let id = coordinator_id.clone();
        tokio::spawn(async move {
            run_coordinator_nats_handler_with(client, coord, Substrate::JetStream, &id).await;
        })
    };

    let mut seen = 0;
    for _ in 0..100 {
        seen = coord
            .read()
            .await
            .workers
            .get(&WorkerId(worker_id.clone()))
            .map(|w| w.events_processed)
            .unwrap_or(0);
        if seen > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    handler.abort();

    assert_eq!(
        seen, 4242,
        "the heartbeat published while the coordinator was down never reached it"
    );
    let c = coord.read().await;
    let worker = c.workers.get(&WorkerId(worker_id.clone())).unwrap();
    assert_eq!(worker.capacity.pipelines_running, 3);
    eprintln!("[{TEST}] JetStream: heartbeat sent during the restart SURVIVED");
}

/// A *replayed* heartbeat older than the liveness timeout is acked and
/// discarded, not applied.
///
/// Durability cuts both ways: `Coordinator::heartbeat` stamps
/// `last_heartbeat = now`, so applying a stale replay would resurrect a worker
/// that has since died. The consumer must therefore look at the message's
/// publish time, not just its existence.
#[tokio::test]
async fn replayed_heartbeat_older_than_the_liveness_timeout_is_discarded() {
    const TEST: &str = "replayed_heartbeat_older_than_the_liveness_timeout_is_discarded";
    let Some(transport) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };

    let worker_id = unique("w");
    // A 1s liveness timeout: anything published more than a second ago is stale.
    let coord = coordinator_with_worker(&worker_id, Duration::from_secs(1)).await;

    transport
        .publish_event(
            &subject_heartbeat(&worker_id),
            &HeartbeatRequest {
                events_processed: 999,
                pipelines_running: 1,
                pipeline_metrics: vec![],
            },
            None,
        )
        .await
        .unwrap();

    // Let it age past the timeout before the coordinator starts.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let handler = {
        let client = transport.client().clone();
        let coord = coord.clone();
        let id = unique("coord");
        tokio::spawn(async move {
            run_coordinator_nats_handler_with(client, coord, Substrate::JetStream, &id).await;
        })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    handler.abort();

    let c = coord.read().await;
    let worker = c.workers.get(&WorkerId(worker_id.clone())).unwrap();
    assert_eq!(
        worker.events_processed, 0,
        "a heartbeat older than the liveness timeout must not be applied — it would \
         mark a dead worker alive"
    );
    eprintln!("[{TEST}] stale replayed heartbeat was acked and discarded, not applied");
}

// ===========================================================================
// GATE 5 — backwards compatibility and provisioning
// ===========================================================================

/// Requesting JetStream against a server that does not have it must fail
/// loudly, never fall back to core: a silent downgrade would hand an operator
/// who asked for durability the exact fire-and-forget loss they were removing.
#[tokio::test]
async fn requesting_jetstream_on_a_core_only_server_fails_loudly() {
    const TEST: &str = "requesting_jetstream_on_a_core_only_server_fails_loudly";

    // A real second broker with JetStream OFF. No mock. In CI this is the
    // `nats:latest` service container, whose default command has no `-js`
    // (VARPULIS_TEST_NATS_CORE_URL); on a dev box we start one ourselves.
    let (url, mut child) = match std::env::var("VARPULIS_TEST_NATS_CORE_URL") {
        Ok(u) if !u.trim().is_empty() => (u, None),
        _ => {
            let port = 4225 + (std::process::id() % 100) as u16;
            let Ok(child) = std::process::Command::new("nats-server")
                .args(["-p", &port.to_string(), "-a", "127.0.0.1"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
            else {
                report_skip(
                    TEST,
                    "no core-only broker: set VARPULIS_TEST_NATS_CORE_URL, or put \
                     `nats-server` on PATH so one can be started",
                );
                return;
            };
            tokio::time::sleep(Duration::from_millis(500)).await;
            (format!("nats://127.0.0.1:{port}"), Some(child))
        }
    };

    let stop = |child: &mut Option<std::process::Child>| {
        if let Some(c) = child.as_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
    };

    // Core works against it. If it does not, the fixture never came up (port
    // clash, for instance) — abstain rather than assert on the wrong failure.
    if let Err(e) = ClusterTransport::connect(&url, Substrate::Core).await {
        stop(&mut child);
        report_skip(
            TEST,
            &format!("core-only server not reachable at {url}: {e}"),
        );
        return;
    }

    // JetStream does not, and says why.
    let js = ClusterTransport::connect(&url, Substrate::JetStream).await;
    let js_err = js.err().map(|e| e.to_string());

    stop(&mut child);

    let err = js_err.expect("JetStream must NOT silently fall back to core pub/sub");
    assert!(
        err.contains("does not have it enabled") && err.contains("-js"),
        "the error must tell the operator how to fix it, got: {err}"
    );
    eprintln!("[{TEST}] {err}");
}

/// The substrate default is Core, so an existing deployment on a plain
/// `nats-server` is unaffected by upgrading Varpulis.
#[tokio::test]
async fn core_substrate_still_round_trips_on_a_jetstream_server() {
    const TEST: &str = "core_substrate_still_round_trips_on_a_jetstream_server";
    let Some(transport) = transport_or_skip(TEST, Substrate::Core).await else {
        return;
    };

    let subject = subject_pipeline(&unique("grp"), "a", "b");
    let mut consumer = transport.consume_route("b", &subject).await.unwrap();
    transport.client().flush().await.unwrap();

    transport
        .publish_event(
            &subject,
            &serde_json::json!({ "event_type": "Ping", "fields": {} }),
            None,
        )
        .await
        .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("core pub/sub must still deliver to a live subscriber")
        .expect("consumer closed");
    let v: serde_json::Value = delivery.json().unwrap();
    assert_eq!(v["event_type"], "Ping");
    // Ack is a no-op on core; it must not error.
    delivery.ack().await.unwrap();
    eprintln!("[{TEST}] core pub/sub unchanged");
}

/// Provisioning is idempotent and creates exactly the streams documented.
#[tokio::test]
async fn streams_are_provisioned_with_the_documented_limits() {
    const TEST: &str = "streams_are_provisioned_with_the_documented_limits";
    let Some(transport) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };
    // Connecting twice must not fail — `ensure_streams` runs on every connect.
    let Some(_second) = transport_or_skip(TEST, Substrate::JetStream).await else {
        return;
    };

    let ctx = transport.context().expect("jetstream context");
    for (name, subject) in [
        (ROUTING_STREAM, "varpulis.cluster.pipeline.>"),
        (HEARTBEAT_STREAM, "varpulis.cluster.heartbeat.>"),
        (DLQ_STREAM, "varpulis.dlq.>"),
    ] {
        let mut stream = ctx.get_stream(name).await.unwrap_or_else(|e| {
            panic!("stream {name} was not provisioned: {e}");
        });
        let info = stream.info().await.expect("stream info");
        assert_eq!(info.config.subjects, vec![subject.to_string()], "{name}");
        assert_eq!(
            info.config.storage,
            async_nats::jetstream::stream::StorageType::File,
            "{name} must survive a broker restart"
        );
        assert_eq!(
            info.config.discard,
            async_nats::jetstream::stream::DiscardPolicy::Old,
            "{name} must evict the oldest at its limit, not reject new publishes"
        );
        assert_eq!(
            info.config.retention,
            async_nats::jetstream::stream::RetentionPolicy::Limits,
            "{name} must not delete on ack — replay and fan-out depend on it"
        );
    }

    let mut routing = ctx.get_stream(ROUTING_STREAM).await.unwrap();
    let info = routing.info().await.unwrap();
    assert_eq!(info.config.max_age, Duration::from_hours(1));
    assert_eq!(info.config.max_bytes, 1024 * 1024 * 1024);
    eprintln!("[{TEST}] streams provisioned; re-provisioning is idempotent");
}
