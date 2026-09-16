//! Regression test for audit critical **C5** on the NATS heartbeat path.
//!
//! The heartbeat handlers replicate a worker's monotonic `heartbeat_seq`
//! (`ClusterCommand::WorkerMetricsUpdated`) so that *every* coordinator — not
//! just the one a worker is homed on — can tell a live worker from a dead one.
//! Before the fix, the NATS heartbeat handler advanced only the receiving
//! coordinator's *local* `heartbeat_seq` and never replicated it, so a
//! NATS-homed worker on a non-leader coordinator was invisible to every other
//! coordinator and could be false-marked `Unhealthy`.
//!
//! This test drives a heartbeat over the **real NATS broker** into the real
//! `run_coordinator_nats_handler`, wired to a real `Coordinator` backed by the
//! real JetStream KV control plane, then asserts the **replicated**
//! `heartbeat_seq` in the bucket advanced — i.e. the NATS heartbeat reached
//! the control plane. No mocking: real broker, real bucket.
//!
//! It was written against openraft with a single-node in-process `SimRouter`
//! harness, about 250 lines of it. Raft is gone; the property is not, and it
//! is now asserted against the backend that actually runs. The harness goes
//! with the backend it simulated.
//!
//! Fail-before / pass-after gate: deleting the replication call in
//! `nats_coordinator::handle_heartbeat_message` leaves the replicated
//! `heartbeat_seq` frozen at 0, so the "advanced past 0" assertion times out
//! and the test fails.
//!
//! ```bash
//! VARPULIS_TEST_NATS_URL=nats://127.0.0.1:4231 VARPULIS_REQUIRE_BROKERS=1 \
//!   cargo test -p varpulis-cluster --features jetstream-control-plane,nats-transport \
//!   --test nats_heartbeat_replication
//! ```
#![cfg(all(feature = "jetstream-control-plane", feature = "nats-transport"))]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use uuid::Uuid;
use varpulis_cluster::control_state::ClusterCommand;
use varpulis_cluster::jetstream_control_plane::{
    Applier, ControlKey, ControlPlane, ControlPlaneConfig, WorkerRecord,
};
use varpulis_cluster::nats_coordinator::run_coordinator_nats_handler;
use varpulis_cluster::nats_transport::{connect_nats, nats_publish, subject_heartbeat};
use varpulis_cluster::worker::{HeartbeatRequest, WorkerCapacity};
use varpulis_cluster::{Coordinator, SharedCoordinator};

fn nats_url() -> String {
    std::env::var("VARPULIS_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

fn replicas() -> usize {
    std::env::var("VARPULIS_CONTROL_PLANE_REPLICAS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// Abstention is only ever allowed when the caller did not promise a broker.
#[track_caller]
fn abstain(reason: &str) {
    assert!(
        std::env::var_os("VARPULIS_REQUIRE_BROKERS").is_none(),
        "VARPULIS_REQUIRE_BROKERS=1 but the broker was unreachable: {reason}. \
         This test is a fail-before/pass-after gate for a merged fix — it must \
         not pass by abstaining."
    );
    eprintln!("[skip] {reason}");
}

/// The replicated `heartbeat_seq` for a worker, read back out of the bucket.
async fn replicated(cp: &ControlPlane, worker_id: &str) -> Option<(u64, u64)> {
    cp.get::<WorkerRecord>(&ControlKey::Worker(worker_id.to_string()))
        .await
        .ok()
        .flatten()
        .map(|v| (v.value.entry.heartbeat_seq, v.value.entry.events_processed))
}

/// Poll the bucket until the worker's replicated `heartbeat_seq` reaches `min`.
async fn wait_for_seq(
    cp: &ControlPlane,
    worker_id: &str,
    min: u64,
    timeout: Duration,
) -> Option<(u64, u64)> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some((seq, events)) = replicated(cp, worker_id).await {
            if seq >= min {
                return Some((seq, events));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn nats_heartbeat_replicates_seq_through_the_control_plane() {
    let url = nats_url();

    let client = match connect_nats(&url).await {
        Ok(c) => c,
        Err(e) => return abstain(&format!("NATS unavailable at {url}: {e}")),
    };

    let cfg = ControlPlaneConfig {
        url: url.clone(),
        bucket: format!("VTEST_NATSHB_{}", std::process::id()),
        // No entry expiry: this test is about replication, not TTL failover,
        // and a record ageing out mid-test would be a confusing false negative.
        ttl: Duration::ZERO,
        num_replicas: replicas(),
        history: 4,
    };
    let cp = match ControlPlane::connect(&cfg).await {
        Ok(cp) => cp,
        Err(e) => return abstain(&format!("JetStream unavailable at {url}: {e}")),
    };
    let applier = Applier::new(cp.clone());

    // 1. Register a worker THROUGH the control plane, so a replicated record
    //    with seq = 0 exists. Unique id so parallel runs do not collide.
    let worker_id = format!("nats-hb-{}", Uuid::new_v4().simple());
    applier
        .apply(ClusterCommand::RegisterWorker {
            id: worker_id.clone(),
            address: "localhost:9100".to_string(),
            api_key: "hb-key".to_string(),
            capacity: WorkerCapacity {
                cpu_cores: 4,
                pipelines_running: 0,
                max_pipelines: 100,
            },
        })
        .await
        .expect("register worker through the control plane");

    assert_eq!(
        replicated(&cp, &worker_id).await.map(|(seq, _)| seq),
        Some(0),
        "a freshly-registered worker must have replicated heartbeat_seq = 0"
    );

    // 2. A real Coordinator on that control plane, with the worker pulled into
    //    its local map so the NATS handler's `heartbeat()` finds it.
    //
    //    The bucket is replicated, so a snapshot taken immediately after the
    //    write can legitimately not contain it yet. That is fine in production
    //    — the sweep is level-triggered and picks it up next tick — but a test
    //    must not assume immediacy, so it sweeps until it sees it.
    let mut coord = Coordinator::new();
    coord.control_plane = Some(applier);
    let wid = varpulis_cluster::WorkerId(worker_id.clone());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        coord.sync_from_control_plane().await;
        if coord.workers.contains_key(&wid) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the coordinator never materialised the worker it is about to hear from"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let coordinator: SharedCoordinator = Arc::new(RwLock::new(coord));

    // 3. The REAL coordinator-side NATS handler against the live broker.
    let handler = tokio::spawn(run_coordinator_nats_handler(
        client.clone(),
        coordinator.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4. First heartbeat over NATS, on the subject the handler subscribes to.
    nats_publish(
        &client,
        &subject_heartbeat(&worker_id),
        &HeartbeatRequest {
            events_processed: 111,
            pipelines_running: 1,
            pipeline_metrics: vec![],
        },
    )
    .await
    .expect("publish heartbeat 1");
    client.flush().await.expect("flush");

    let (seq1, events1) = wait_for_seq(&cp, &worker_id, 1, Duration::from_secs(10))
        .await
        .unwrap_or_else(|| {
            panic!(
                "C5(NATS): the first NATS heartbeat did NOT advance the replicated \
                 heartbeat_seq past 0 — it never reached the control plane"
            )
        });
    assert!(seq1 >= 1, "replicated seq must reach >= 1, got {seq1}");
    assert_eq!(
        events1, 111,
        "events_processed must ride along on the same replicated command"
    );

    // 5. Second heartbeat: the replicated seq must advance again.
    nats_publish(
        &client,
        &subject_heartbeat(&worker_id),
        &HeartbeatRequest {
            events_processed: 222,
            pipelines_running: 2,
            pipeline_metrics: vec![],
        },
    )
    .await
    .expect("publish heartbeat 2");
    client.flush().await.expect("flush");

    let (seq2, events2) = wait_for_seq(&cp, &worker_id, seq1 + 1, Duration::from_secs(10))
        .await
        .unwrap_or_else(|| {
            panic!(
                "C5(NATS): the second NATS heartbeat did NOT advance the replicated \
                 heartbeat_seq again (still {seq1})"
            )
        });
    assert!(
        seq2 > seq1,
        "the second heartbeat must advance the replicated seq: {seq1} -> {seq2}"
    );
    assert_eq!(
        events2, 222,
        "the second heartbeat's metrics must replicate too"
    );

    handler.abort();
    cp.delete(&ControlKey::Worker(worker_id)).await.ok();
}
