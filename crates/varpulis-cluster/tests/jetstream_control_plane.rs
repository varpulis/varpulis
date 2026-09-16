//! Integration tests for the JetStream KV control plane, against a **real**
//! `nats-server` with JetStream enabled.
//!
//! ```bash
//! docker run -d --name varpulis-js-test -p 4223:4222 nats:2.10-alpine -js -sd /tmp/js
//! VARPULIS_TEST_NATS_URL=nats://127.0.0.1:4223 \
//!   cargo test -p varpulis-cluster --features jetstream-control-plane \
//!   --test jetstream_control_plane
//! ```
//!
//! A single server exercises the logic but not the durability. Against a
//! three-node cluster, `VARPULIS_CONTROL_PLANE_REPLICAS=3` runs the same
//! suite on a replicated bucket — which is what a deployment that has
//! replaced Raft with this actually has, and the only configuration in which
//! passing here says anything about surviving a broker failure.
//!
//! ```bash
//! scripts/nats-cluster.sh start
//! VARPULIS_TEST_NATS_URL=nats://127.0.0.1:4231 \
//!   VARPULIS_CONTROL_PLANE_REPLICAS=3 VARPULIS_REQUIRE_BROKERS=1 \
//!   cargo test -p varpulis-cluster --features jetstream-control-plane \
//!   --test jetstream_control_plane
//! ```
//!
//! No mocking, and no abstaining-as-passing: with `VARPULIS_REQUIRE_BROKERS=1`
//! an unreachable broker is a hard failure. Without it the abstention is
//! reported loudly on stderr and in the GitHub job summary.
#![cfg(feature = "jetstream-control-plane")]

use std::time::Duration;

use varpulis_cluster::control_state::{
    apply_command, ClusterCommand, ClusterResponse, CoordinatorState, WorkerEntry,
};
use varpulis_cluster::jetstream_control_plane::{
    fence_out, materialize, now_ms, Applier, ConnectorSecretPolicy, ControlKey, ControlPlane,
    ControlPlaneConfig, Expect, FencedCommand, LeaderLease, LeaderState, MigrationPhase,
    MigrationRecord, Reconciler, WorkerLease, WorkerRecord, STATUS_FENCED,
};
use varpulis_cluster::worker::{PipelineMetrics, WorkerCapacity};

// ---------------------------------------------------------------------------
// Broker gate (mirrors crates/varpulis-connector-redis/src/lib.rs)
// ---------------------------------------------------------------------------
//
// A broker-backed test must NEVER report success because the broker was
// absent. Every CI job that provisions a broker sets
// `VARPULIS_REQUIRE_BROKERS=1`; under that flag a missing broker is a hard
// failure. Without the flag (a dev box with nothing running) the abstention is
// reported loudly so nobody mistakes it for a pass.

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

fn nats_url() -> String {
    std::env::var("VARPULIS_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4223".to_string())
}

/// How many JetStream replicas the test buckets should have.
///
/// 1 by default so the documented single-node `docker run` still works.
fn replicas() -> usize {
    std::env::var("VARPULIS_CONTROL_PLANE_REPLICAS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1)
        .clamp(1, 5)
}

/// Open a control plane on a bucket private to this test.
///
/// Returns `None` (after reporting the skip) when the broker is unreachable.
async fn open(test: &str) -> Option<ControlPlane> {
    let bucket = format!(
        "VTEST_{}_{}",
        test.to_uppercase()
            .replace(|c: char| !c.is_alphanumeric(), "_"),
        std::process::id()
    );
    let bucket: String = bucket.chars().take(60).collect();
    let cfg = ControlPlaneConfig {
        url: nats_url(),
        bucket,
        // No entry expiry: these tests exercise fencing, not TTL failover, and
        // a record ageing out mid-test would be a confusing false negative.
        ttl: Duration::ZERO,
        // Replication is the whole of this control plane's durability, and
        // hardcoding 1 meant the suite never exercised it: every property
        // below was only ever checked against a bucket living on one server.
        // Point `VARPULIS_TEST_NATS_URL` at a clustered broker and set
        // `VARPULIS_CONTROL_PLANE_REPLICAS=3` to run the same suite against a
        // replicated bucket, which is what a deployment replacing Raft has.
        num_replicas: replicas(),
        history: 8,
    };
    match ControlPlane::connect(&cfg).await {
        Ok(cp) => Some(cp),
        Err(e) => {
            report_skip(test, &format!("no JetStream at {}: {e}", nats_url()));
            None
        }
    }
}

/// The bucket really has the replication that was asked for.
///
/// `num_replicas` is the whole of this control plane's durability, and until
/// recently nothing could set it: `ControlPlaneConfig::from_env` read the URL,
/// the bucket and the TTL and not the replica count, so every deployment
/// configured the documented way ran on one replica. This asserts the number
/// the broker reports, not the number the config holds, because binding to a
/// pre-existing bucket ignores the requested count entirely — a coordinator
/// can believe it asked for three and be running on one.
///
/// Against a single server it asserts 1, which is the honest answer there.
/// Against `scripts/nats-cluster.sh` with `VARPULIS_CONTROL_PLANE_REPLICAS=3`
/// it asserts 3, and that is the configuration in which the rest of this file
/// says anything about surviving a broker failure.
#[tokio::test]
async fn the_bucket_has_the_replication_that_was_asked_for() {
    let Some(cp) = open("replication").await else {
        return;
    };
    let want = replicas();
    let got = cp.replicas().await.expect("bucket status");
    assert_eq!(
        got, want,
        "asked for {want} replica(s) and the broker reports {got}; the control \
         plane's durability is exactly this number"
    );
}

fn worker(id: &str, pipelines: &[&str]) -> WorkerRecord {
    WorkerRecord::new(WorkerEntry {
        id: id.to_string(),
        address: format!("http://{id}:9000"),
        api_key: "k".to_string(),
        status: "ready".to_string(),
        cpu_cores: 4,
        pipelines_running: pipelines.len(),
        max_pipelines: 16,
        assigned_pipelines: pipelines.iter().map(|s| s.to_string()).collect(),
        events_processed: 0,
        heartbeat_seq: 0,
    })
}

/// A `Coordinator` wired to this control plane and nothing else.
fn coordinator_on(cp: &ControlPlane) -> varpulis_cluster::Coordinator {
    let mut c = varpulis_cluster::Coordinator::new();
    c.control_plane = Some(Applier::new(cp.clone()));
    c
}

/// The minimum migration plan `record_migration_started` reads from.
fn migrate_plan(
    id: &str,
    pipeline: &str,
    group: &str,
    source: &str,
    target: &str,
) -> varpulis_cluster::coordinator::MigratePipelinePlan {
    use varpulis_cluster::pipeline_group::{PipelineDeployment, PipelineDeploymentStatus};
    use varpulis_cluster::WorkerId;

    varpulis_cluster::coordinator::MigratePipelinePlan {
        migration_id: id.to_string(),
        pipeline_name: pipeline.to_string(),
        group_id: group.to_string(),
        source_worker_id: WorkerId(source.to_string()),
        target_worker_id: WorkerId(target.to_string()),
        target_address: format!("http://{target}:9000"),
        target_api_key: "k".to_string(),
        deployment: PipelineDeployment {
            worker_id: WorkerId(source.to_string()),
            worker_address: format!("http://{source}:9000"),
            worker_api_key: "k".to_string(),
            pipeline_id: format!("{pipeline}-1"),
            status: PipelineDeploymentStatus::Running,
            epoch: 1,
            failure_reason: None,
        },
        vpl_source: "stream s from e".to_string(),
        reason: varpulis_cluster::MigrationReason::Manual,
        migrate_start: std::time::Instant::now(),
    }
}

// ===========================================================================
// 1. Parity: the KV backend applies the same commands to the same state
// ===========================================================================

/// The whole premise of the migration is that the KV backend is a *different
/// substrate for the same state machine*, not a different state machine.
///
/// This drives the identical command sequence through
/// `control_state::apply_command` (what Raft's state machine does) and through
/// the KV `Applier`, then asserts the two resulting `CoordinatorState` values
/// are byte-identical after serialisation.
#[tokio::test]
async fn kv_backend_matches_the_raft_state_machine_command_for_command() {
    const TEST: &str = "kv_backend_matches_the_raft_state_machine_command_for_command";
    let Some(cp) = open("parity").await else {
        return;
    };
    let applier = Applier::new(cp.clone());

    let commands = vec![
        ClusterCommand::RegisterWorker {
            id: "w1".into(),
            address: "http://w1:9000".into(),
            api_key: "k1".into(),
            capacity: WorkerCapacity {
                cpu_cores: 4,
                pipelines_running: 0,
                max_pipelines: 16,
            },
        },
        ClusterCommand::RegisterWorker {
            id: "w2".into(),
            address: "http://w2:9000".into(),
            api_key: "k2".into(),
            capacity: WorkerCapacity {
                cpu_cores: 8,
                pipelines_running: 0,
                max_pipelines: 32,
            },
        },
        ClusterCommand::WorkerStatusChanged {
            id: "w2".into(),
            status: "draining".into(),
        },
        ClusterCommand::WorkerPipelinesUpdated {
            id: "w1".into(),
            assigned_pipelines: vec!["p1".into(), "p2".into()],
        },
        ClusterCommand::WorkerMetricsUpdated {
            id: "w1".into(),
            events_processed: 5_000,
            pipelines_running: 2,
            pipeline_metrics: vec![PipelineMetrics {
                pipeline_name: "p1".into(),
                events_in: 5_000,
                events_out: 42,
                connector_health: vec![],
            }],
            heartbeat_seq: 3,
        },
        // Unknown worker: both backends must no-op, not resurrect.
        ClusterCommand::WorkerStatusChanged {
            id: "ghost".into(),
            status: "ready".into(),
        },
        ClusterCommand::WorkerMetricsUpdated {
            id: "ghost".into(),
            events_processed: 1,
            pipelines_running: 1,
            pipeline_metrics: vec![],
            heartbeat_seq: 1,
        },
        ClusterCommand::GroupDeployed {
            name: "g1".into(),
            group: serde_json::json!({"status": "running", "placements": {}}),
        },
        ClusterCommand::GroupUpdated {
            name: "g1".into(),
            group: serde_json::json!({"status": "degraded", "placements": {}}),
        },
        ClusterCommand::GroupDeployed {
            name: "g2".into(),
            group: serde_json::json!({"status": "running"}),
        },
        ClusterCommand::GroupRemoved { name: "g2".into() },
        ClusterCommand::MigrationStarted {
            task: serde_json::json!({"id": "m1", "status": "checkpointing"}),
        },
        ClusterCommand::MigrationUpdated {
            id: "m1".into(),
            status: "deploying".into(),
        },
        // No id: both backends must drop it.
        ClusterCommand::MigrationStarted {
            task: serde_json::json!({"status": "orphan"}),
        },
        ClusterCommand::MigrationStarted {
            task: serde_json::json!({"id": "m2", "status": "checkpointing"}),
        },
        ClusterCommand::MigrationRemoved { id: "m2".into() },
        ClusterCommand::ConnectorCreated {
            name: "c1".into(),
            connector: varpulis_cluster::ClusterConnector {
                name: "c1".into(),
                connector_type: "mqtt".into(),
                params: std::iter::once(("host".to_string(), "localhost".to_string())).collect(),
                description: None,
            },
        },
        ClusterCommand::ConnectorUpdated {
            name: "c1".into(),
            connector: varpulis_cluster::ClusterConnector {
                name: "c1".into(),
                connector_type: "mqtt".into(),
                params: std::iter::once(("host".to_string(), "broker".to_string())).collect(),
                description: Some("updated".into()),
            },
        },
        ClusterCommand::ConnectorCreated {
            name: "c2".into(),
            connector: varpulis_cluster::ClusterConnector {
                name: "c2".into(),
                connector_type: "kafka".into(),
                params: Default::default(),
                description: None,
            },
        },
        ClusterCommand::ConnectorRemoved { name: "c2".into() },
        ClusterCommand::ScalingPolicySet {
            policy: Some(serde_json::json!({"min_workers": 2, "max_workers": 9})),
        },
        ClusterCommand::ModelRegistered {
            name: "mdl".into(),
            entry: serde_json::from_value(serde_json::json!({
                "name": "mdl",
                "s3_key": "models/mdl.onnx",
                "format": "onnx",
                "inputs": ["x"],
                "outputs": ["y"],
                "size_bytes": 1024,
                "uploaded_at": "2026-01-01T00:00:00Z"
            }))
            .expect("ModelRegistryEntry shape changed — update this fixture"),
        },
        ClusterCommand::ModelRegistered {
            name: "gone".into(),
            entry: serde_json::from_value(serde_json::json!({
                "name": "gone",
                "s3_key": "models/gone.onnx",
                "format": "onnx",
                "inputs": [],
                "outputs": [],
                "size_bytes": 1,
                "uploaded_at": "2026-01-01T00:00:00Z"
            }))
            .unwrap(),
        },
        ClusterCommand::ModelRemoved {
            name: "gone".into(),
        },
        ClusterCommand::DeregisterWorker { id: "w2".into() },
        #[cfg(feature = "distributed-checkpoint")]
        ClusterCommand::CheckpointCompleted {
            group_id: "g1".into(),
            checkpoint_id: 4,
        },
        #[cfg(feature = "distributed-checkpoint")]
        ClusterCommand::CheckpointCompleted {
            group_id: "g1".into(),
            checkpoint_id: 9,
        },
        // Stale replay: must not roll the recovery pointer backwards on
        // either backend.
        #[cfg(feature = "distributed-checkpoint")]
        ClusterCommand::CheckpointCompleted {
            group_id: "g1".into(),
            checkpoint_id: 2,
        },
        #[cfg(feature = "distributed-checkpoint")]
        ClusterCommand::CheckpointAborted {
            group_id: "g1".into(),
            checkpoint_id: 10,
            reason: "ack timeout".into(),
        },
    ];

    let mut raft_state = CoordinatorState::default();
    for cmd in &commands {
        apply_command(&mut raft_state, cmd.clone());
        let resp = applier
            .apply(cmd.clone())
            .await
            .unwrap_or_else(|e| panic!("{TEST}: KV apply failed for {cmd:?}: {e}"));
        assert!(
            matches!(resp, ClusterResponse::Ok),
            "{TEST}: KV backend refused {cmd:?}: {resp:?}"
        );
    }

    let snapshot = cp.snapshot().await.expect("snapshot");
    let kv_state = materialize(&snapshot);

    let a = serde_json::to_value(&raft_state).unwrap();
    let b = serde_json::to_value(&kv_state).unwrap();
    assert_eq!(
        a, b,
        "{TEST}: KV materialisation diverged from the Raft state machine"
    );

    // Sanity: the fixture actually exercised things.
    assert_eq!(kv_state.workers.len(), 1);
    assert_eq!(kv_state.workers["w1"].heartbeat_seq, 3);
    assert_eq!(kv_state.worker_pipeline_metrics["w1"].len(), 1);
    assert_eq!(kv_state.pipeline_groups.len(), 1);
    assert_eq!(kv_state.connectors.len(), 1);
    assert_eq!(kv_state.active_migrations.len(), 1);
    assert_eq!(kv_state.models.len(), 1);
    assert!(kv_state.scaling_policy.is_some());
}

/// Every key lands in the namespace the layout promises.
#[tokio::test]
async fn commands_write_the_documented_key_layout() {
    let Some(cp) = open("layout").await else {
        return;
    };
    let applier = Applier::new(cp.clone());

    applier
        .apply(ClusterCommand::RegisterWorker {
            id: "w1".into(),
            address: "a".into(),
            api_key: "k".into(),
            capacity: WorkerCapacity {
                cpu_cores: 1,
                pipelines_running: 0,
                max_pipelines: 1,
            },
        })
        .await
        .unwrap();
    applier
        .apply(ClusterCommand::GroupDeployed {
            name: "g1".into(),
            group: serde_json::json!({}),
        })
        .await
        .unwrap();
    applier
        .apply(ClusterCommand::MigrationStarted {
            task: serde_json::json!({"id": "m1"}),
        })
        .await
        .unwrap();
    applier
        .apply(ClusterCommand::ScalingPolicySet {
            policy: Some(serde_json::json!({})),
        })
        .await
        .unwrap();

    let snap = cp.snapshot().await.unwrap();
    let mut keys: Vec<_> = snap.entries.keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "groups/g1".to_string(),
            "migrations/m1".to_string(),
            "scaling/policy".to_string(),
            "workers/w1".to_string(),
        ]
    );
}

/// Connector params that look like credentials are refused, not relocated.
#[tokio::test]
async fn credential_bearing_connectors_are_refused_by_default() {
    let Some(cp) = open("connsecret").await else {
        return;
    };
    let secretive = varpulis_cluster::ClusterConnector {
        name: "kafka1".into(),
        connector_type: "kafka".into(),
        params: [
            ("bootstrap.servers".to_string(), "b:9092".to_string()),
            ("sasl.password".to_string(), "hunter2".to_string()),
        ]
        .into_iter()
        .collect(),
        description: None,
    };

    let refusing = Applier::new(cp.clone());
    let resp = refusing
        .apply(ClusterCommand::ConnectorCreated {
            name: "kafka1".into(),
            connector: secretive.clone(),
        })
        .await
        .unwrap();
    match resp {
        ClusterResponse::Error { message } => {
            assert!(
                message.contains("cleartext"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(
        cp.snapshot().await.unwrap().entries.is_empty(),
        "the refusal must not have written the secret anyway"
    );

    // The opt-in exists for an operator who has confirmed the bucket is
    // encrypted; it must actually work, or the flag is a lie.
    let allowing =
        Applier::new(cp.clone()).with_connector_policy(ConnectorSecretPolicy::AllowPlaintext);
    let resp = allowing
        .apply(ClusterCommand::ConnectorCreated {
            name: "kafka1".into(),
            connector: secretive,
        })
        .await
        .unwrap();
    assert!(matches!(resp, ClusterResponse::Ok));
    assert_eq!(cp.snapshot().await.unwrap().len(), 1);
}

// ===========================================================================
// 2. Fencing
// ===========================================================================

/// **The main prize.** A worker that is partitioned, declared dead, and then
/// returns must have its next write *refused*.
///
/// Side by side, on the same scenario:
/// * the Raft state machine accepts the zombie's `WorkerMetricsUpdated`;
/// * the KV control plane refuses the zombie's CAS.
#[tokio::test]
async fn a_returning_zombie_worker_is_refused_by_the_control_plane() {
    const TEST: &str = "a_returning_zombie_worker_is_refused_by_the_control_plane";
    let Some(cp) = open("zombie").await else {
        return;
    };

    // --- The zombie registers and runs happily. ---------------------------
    let mut zombie = WorkerLease::acquire(&cp, &worker("w1", &["p1"]), Duration::from_secs(30))
        .await
        .expect("initial acquire");
    let guard = zombie.guard();
    assert!(guard.is_valid(), "{TEST}: a fresh lease must be valid");

    let mut rec = worker("w1", &["p1"]);
    rec.entry.heartbeat_seq = 1;
    zombie.renew(&rec).await.expect("first heartbeat");
    let zombie_fence = zombie.fence();

    // --- It is partitioned; the coordinator declares it dead and fences it.
    let observed = cp
        .revision_of(&ControlKey::Worker("w1".into()))
        .await
        .unwrap()
        .unwrap();
    let fence_rev = fence_out(&cp, "w1", observed).await.expect("fence_out");
    assert!(
        fence_rev > zombie_fence,
        "{TEST}: fencing must advance the revision past the zombie's ({fence_rev} !> {zombie_fence})"
    );

    // --- It returns and tries to keep writing. ----------------------------
    rec.entry.heartbeat_seq = 2;
    rec.entry.events_processed = 999_999;
    let err = zombie
        .renew(&rec)
        .await
        .expect_err("the zombie's write MUST be refused");
    assert!(
        err.revokes_authority(),
        "{TEST}: expected an authority-revoking error, got {err}"
    );
    eprintln!("{TEST}: zombie write refused with: {err}");

    // The refusal is terminal: it cannot renew its way back to life.
    assert!(
        !guard.is_valid(),
        "{TEST}: a fenced worker's emit guard must be closed"
    );
    assert!(zombie.renew(&rec).await.is_err(), "{TEST}: still fenced");

    // And the bucket did not take the zombie's numbers.
    let stored = cp
        .get::<WorkerRecord>(&ControlKey::Worker("w1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.value.entry.events_processed, 0,
        "{TEST}: the zombie's counter leaked into the control plane"
    );
    assert_eq!(stored.value.entry.status, STATUS_FENCED);

    // --- Side by side: Raft's state machine ACCEPTS the same write. -------
    let mut raft_state = CoordinatorState::default();
    apply_command(
        &mut raft_state,
        ClusterCommand::RegisterWorker {
            id: "w1".into(),
            address: "a".into(),
            api_key: "k".into(),
            capacity: WorkerCapacity {
                cpu_cores: 4,
                pipelines_running: 1,
                max_pipelines: 16,
            },
        },
    );
    apply_command(
        &mut raft_state,
        ClusterCommand::WorkerStatusChanged {
            id: "w1".into(),
            status: "unhealthy".into(),
        },
    );
    apply_command(
        &mut raft_state,
        ClusterCommand::WorkerMetricsUpdated {
            id: "w1".into(),
            events_processed: 999_999,
            pipelines_running: 1,
            pipeline_metrics: vec![],
            heartbeat_seq: 2,
        },
    );
    assert_eq!(
        raft_state.workers["w1"].events_processed, 999_999,
        "{TEST}: this assertion documents the CURRENT Raft behaviour — the \
         zombie's write is applied unconditionally. If it ever fails, Raft \
         grew a fence and this comparison needs revisiting."
    );
}

/// A worker that cannot reach the broker at all must stop on its own clock.
///
/// This is the half that actually stops duplicate alerts: a partitioned
/// worker cannot be *told* it was fenced.
#[tokio::test]
async fn a_partitioned_worker_self_fences_without_hearing_anything() {
    const TEST: &str = "a_partitioned_worker_self_fences_without_hearing_anything";
    let Some(cp) = open("selffence").await else {
        return;
    };
    let ttl = Duration::from_millis(150);
    let lease = WorkerLease::acquire(&cp, &worker("w1", &["p1"]), ttl)
        .await
        .expect("acquire");
    let guard = lease.guard();
    assert!(guard.is_valid(), "{TEST}: valid right after acquiring");

    // Simulate the partition by simply not renewing.
    tokio::time::sleep(ttl + Duration::from_millis(80)).await;

    assert!(
        !guard.is_valid(),
        "{TEST}: an un-renewed lease must fail closed on its own clock"
    );
    assert!(
        !guard.is_fenced(),
        "{TEST}: expiry is not the same as having been fenced out"
    );
    eprintln!(
        "{TEST}: guard closed after {:?} without renewal (ttl {ttl:?})",
        guard.since_renewal()
    );
}

/// A command minted for the previous incarnation of a worker id is refused by
/// the new incarnation.
#[tokio::test]
async fn a_command_addressed_to_a_dead_incarnation_is_refused() {
    const TEST: &str = "a_command_addressed_to_a_dead_incarnation_is_refused";
    let Some(cp) = open("staleincarn").await else {
        return;
    };

    let first = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
        .await
        .expect("acquire #1");
    let old_fence = first.fence();
    let stale_cmd = FencedCommand::new(old_fence, serde_json::json!({"deploy": "p1"}));
    assert!(
        first.accept(&stale_cmd).is_ok(),
        "{TEST}: the incarnation the command was minted for must accept it"
    );

    // The worker dies and a replacement registers under the same id.
    fence_out(&cp, "w1", first.fence())
        .await
        .expect("fence_out");
    drop(first);
    let second = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
        .await
        .expect("acquire #2 (re-registering a fenced id is legitimate)");
    assert!(
        second.acquired_fence() > old_fence,
        "{TEST}: a new incarnation must acquire at a strictly higher revision"
    );

    let err = second
        .accept(&stale_cmd)
        .expect_err("{TEST}: a command for the dead incarnation must be refused");
    assert!(err.revokes_authority());
    eprintln!("{TEST}: stale command refused with: {err}");

    // A freshly-minted command is accepted.
    let fresh = FencedCommand::new(second.fence(), serde_json::json!({"deploy": "p1"}));
    assert!(second.accept(&fresh).is_ok());
}

/// Two live processes must not both own one worker id.
#[tokio::test]
async fn a_second_process_cannot_take_a_live_worker_id() {
    let Some(cp) = open("dupeid").await else {
        return;
    };
    let _held = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
        .await
        .expect("acquire");
    let err = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
        .await
        .expect_err("a live id must not be stealable");
    assert!(
        !err.revokes_authority(),
        "failing to acquire is not the same as losing authority: {err}"
    );
    eprintln!("second acquire refused with: {err}");
}

/// Two coordinators racing to fence the same worker: exactly one wins.
#[tokio::test]
async fn concurrent_fence_outs_do_not_double_fence_a_replacement() {
    let Some(cp) = open("racefence").await else {
        return;
    };
    let lease = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
        .await
        .unwrap();
    let observed = lease.fence();

    let (a, b) = tokio::join!(
        fence_out(&cp, "w1", observed),
        fence_out(&cp, "w1", observed)
    );
    let wins = [a.is_ok(), b.is_ok()].iter().filter(|x| **x).count();
    assert_eq!(
        wins, 1,
        "exactly one fence-out at a given observed revision must commit \
         (got a={a:?} b={b:?})"
    );
}

// ===========================================================================
// 3. Coordinator lease
// ===========================================================================

#[tokio::test]
async fn exactly_one_coordinator_holds_the_lease_and_a_stalled_one_stands_down() {
    const TEST: &str = "exactly_one_coordinator_holds_the_lease_and_a_stalled_one_stands_down";
    let Some(cp) = open("leaderlease").await else {
        return;
    };

    let mut c1 = LeaderLease::new(cp.clone(), "coord-1", Duration::from_secs(30));
    let mut c2 = LeaderLease::new(cp.clone(), "coord-2", Duration::from_secs(30));

    assert_eq!(c1.tick().await, LeaderState::Leader);
    assert_eq!(
        c2.tick().await,
        LeaderState::Follower,
        "{TEST}: two coordinators must not both lead"
    );
    assert!(c1.is_leader() && !c2.is_leader());
    assert_eq!(c1.current_holder().await, Some("coord-1".to_string()));

    // c1 renews happily.
    assert_eq!(c1.tick().await, LeaderState::Leader);

    // Now simulate c1 stalling while someone else takes the key: c2 cannot
    // create (occupied), so force the handover the way a TTL expiry would —
    // delete the key, then let c2 acquire. c1's next CAS is then stale.
    cp.delete(&ControlKey::Leader).await.unwrap();
    assert_eq!(c2.tick().await, LeaderState::Leader);

    assert_eq!(
        c1.tick().await,
        LeaderState::Follower,
        "{TEST}: a stalled leader whose CAS is refused must stand down, not keep driving"
    );
    assert!(!c1.is_leader());
    assert_eq!(c2.current_holder().await, Some("coord-2".to_string()));

    // Graceful resignation hands over immediately.
    c2.resign().await;
    assert_eq!(c1.tick().await, LeaderState::Leader);
}

/// The lease is only worth having if the coordinator consults it.
///
/// `Coordinator::ha_role` defaults to `Standalone`, and `Standalone::is_writer()`
/// is `true`. So before `update_control_plane_role` existed, a coordinator
/// built without the `raft` feature never changed role for the whole process
/// lifetime, and *every* coordinator in a JetStream deployment passed
/// `require_writer`: all of them swept health, drove failovers and reconciled
/// placements at once, against each other.
///
/// This is the gate for that. It asserts on `Coordinator`, not on `LeaderLease`,
/// because the lease was already correct — it was simply not called.
#[tokio::test]
async fn only_the_lease_holding_coordinator_is_a_writer() {
    const TEST: &str = "only_the_lease_holding_coordinator_is_a_writer";
    let Some(cp) = open("coordrole").await else {
        return;
    };
    cp.delete(&ControlKey::Leader).await.ok();

    let ttl = Duration::from_secs(30);
    let mut a = varpulis_cluster::Coordinator::new();
    let mut b = varpulis_cluster::Coordinator::new();

    // Both start as Standalone, which is a writer — the pre-fix state, and
    // the reason this test exists.
    assert!(a.require_writer().is_ok());
    assert!(b.require_writer().is_ok());

    a.leader_lease = Some(LeaderLease::new(cp.clone(), "coord-a", ttl));
    b.leader_lease = Some(LeaderLease::new(cp.clone(), "coord-b", ttl));

    a.update_control_plane_role().await;
    b.update_control_plane_role().await;

    let writers = [&a, &b]
        .iter()
        .filter(|c| c.require_writer().is_ok())
        .count();
    assert_eq!(
        writers, 1,
        "{TEST}: exactly one coordinator may write, got {writers}"
    );
    assert!(
        a.require_writer().is_ok(),
        "{TEST}: a acquired first, so a leads"
    );

    // The follower names the leader, so an API caller is told where to go
    // rather than just being refused.
    match b.require_writer() {
        Err(varpulis_cluster::ClusterError::NotLeader(who)) => {
            assert_eq!(who, "coord-a", "{TEST}: the follower must name the holder");
        }
        other => panic!("{TEST}: expected NotLeader, got {other:?}"),
    }

    // Graceful handover: the holder resigns on its way out, so the standby
    // takes over on its next sweep instead of waiting out the TTL.
    //
    // `b` is ticked first on purpose. A resigned lease holds nothing, so its
    // own next tick would `create` and win the empty key straight back — which
    // is right for a coordinator that resigned and kept running, and wrong as
    // a model of one that resigned because it is shutting down. Ticking the
    // standby first is the shutdown case, and it is the one the TTL and the
    // `resign()` call both exist for.
    if let Some(lease) = a.leader_lease.as_mut() {
        lease.resign().await;
    }
    b.update_control_plane_role().await;
    assert!(
        b.require_writer().is_ok(),
        "{TEST}: after the holder resigns, the standby must take over"
    );

    // And the resigned one demotes itself on its own next sweep rather than
    // continuing to believe it writes.
    a.update_control_plane_role().await;
    match a.require_writer() {
        Err(varpulis_cluster::ClusterError::NotLeader(who)) => {
            assert_eq!(
                who, "coord-b",
                "{TEST}: the ex-leader must name the new holder"
            );
        }
        other => panic!("{TEST}: the resigned coordinator must not still write, got {other:?}"),
    }
}

/// A follower must be able to say *where* the leader is, not just that it is
/// not itself.
///
/// Raft mode forwarded a follower's writes to the leader transparently, by
/// resolving a `NodeId` through the peer-address map built from
/// `--raft-peers`. The control plane has no such map, so the leader publishes
/// its own reachable URL in the lease record; without that a follower can only
/// refuse, which is correct but strictly worse than what Raft did.
#[tokio::test]
async fn a_follower_learns_where_to_forward_and_an_old_record_degrades_safely() {
    const TEST: &str = "a_follower_learns_where_to_forward_and_an_old_record_degrades_safely";
    let Some(cp) = open("forwardaddr").await else {
        return;
    };
    cp.delete(&ControlKey::Leader).await.ok();

    let ttl = Duration::from_secs(30);
    let mut leader =
        LeaderLease::new(cp.clone(), "coord-a", ttl).with_address("http://coord-a:8080");
    let follower = LeaderLease::new(cp.clone(), "coord-b", ttl);

    assert_eq!(leader.tick().await, LeaderState::Leader);
    assert_eq!(
        follower.current_leader_address().await,
        Some("http://coord-a:8080".to_string()),
        "{TEST}: the follower must read the holder's address out of the record"
    );

    // The same, through the coordinator, which is what `forward_to_leader` calls.
    let mut b = coordinator_on(&cp);
    b.leader_lease = Some(LeaderLease::new(cp.clone(), "coord-b", ttl));
    b.update_control_plane_role().await;
    assert!(
        b.require_writer().is_err(),
        "{TEST}: coord-b is a follower here"
    );
    assert_eq!(
        b.control_plane_leader_addr().await,
        Some("http://coord-a:8080".to_string()),
        "{TEST}: a follower coordinator must resolve the leader's address"
    );

    // A record minted by a build that did not publish one — a bucket outlives
    // a rolling upgrade — must read as "no address", so the caller refuses
    // rather than forwarding to an empty URL.
    let held = leader.revision().expect("leader holds a revision");
    cp.write(
        &ControlKey::Leader,
        &serde_json::json!({ "id": "coord-a", "term": 9 }),
        Expect::Revision(held),
    )
    .await
    .expect("overwriting with a legacy-shaped record must succeed");
    assert_eq!(
        b.control_plane_leader_addr().await,
        None,
        "{TEST}: an address-less record must not produce a forwarding target"
    );

    cp.delete(&ControlKey::Leader).await.ok();
}

/// Every coordinator, follower included, must serve state it read from the
/// bucket rather than only what it happened to see directly.
///
/// `sync_from_raft` did this for Raft on every sweep, on every node. The
/// control-plane build had no counterpart, so a coordinator that had just
/// started answered read-only API calls out of an empty map.
#[tokio::test]
async fn a_coordinator_materialises_cluster_state_from_the_bucket() {
    const TEST: &str = "a_coordinator_materialises_cluster_state_from_the_bucket";
    let Some(cp) = open("materialise").await else {
        return;
    };
    cp.delete(&ControlKey::Worker("w-elsewhere".into()))
        .await
        .ok();

    // A worker registered against some *other* coordinator.
    cp.write(
        &ControlKey::Worker("w-elsewhere".into()),
        &worker("w-elsewhere", &["p-a", "p-b"]),
        Expect::Absent,
    )
    .await
    .expect("seeding the worker must succeed");

    // This coordinator has never heard of it.
    let mut c = coordinator_on(&cp);
    assert!(
        c.workers.is_empty(),
        "{TEST}: a fresh coordinator starts knowing nothing"
    );

    // The bucket is replicated, so a snapshot taken immediately after the write
    // can legitimately not contain it yet. That is fine in production — the
    // sweep is level-triggered and picks it up next tick — but the test must
    // assert convergence, not immediacy, or it fails on timing rather than on
    // behaviour.
    let wid = varpulis_cluster::WorkerId("w-elsewhere".into());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        c.sync_from_control_plane().await;
        if c.workers.contains_key(&wid) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{TEST}: the worker never appeared after materialising"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let w = c
        .workers
        .get(&wid)
        .unwrap_or_else(|| panic!("{TEST}: the worker must appear after materialising"));
    assert_eq!(
        w.assigned_pipelines,
        vec!["p-a".to_string(), "p-b".to_string()],
        "{TEST}: and with the pipelines the bucket says it runs"
    );

    cp.delete(&ControlKey::Worker("w-elsewhere".into()))
        .await
        .ok();
}

/// A migration abandoned mid-flight must be finishable by another coordinator.
///
/// The old path ran the whole migration inside one HTTP request —
/// `plan_migrate_pipeline`, then `execute_migrate_plan` doing checkpoint,
/// deploy, restore, switch and clean-up over HTTP, then
/// `commit_migrate_pipeline` writing a `MigrationTask` **after** it finished.
/// The five intermediate phases were never materialised anywhere, so a
/// coordinator that died partway through left no trace: the pipeline could be
/// deployed on the target and still assigned on the source, and no surviving
/// coordinator knew a migration had been in flight at all.
///
/// This asserts the two halves of the fix together — the coordinator writes
/// the record before executing, and a *different* coordinator's reconciler
/// picks it up and terminates it.
#[tokio::test]
async fn a_migration_abandoned_by_a_dead_coordinator_is_finished_by_another() {
    const TEST: &str = "a_migration_abandoned_by_a_dead_coordinator_is_finished_by_another";
    let Some(cp) = open("abandoned").await else {
        return;
    };

    // The bucket outlives a failed run of this test, and the mint below is a
    // create-if-absent. Clear the keys first so a previous failure does not
    // turn into a different failure here.
    cp.delete(&ControlKey::Migration("mig-orphan".into()))
        .await
        .ok();
    cp.delete(&ControlKey::Worker("w-dying".into())).await.ok();

    // The source worker exists and runs the pipeline.
    cp.write(
        &ControlKey::Worker("w-dying".into()),
        &worker("w-dying", &["p-orphan"]),
        Expect::Absent,
    )
    .await
    .ok();

    // Coordinator A mints the record, then dies. A one-millisecond deadline
    // stands in for "it has been stuck far too long"; the phase machine reads
    // the same clock either way.
    let a = coordinator_on(&cp);
    let plan = migrate_plan("mig-orphan", "p-orphan", "g1", "w-dying", "w-target");
    a.record_migration_started(&plan, Duration::from_millis(1))
        .await
        .expect("minting the migration record must not fail");

    let minted = cp
        .get::<MigrationRecord>(&ControlKey::Migration("mig-orphan".into()))
        .await
        .unwrap()
        .expect("the record must exist before execution, not after it");
    assert_eq!(
        minted.value.phase,
        MigrationPhase::Checkpointing,
        "{TEST}: a migration is recorded from its first phase"
    );
    assert_eq!(minted.value.source, "w-dying");
    assert_eq!(minted.value.target, "w-target");
    drop(a); // coordinator A is gone; nothing will ever commit this migration

    // Coordinator B sweeps. It has never heard of this migration.
    let b = coordinator_on(&cp);
    let report = b
        .reconcile_migrations()
        .await
        .expect("a configured control plane must produce a tick report");
    assert!(
        report.examined >= 1,
        "{TEST}: the surviving coordinator must see the orphaned record, saw {}",
        report.examined
    );

    let after = cp
        .get::<MigrationRecord>(&ControlKey::Migration("mig-orphan".into()))
        .await
        .unwrap()
        .expect("record still present, now terminal");
    assert_eq!(
        after.value.phase,
        MigrationPhase::Failed,
        "{TEST}: past its deadline the migration must be failed, not left pinning the pipeline"
    );
    assert!(
        after.value.failure.is_some(),
        "{TEST}: a failed migration must say why"
    );

    cp.delete(&ControlKey::Migration("mig-orphan".into()))
        .await
        .ok();
    cp.delete(&ControlKey::Worker("w-dying".into())).await.ok();
}

// ===========================================================================
// 4. Reconciliation loop, end to end
// ===========================================================================

/// Drive a migration through all six phases with the real reconciler against
/// the real bucket, and assert the cut-over actually fences the source.
#[tokio::test]
async fn the_reconciler_drives_a_migration_and_fences_the_source_at_cutover() {
    const TEST: &str = "the_reconciler_drives_a_migration_and_fences_the_source_at_cutover";
    let Some(cp) = open("reconcile").await else {
        return;
    };
    let reconciler = Reconciler::new(cp.clone()).with_linger(Duration::from_millis(1));

    // World: source runs p1, target is idle, and a durable checkpoint exists.
    let src = WorkerLease::acquire(&cp, &worker("w-src", &["p1"]), Duration::from_secs(30))
        .await
        .unwrap();
    cp.write(
        &ControlKey::Worker("w-tgt".into()),
        &worker("w-tgt", &[]),
        Expect::Absent,
    )
    .await
    .unwrap();
    cp.write(
        &ControlKey::Checkpoint("g1".into()),
        &serde_json::json!({"latest_completed": 12}),
        Expect::Absent,
    )
    .await
    .unwrap();

    let now = now_ms();
    let mig = MigrationRecord {
        id: "m1".into(),
        pipeline: "p1".into(),
        group: "g1".into(),
        source: "w-src".into(),
        target: "w-tgt".into(),
        phase: MigrationPhase::Checkpointing,
        reason: "rebalance".into(),
        source_fence: src.fence(),
        restore_checkpoint: None,
        restored: false,
        deadline_ms: now + 60_000,
        finished_ms: None,
        failure: None,
    };
    cp.write(&ControlKey::Migration("m1".into()), &mig, Expect::Absent)
        .await
        .unwrap();

    let phase = |cp: ControlPlane| async move {
        cp.get::<MigrationRecord>(&ControlKey::Migration("m1".into()))
            .await
            .unwrap()
            .map(|v| v.value.phase)
    };

    // Tick 1: a durable checkpoint exists -> Deploying, restore point recorded.
    reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(phase(cp.clone()).await, Some(MigrationPhase::Deploying));
    let rec = cp
        .get::<MigrationRecord>(&ControlKey::Migration("m1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rec.value.restore_checkpoint,
        Some(12),
        "{TEST}: the restore point must be pinned from the checkpoints namespace"
    );

    // Tick 2: the target has not reported the pipeline -> wait, do not advance.
    let report = reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(
        report.waiting, 1,
        "{TEST}: must not advance without evidence"
    );
    assert_eq!(phase(cp.clone()).await, Some(MigrationPhase::Deploying));

    // The executor deploys; the target now reports p1.
    let tgt = cp
        .get::<WorkerRecord>(&ControlKey::Worker("w-tgt".into()))
        .await
        .unwrap()
        .unwrap();
    let mut tgt_rec = tgt.value;
    tgt_rec.entry.assigned_pipelines.push("p1".into());
    cp.write(
        &ControlKey::Worker("w-tgt".into()),
        &tgt_rec,
        Expect::Revision(tgt.revision),
    )
    .await
    .unwrap();

    // Tick 3: evidence present -> Restoring.
    reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(phase(cp.clone()).await, Some(MigrationPhase::Restoring));

    // Tick 4: no restore ack yet -> wait.
    assert_eq!(reconciler.tick(now_ms()).await.unwrap().waiting, 1);

    // The executor acks the restore (a CAS on the same single key).
    let cur = cp
        .get::<MigrationRecord>(&ControlKey::Migration("m1".into()))
        .await
        .unwrap()
        .unwrap();
    let mut r = cur.value;
    r.restored = true;
    cp.write(
        &ControlKey::Migration("m1".into()),
        &r,
        Expect::Revision(cur.revision),
    )
    .await
    .unwrap();

    // Tick 5: -> Switching.
    reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(phase(cp.clone()).await, Some(MigrationPhase::Switching));

    // Tick 6: the cut-over. The source is fenced; the record deliberately
    // does NOT advance in the same tick.
    let report = reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(
        report.effects, 1,
        "{TEST}: the cut-over must fence the source"
    );
    assert_eq!(phase(cp.clone()).await, Some(MigrationPhase::Switching));

    let fenced = cp
        .get::<WorkerRecord>(&ControlKey::Worker("w-src".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fenced.value.entry.status, STATUS_FENCED);

    // And the source, if it comes back, cannot write.
    let mut zombie = src;
    let err = zombie
        .renew(&worker("w-src", &["p1"]))
        .await
        .expect_err("{TEST}: the fenced source must not be able to write");
    eprintln!("{TEST}: source refused after cut-over with: {err}");

    // Tick 7: fence observable -> CleaningUp. Tick 8: source drained -> Completed.
    reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(phase(cp.clone()).await, Some(MigrationPhase::CleaningUp));
    reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(phase(cp.clone()).await, Some(MigrationPhase::Completed));

    // Tick 9: past the linger window -> retired.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let report = reconciler.tick(now_ms()).await.unwrap();
    assert_eq!(report.retired, 1);
    assert_eq!(phase(cp.clone()).await, None);
}

/// A tick that is simply skipped costs latency, not correctness: running the
/// reconciler N extra times against an unchanged world changes nothing.
#[tokio::test]
async fn reconciliation_is_idempotent_and_missing_a_tick_is_harmless() {
    let Some(cp) = open("idempotent").await else {
        return;
    };
    let reconciler = Reconciler::new(cp.clone());

    cp.write(
        &ControlKey::Worker("w-tgt".into()),
        &worker("w-tgt", &[]),
        Expect::Absent,
    )
    .await
    .unwrap();
    let now = now_ms();
    let mig = MigrationRecord {
        id: "m1".into(),
        pipeline: "p1".into(),
        group: "g1".into(),
        source: "w-src".into(),
        target: "w-tgt".into(),
        phase: MigrationPhase::Deploying,
        reason: "manual".into(),
        source_fence: 0,
        restore_checkpoint: Some(1),
        restored: false,
        deadline_ms: now + 60_000,
        finished_ms: None,
        failure: None,
    };
    cp.write(&ControlKey::Migration("m1".into()), &mig, Expect::Absent)
        .await
        .unwrap();

    let before = cp
        .get::<MigrationRecord>(&ControlKey::Migration("m1".into()))
        .await
        .unwrap()
        .unwrap();
    for _ in 0..5 {
        let r = reconciler.tick(now_ms()).await.unwrap();
        assert_eq!(r.advanced, 0);
        assert_eq!(r.waiting, 1);
    }
    let after = cp
        .get::<MigrationRecord>(&ControlKey::Migration("m1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before.revision, after.revision,
        "a waiting reconciler must not churn the record"
    );
}

/// Two coordinators reconciling the same migration: at most one CAS commits,
/// and the loser recomputes rather than clobbering.
#[tokio::test]
async fn concurrent_reconcilers_do_not_lose_an_update() {
    const TEST: &str = "concurrent_reconcilers_do_not_lose_an_update";
    let Some(cp) = open("racerecon").await else {
        return;
    };
    let a = Reconciler::new(cp.clone());
    let b = Reconciler::new(cp.clone());

    cp.write(
        &ControlKey::Worker("w-tgt".into()),
        &worker("w-tgt", &["p1"]),
        Expect::Absent,
    )
    .await
    .unwrap();
    let now = now_ms();
    let mig = MigrationRecord {
        id: "m1".into(),
        pipeline: "p1".into(),
        group: "g1".into(),
        source: "w-src".into(),
        target: "w-tgt".into(),
        phase: MigrationPhase::Deploying,
        reason: "manual".into(),
        source_fence: 0,
        restore_checkpoint: Some(1),
        restored: false,
        deadline_ms: now + 60_000,
        finished_ms: None,
        failure: None,
    };
    cp.write(&ControlKey::Migration("m1".into()), &mig, Expect::Absent)
        .await
        .unwrap();

    let t = now_ms();
    let (ra, rb) = tokio::join!(a.tick(t), b.tick(t));
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    let advanced = ra.advanced + rb.advanced;
    let contended = ra.contended + rb.contended;
    assert_eq!(
        advanced, 1,
        "{TEST}: exactly one of two racing reconcilers may commit (a={ra:?} b={rb:?})"
    );
    assert!(
        contended <= 1,
        "{TEST}: the loser must observe a refused CAS, not silently win too"
    );
    assert_eq!(
        cp.get::<MigrationRecord>(&ControlKey::Migration("m1".into()))
            .await
            .unwrap()
            .unwrap()
            .value
            .phase,
        MigrationPhase::Restoring
    );
}

/// A migration that cannot make progress must terminate rather than pin a
/// pipeline forever.
#[tokio::test]
async fn a_stuck_migration_fails_at_its_deadline() {
    let Some(cp) = open("deadline").await else {
        return;
    };
    let reconciler = Reconciler::new(cp.clone());
    let now = now_ms();
    let mig = MigrationRecord {
        id: "m1".into(),
        pipeline: "p1".into(),
        group: "g1".into(),
        source: "w-src".into(),
        target: "w-tgt".into(),
        phase: MigrationPhase::Restoring,
        reason: "failover".into(),
        source_fence: 0,
        restore_checkpoint: Some(1),
        restored: false,
        // Already past.
        deadline_ms: now.saturating_sub(1),
        finished_ms: None,
        failure: None,
    };
    cp.write(&ControlKey::Migration("m1".into()), &mig, Expect::Absent)
        .await
        .unwrap();

    reconciler.tick(now_ms()).await.unwrap();
    let rec = cp
        .get::<MigrationRecord>(&ControlKey::Migration("m1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rec.value.phase, MigrationPhase::Failed);
    assert!(rec.value.failure.unwrap().contains("deadline"));
}

// ===========================================================================
// 5. The unfenced (Raft-equivalent) path, for the fail-before comparison
// ===========================================================================

/// Documents, against the real broker, what the *absence* of fencing looks
/// like: an unconditional write — the Raft-log semantic — takes the zombie's
/// data. `force_put` exists only to make this contrast runnable.
#[tokio::test]
async fn without_a_cas_precondition_the_zombie_write_lands() {
    const TEST: &str = "without_a_cas_precondition_the_zombie_write_lands";
    let Some(cp) = open("unfenced").await else {
        return;
    };
    let lease = WorkerLease::acquire(&cp, &worker("w1", &["p1"]), Duration::from_secs(30))
        .await
        .unwrap();
    fence_out(&cp, "w1", lease.fence()).await.unwrap();

    // The zombie writes with no precondition, exactly as an unconditional
    // Raft apply would.
    let mut zombie_rec = worker("w1", &["p1"]);
    zombie_rec.entry.events_processed = 999_999;
    cp.force_put(&ControlKey::Worker("w1".into()), &zombie_rec)
        .await
        .expect("an unconditional write is accepted — that is the bug");

    let stored = cp
        .get::<WorkerRecord>(&ControlKey::Worker("w1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.value.entry.events_processed, 999_999,
        "{TEST}: without the CAS precondition the fenced worker's data lands"
    );
    assert_eq!(
        stored.value.entry.status, "ready",
        "{TEST}: and it even un-fences itself"
    );
}

/// A worker that hands its id back and a replacement that takes it must not be
/// able to share a fence: the tombstone a delete leaves behind is at a higher
/// sequence than the record it replaced, so the replacement always acquires
/// strictly above the previous incarnation. The whole fencing argument rests
/// on that, so it is asserted against the real server rather than assumed.
#[tokio::test]
async fn releasing_and_reacquiring_an_id_never_recycles_a_fence() {
    const TEST: &str = "releasing_and_reacquiring_an_id_never_recycles_a_fence";
    let Some(cp) = open("recycle").await else {
        return;
    };

    let mut fences = Vec::new();
    for round in 0..4 {
        let mut lease = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("{TEST}: acquire in round {round} failed: {e}"));
        fences.push(lease.acquired_fence());
        let mut rec = worker("w1", &[]);
        rec.entry.heartbeat_seq = round;
        lease.renew(&rec).await.expect("renew");
        fences.push(lease.fence());
        lease.release().await.expect("release");
    }

    for w in fences.windows(2) {
        assert!(
            w[1] > w[0],
            "{TEST}: fences must be strictly increasing across incarnations, got {fences:?}"
        );
    }
    eprintln!("{TEST}: fences across four incarnations: {fences:?}");
}

/// After a release, the released lease is dead: it cannot write again even
/// though the key is momentarily absent.
#[tokio::test]
async fn a_released_lease_cannot_write_again() {
    let Some(cp) = open("released").await else {
        return;
    };
    let first = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
        .await
        .unwrap();
    let held = first.fence();
    first.release().await.expect("release");

    // A replacement takes the id.
    let second = WorkerLease::acquire(&cp, &worker("w1", &[]), Duration::from_secs(30))
        .await
        .expect("replacement acquires after the handback");
    assert!(second.acquired_fence() > held);

    // The old holder's revision is now stale; a write guarded on it is refused.
    let err = cp
        .write(
            &ControlKey::Worker("w1".into()),
            &worker("w1", &["ghost"]),
            Expect::Revision(held),
        )
        .await
        .expect_err("a write at the released revision must be refused");
    assert!(err.is_cas_conflict(), "expected a CAS conflict, got {err}");
}

/// The fence must reach the data plane, not stop at the control plane.
///
/// Refusing a fenced worker's control-plane write closes nothing a user can
/// see: on its own the zombie keeps consuming its sources, mutating window and
/// pattern state, advancing offsets, and emitting duplicate alerts. This pins
/// the wiring that makes the engine share the guard's flag, so a lost lease
/// stops ingestion.
///
/// Fail-before: the engine keeps processing after the lease is fenced out.
#[tokio::test]
async fn losing_the_lease_stops_the_engine_not_just_the_control_plane() {
    let Some(cp) = open("fence_reaches_data_plane").await else {
        return;
    };

    let ttl = Duration::from_secs(30);
    let mut lease = WorkerLease::acquire(&cp, &worker("w-dp", &["p1"]), ttl)
        .await
        .expect("acquire");

    // An engine that shares the guard's flag.
    let program =
        varpulis_parser::parse("event Tick:\n    n: int\n\nstream A = Tick\n    .emit(n: n)\n")
            .expect("parse");
    let (tx, _rx) = tokio::sync::mpsc::channel::<varpulis_runtime::event::Event>(16);
    let mut engine = varpulis_runtime::engine::Engine::new(tx);
    engine.load(&program).expect("load");
    engine.use_fence_handle(lease.guard().fence_flag());

    let mut ev = varpulis_runtime::event::Event::new("Tick");
    ev.data.insert("n".into(), varpulis_core::Value::Int(1));
    engine
        .process_batch(vec![ev.clone()])
        .await
        .expect("healthy before the fence");

    // The coordinator fences this worker out — what happens at a migration
    // cut-over, or when a health sweep declares it dead. A competing acquire
    // would NOT do it: the control plane correctly refuses to hand a live
    // owner's id to a second process.
    let observed = lease.fence();
    fence_out(&cp, "w-dp", observed)
        .await
        .expect("the coordinator must be able to fence a worker out");

    // The worker learns it lost on its next renewal, and clearing the shared
    // flag is what reaches the data plane.
    let err = lease
        .renew(&worker("w-dp", &["p1"]))
        .await
        .expect_err("a fenced lease must fail to renew");
    eprintln!("[fence_reaches_data_plane] renewal refused with: {err}");

    assert!(
        engine.is_fenced(),
        "losing the lease must fence the engine, or the zombie keeps emitting"
    );
    assert!(
        engine.process_batch(vec![ev]).await.is_err(),
        "a fenced engine must refuse to process"
    );
    eprintln!("[fence_reaches_data_plane] engine refused after the lease was fenced out");
}
