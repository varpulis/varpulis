//! Multi-Replica Checkpoint/Restore Test (Task 4.2 — Flink-Parity Phase 4).
//!
//! Companion to [`distributed_checkpoint`] (Task 4.1). Where 4.1 verifies a
//! single-pipeline kill mid-checkpoint, this test verifies the same property
//! with a *partitioned* deployment:
//!
//!   * `replicas: 3`, `partition_key: "device_id"`,
//!   * 100 distinct `device_id`s, each emitting 100 sequential events,
//!   * `.partition_by(device_id).window(count, 10).aggregate(...)` so each
//!     replica maintains independent per-device windowing state,
//!   * Kafka input topic with `key=device_id` so each device sticks to one
//!     Kafka partition (and therefore to one replica via the consumer-group
//!     assignment).
//!
//! One of the three replica-hosting workers is killed mid-stream. Migration
//! must restore the dead replica's per-device window state on a surviving
//! worker. After the system stabilises and the output topic is drained with
//! `isolation.level=read_committed`, the test asserts:
//!
//!   1. Total committed output == `NUM_DEVICES * (EVENTS_PER_DEVICE/WINDOW)`
//!      → no duplicate windows from re-emission, no missing windows.
//!   2. For every `device_id`, the windows seen carry exactly the expected
//!      set of `max_seq` boundaries `{ WINDOW-1, 2*WINDOW-1, ... }` and each
//!      has `cnt == WINDOW`.
//!   3. No device is missing entirely (i.e. all 100 devices produced output).
//!
//! ## Prerequisites
//!
//! Same as [`distributed_checkpoint`] — `varpulis` built with
//! `kafka + nats-transport + distributed-checkpoint + raft`, Kafka on
//! `localhost:9092`, NATS on `localhost:4222`, Kafka container reachable via
//! `docker exec` for topic admin.
//!
//! ## Running
//!
//! ```sh
//! cargo test --release \
//!     --test chaos \
//!     --features distributed-checkpoint \
//!     test_multi_replica_checkpoint \
//!     -- --ignored --nocapture
//! ```
//!
//! ## Skip behaviour
//!
//! The test skips cleanly (printing a `[skip]` line) when Docker / Kafka /
//! NATS are unavailable, so it stays compileable in CI without infra.
//!
//! ## Known limitation
//!
//! Cluster-side wiring of the distributed-checkpoint orchestrator into the
//! tenant deploy path is still incomplete (see Task 4.1 module docs and the
//! Task 2.3 / 2.1 follow-ups). Without that, the test exercises the chaos
//! plumbing — kill, migration, drain — and surfaces gaps end-to-end. The
//! per-device exactness assertion is the property that goes green only once
//! the orchestrator and per-replica `transactional.id` isolation are wired.

#![cfg(all(unix, feature = "distributed-checkpoint"))]
#![allow(clippy::too_many_lines)]

use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command as TokioCommand;
use tokio::time::sleep;

use super::ProcessCluster;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Number of distinct `device_id`s in the input stream.
const NUM_DEVICES: usize = 100;

/// Sequential events generated per device.
const EVENTS_PER_DEVICE: usize = 100;

/// Total events injected per iteration.
const INPUT_EVENT_COUNT: usize = NUM_DEVICES * EVENTS_PER_DEVICE;

/// Window size for `.partition_by(device_id).window(count, ...)`.
const WINDOW_SIZE: usize = 10;

/// Per-device window count.
const WINDOWS_PER_DEVICE: usize = EVENTS_PER_DEVICE / WINDOW_SIZE;

/// Total expected output windows (per-device * device count).
const EXPECTED_OUTPUT_COUNT: usize = NUM_DEVICES * WINDOWS_PER_DEVICE;

/// Number of replicas the pipeline is deployed with.
const NUM_REPLICAS: usize = 3;

/// Number of workers in the chaos cluster — one per replica.
const NUM_WORKERS: usize = 3;

/// Number of Kafka partitions for the input topic. Picked > NUM_REPLICAS so
/// the consumer group can spread cleanly while each device still hashes to a
/// single stable partition.
const INPUT_PARTITIONS: u32 = 6;

/// Kill the replica-hosting worker once this fraction of expected output has
/// been committed (mid-stream chaos).
const KILL_FRACTION: f64 = 0.50;

/// Per-iteration soft timeout. A 10K-event drain through 3 replicas with one
/// mid-run kill should fit comfortably; this guards against hangs.
const ITERATION_TIMEOUT: Duration = Duration::from_mins(4);

/// Polling cadence while waiting for the output topic to fill.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Environment helpers (mirrored from distributed_checkpoint.rs)
// ---------------------------------------------------------------------------

fn kafka_bootstrap() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn kafka_container() -> String {
    std::env::var("KAFKA_CONTAINER").unwrap_or_else(|_| "varpulis-kafka".to_string())
}

fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

async fn docker_available() -> bool {
    TokioCommand::new("docker")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success())
}

async fn tcp_reachable(addr: &str) -> bool {
    let target = addr.to_string();
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&target),
    )
    .await
    .is_ok_and(|r| r.is_ok())
}

fn first_host_port(bootstrap: &str) -> &str {
    bootstrap.split(',').next().unwrap_or(bootstrap).trim()
}

fn nats_tcp_target(url: &str) -> String {
    url.trim_start_matches("nats://")
        .trim_end_matches('/')
        .to_string()
}

// ---------------------------------------------------------------------------
// Kafka admin helpers (driven via `docker exec`)
// ---------------------------------------------------------------------------

async fn kafka_create_topic(topic: &str, partitions: u32, replication: u32) -> bool {
    let container = kafka_container();
    let bootstrap_intra = "kafka:29092";
    TokioCommand::new("docker")
        .args([
            "exec",
            &container,
            "kafka-topics",
            "--bootstrap-server",
            bootstrap_intra,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            &partitions.to_string(),
            "--replication-factor",
            &replication.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success())
}

async fn kafka_delete_topic(topic: &str) {
    let container = kafka_container();
    let bootstrap_intra = "kafka:29092";
    let _ = TokioCommand::new("docker")
        .args([
            "exec",
            &container,
            "kafka-topics",
            "--bootstrap-server",
            bootstrap_intra,
            "--delete",
            "--topic",
            topic,
            "--if-exists",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

/// Pre-populate the input topic with `NUM_DEVICES * EVENTS_PER_DEVICE` events,
/// keyed by `device_id` so each device's events stick to one Kafka partition.
///
/// Events are interleaved across devices (round-robin) so a single replica's
/// per-device windows fill incrementally — that gives the chaos kill a
/// realistic chance of landing on partial per-device window state.
async fn kafka_seed_keyed_input(topic: &str) -> bool {
    let container = kafka_container();
    let bootstrap_intra = "kafka:29092";

    // Build the keyed corpus. Format: `device_id\tjson`. The `\t` separator is
    // the kafka-console-producer default (`key.separator` -> tab via
    // `parse.key=true` configuration below).
    let mut payload = String::with_capacity(INPUT_EVENT_COUNT * 80);
    for seq in 0..EVENTS_PER_DEVICE {
        for d in 0..NUM_DEVICES {
            let device_id = format!("device_{d:03}");
            payload.push_str(&format!(
                "{device_id}\t{{\"event_type\":\"Tick\",\"device_id\":\"{device_id}\",\"seq\":{seq},\"value\":{seq}}}\n",
            ));
        }
    }

    let mut child = match TokioCommand::new("docker")
        .args([
            "exec",
            "-i",
            &container,
            "kafka-console-producer",
            "--bootstrap-server",
            bootstrap_intra,
            "--topic",
            topic,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=\t",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        if stdin.write_all(payload.as_bytes()).await.is_err() {
            return false;
        }
        // Drop closes stdin so the producer exits.
    }

    child.wait().await.is_ok_and(|s| s.success())
}

/// Drain a topic with `isolation.level=read_committed`. Returns the raw JSON
/// payload of every committed message (one per `\n`).
async fn kafka_drain_committed(topic: &str, expected: usize, timeout: Duration) -> Vec<String> {
    let container = kafka_container();
    let bootstrap_intra = "kafka:29092";

    // Allow slack so duplicate emissions show up as count > expected.
    let cap = expected.saturating_mul(2).max(expected + 10);
    let timeout_ms = timeout.as_millis() as u64;

    let output = match TokioCommand::new("docker")
        .args([
            "exec",
            &container,
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap_intra,
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            &cap.to_string(),
            "--timeout-ms",
            &timeout_ms.to_string(),
            "--consumer-property",
            "isolation.level=read_committed",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
    {
        Ok(out) => out,
        Err(_) => return Vec::new(),
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Same as `kafka_drain_committed` but only returns the count, with a shorter
/// poll-friendly timeout. Used while waiting for the kill threshold.
async fn kafka_count_committed(topic: &str, expected: usize, timeout: Duration) -> Option<usize> {
    let drained = kafka_drain_committed(topic, expected, timeout).await;
    Some(drained.len())
}

/// Returns true when the input topic has at least `INPUT_EVENT_COUNT` keyed
/// messages. Used as a readiness gate.
async fn input_topic_filled(topic: &str) -> bool {
    kafka_count_committed(topic, INPUT_EVENT_COUNT, Duration::from_secs(15))
        .await
        .is_some_and(|c| c >= INPUT_EVENT_COUNT)
}

// ---------------------------------------------------------------------------
// Cluster wiring helpers
// ---------------------------------------------------------------------------

async fn register_kafka_source_connector(cluster: &ProcessCluster, name: &str, group_id: &str) {
    let body = serde_json::json!({
        "name": name,
        "connector_type": "kafka",
        "params": {
            "brokers": kafka_bootstrap(),
            "group_id": group_id,
            "auto_offset_reset": "earliest"
        }
    });

    let resp = cluster
        .http_client
        .post(cluster.api_url("/connectors"))
        .header("x-api-key", &cluster.api_key)
        .json(&body)
        .send()
        .await
        .expect("register source connector");

    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    assert!(
        status.is_success(),
        "register_kafka_source_connector failed: {status} {body}"
    );
}

async fn register_kafka_sink_connector(cluster: &ProcessCluster, name: &str, txn_id_base: &str) {
    let body = serde_json::json!({
        "name": name,
        "connector_type": "kafka",
        "params": {
            "brokers": kafka_bootstrap(),
            "group_id": format!("{txn_id_base}-sink-grp"),
            "exactly_once": "true",
            "transactional_id": txn_id_base
        }
    });

    let resp = cluster
        .http_client
        .post(cluster.api_url("/connectors"))
        .header("x-api-key", &cluster.api_key)
        .json(&body)
        .send()
        .await
        .expect("register sink connector");

    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    assert!(
        status.is_success(),
        "register_kafka_sink_connector failed: {status} {body}"
    );
}

/// VPL source for the multi-replica test. Per-device windowing requires
/// `.partition_by(device_id)` so each replica maintains independent state per
/// device it sees.
fn pipeline_vpl(input_topic: &str, output_topic: &str) -> String {
    format!(
        r#"
event Tick:
    device_id: str
    seq: int
    value: int

stream Out = Tick
    .from(KafkaIn, topic: "{input_topic}")
    .partition_by(device_id)
    .window(count, {WINDOW_SIZE})
    .aggregate(device_id: last(device_id), cnt: count(), max_seq: max(seq))
    .to(KafkaOut, topic: "{output_topic}")
"#,
    )
}

/// Look up the workers hosting any replica of the pipeline within a topology
/// snapshot. Replicas are deployed as `{name}#{idx}` so we match by prefix.
fn locate_replica_workers(
    topology: &serde_json::Value,
    group_id: &str,
    pipeline_name: &str,
) -> Vec<String> {
    let mut workers = Vec::new();
    let groups = match topology["groups"].as_array() {
        Some(g) => g,
        None => return workers,
    };
    for group in groups {
        if group["group_id"].as_str() != Some(group_id) {
            continue;
        }
        let pipelines = match group["pipelines"].as_array() {
            Some(p) => p,
            None => continue,
        };
        for p in pipelines {
            if let Some(name) = p["name"].as_str() {
                let is_replica =
                    name == pipeline_name || name.starts_with(&format!("{pipeline_name}#"));
                if is_replica {
                    if let Some(w) = p["worker_id"].as_str() {
                        workers.push(w.to_string());
                    }
                }
            }
        }
    }
    workers
}

// ---------------------------------------------------------------------------
// Per-device output verification
// ---------------------------------------------------------------------------

/// Aggregated per-device output from the sink topic.
#[derive(Debug, Default)]
struct PerDeviceSummary {
    /// `device_id` -> set of `max_seq` boundaries observed.
    boundaries: BTreeMap<String, BTreeSet<i64>>,
    /// `device_id` -> total `cnt` summed across all windows for that device.
    total_count: BTreeMap<String, u64>,
    /// Output rows that did not parse cleanly.
    parse_errors: usize,
    /// Output rows missing the `device_id` field.
    missing_device_id: usize,
}

impl PerDeviceSummary {
    /// Collect per-device summaries from the raw committed JSON lines.
    fn from_lines(lines: &[String]) -> Self {
        let mut s = Self::default();
        for line in lines {
            let parsed: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => {
                    s.parse_errors += 1;
                    continue;
                }
            };

            // Output JSON is the engine's emit shape: `{ "device_id": ..., "cnt": ..., "max_seq": ... }`
            // either at the top level or inside `data` / `fields` depending on the sink format.
            let device_id_v = parsed
                .get("device_id")
                .or_else(|| parsed.get("data").and_then(|d| d.get("device_id")))
                .or_else(|| parsed.get("fields").and_then(|f| f.get("device_id")));

            let cnt_v = parsed
                .get("cnt")
                .or_else(|| parsed.get("data").and_then(|d| d.get("cnt")))
                .or_else(|| parsed.get("fields").and_then(|f| f.get("cnt")));

            let max_seq_v = parsed
                .get("max_seq")
                .or_else(|| parsed.get("data").and_then(|d| d.get("max_seq")))
                .or_else(|| parsed.get("fields").and_then(|f| f.get("max_seq")));

            let device_id = match device_id_v.and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => {
                    s.missing_device_id += 1;
                    continue;
                }
            };
            let cnt = cnt_v.and_then(|v| v.as_u64()).unwrap_or(0);
            let max_seq = max_seq_v.and_then(|v| v.as_i64()).unwrap_or(-1);

            s.boundaries
                .entry(device_id.clone())
                .or_default()
                .insert(max_seq);
            *s.total_count.entry(device_id).or_default() += cnt;
        }
        s
    }
}

/// Expected per-device `max_seq` boundary set for a stream of
/// `EVENTS_PER_DEVICE` events grouped into `WINDOW_SIZE`-event windows
/// (in-order on a single replica): `{ WINDOW-1, 2*WINDOW-1, ... }`.
fn expected_boundaries() -> BTreeSet<i64> {
    (1..=WINDOWS_PER_DEVICE)
        .map(|w| (w * WINDOW_SIZE) as i64 - 1)
        .collect()
}

// ---------------------------------------------------------------------------
// Skip-on-missing-infra
// ---------------------------------------------------------------------------

struct InfraStatus {
    available: bool,
    reason: Option<&'static str>,
}

async fn probe_infra() -> InfraStatus {
    if !docker_available().await {
        return InfraStatus {
            available: false,
            reason: Some("docker CLI not on PATH"),
        };
    }
    if !tcp_reachable(first_host_port(&kafka_bootstrap())).await {
        return InfraStatus {
            available: false,
            reason: Some("Kafka broker not reachable"),
        };
    }
    if !tcp_reachable(&nats_tcp_target(&nats_url())).await {
        return InfraStatus {
            available: false,
            reason: Some("NATS broker not reachable"),
        };
    }
    InfraStatus {
        available: true,
        reason: None,
    }
}

// ---------------------------------------------------------------------------
// Single multi-replica chaos iteration
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct IterationOutcome {
    visible_count: usize,
    devices_with_output: usize,
    devices_with_correct_count: usize,
    devices_with_correct_boundaries: usize,
    elapsed: Duration,
    kill_executed: bool,
    kill_target_worker: Option<String>,
}

/// One multi-replica chaos run.
///
/// `kill_fraction == 1.0` is effectively no kill (waits for full drain before
/// the threshold trips). `kill_fraction < 1.0` triggers a SIGKILL of the
/// hosting worker once the output topic crosses the threshold.
async fn run_iteration(label: &str, kill_fraction: f64) -> IterationOutcome {
    let started = Instant::now();
    let pid = std::process::id();
    let input_topic = format!("mr-chaos-in-{pid}-{label}");
    let output_topic = format!("mr-chaos-out-{pid}-{label}");
    let txn_id = format!("mr-chaos-txn-{pid}-{label}");
    let consumer_group = format!("mr-chaos-grp-{pid}-{label}");

    eprintln!(
        "  [iter {label}] kill_fraction={kill_fraction:.2}, devices={NUM_DEVICES}, replicas={NUM_REPLICAS}, input={input_topic}, output={output_topic}"
    );

    // ---- Topic prep ----------------------------------------------------
    assert!(
        kafka_create_topic(&input_topic, INPUT_PARTITIONS, 1).await,
        "[iter {label}] failed to create input topic"
    );
    assert!(
        kafka_create_topic(&output_topic, INPUT_PARTITIONS, 1).await,
        "[iter {label}] failed to create output topic"
    );

    assert!(
        kafka_seed_keyed_input(&input_topic).await,
        "[iter {label}] failed to seed input topic with {INPUT_EVENT_COUNT} keyed events"
    );

    assert!(
        input_topic_filled(&input_topic).await,
        "[iter {label}] input topic appears under-filled"
    );

    // ---- Cluster startup -----------------------------------------------
    let mut cluster = ProcessCluster::start(NUM_WORKERS).await;

    register_kafka_source_connector(&cluster, "KafkaIn", &consumer_group).await;
    register_kafka_sink_connector(&cluster, "KafkaOut", &txn_id).await;

    let pipeline_name = format!("mr-pipe-{label}");
    let group_spec = serde_json::json!({
        "name": format!("mr-chaos-{label}"),
        "pipelines": [{
            "name": pipeline_name.clone(),
            "source": pipeline_vpl(&input_topic, &output_topic),
            "replicas": NUM_REPLICAS,
            "partition_key": "device_id",
        }],
        "routes": []
    });

    let group_id = cluster.deploy_group(group_spec).await;

    // ---- Wait for initial drain progress -------------------------------
    let kill_target = ((EXPECTED_OUTPUT_COUNT as f64) * kill_fraction).round() as usize;
    let mut kill_executed = false;
    let mut kill_target_worker: Option<String> = None;
    let drain_deadline = Instant::now() + ITERATION_TIMEOUT;
    let mut last_seen = 0usize;

    loop {
        if Instant::now() >= drain_deadline {
            break;
        }

        let count =
            kafka_count_committed(&output_topic, EXPECTED_OUTPUT_COUNT, Duration::from_secs(2))
                .await
                .unwrap_or(0);

        if count != last_seen {
            eprintln!(
                "  [iter {label}] drained {count}/{EXPECTED_OUTPUT_COUNT} (kill_target={kill_target})"
            );
            last_seen = count;
        }

        // Trigger the chaos kill once we cross the threshold (and only once).
        if !kill_executed && count >= kill_target {
            let topology = cluster.get_topology().await;
            let workers = locate_replica_workers(&topology, &group_id, &pipeline_name);
            if let Some(victim) = workers.into_iter().next() {
                eprintln!(
                    "  [iter {label}] killing worker {victim} at count={count} (target {kill_target})"
                );
                cluster.kill_worker(&victim);
                kill_target_worker = Some(victim);
                kill_executed = true;
            } else if count > 0 {
                eprintln!(
                    "  [iter {label}] kill threshold reached but no replica-hosting worker visible — skipping kill"
                );
                kill_executed = true;
            }
        }

        if count >= EXPECTED_OUTPUT_COUNT {
            break;
        }

        sleep(DRAIN_POLL_INTERVAL).await;
    }

    // Allow late-arriving commits a tail window after we believe we are done.
    sleep(Duration::from_secs(5)).await;

    let drained = kafka_drain_committed(
        &output_topic,
        EXPECTED_OUTPUT_COUNT,
        Duration::from_secs(20),
    )
    .await;
    let visible_count = drained.len();

    cluster.shutdown().await;

    // ---- Cleanup -------------------------------------------------------
    kafka_delete_topic(&input_topic).await;
    kafka_delete_topic(&output_topic).await;

    // ---- Per-device verification ---------------------------------------
    let summary = PerDeviceSummary::from_lines(&drained);
    let expected_set = expected_boundaries();

    let devices_with_output = summary.boundaries.len();
    let devices_with_correct_count = summary
        .total_count
        .iter()
        .filter(|(_, &cnt)| cnt as usize == EVENTS_PER_DEVICE)
        .count();
    let devices_with_correct_boundaries = summary
        .boundaries
        .iter()
        .filter(|(_, set)| *set == &expected_set)
        .count();

    let elapsed = started.elapsed();

    eprintln!(
        "  [iter {label}] DONE — visible={visible_count}/{EXPECTED_OUTPUT_COUNT}, devices={devices_with_output}/{NUM_DEVICES}, correct_count={devices_with_correct_count}, correct_boundaries={devices_with_correct_boundaries}, parse_errors={pe}, missing_dev={md}, elapsed={el:.1}s, kill={ke}",
        pe = summary.parse_errors,
        md = summary.missing_device_id,
        el = elapsed.as_secs_f64(),
        ke = kill_executed,
    );

    IterationOutcome {
        visible_count,
        devices_with_output,
        devices_with_correct_count,
        devices_with_correct_boundaries,
        elapsed,
        kill_executed,
        kill_target_worker,
    }
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

/// Multi-replica chaos test with a mid-stream kill.
///
/// Skips when prerequisites (Docker / Kafka / NATS) are unavailable so it can
/// stay in the chaos test harness without forcing infra in CI. Run with
/// `--ignored --nocapture` to opt in.
#[tokio::test]
#[ignore]
async fn test_multi_replica_checkpoint() {
    let infra = probe_infra().await;
    if !infra.available {
        eprintln!(
            "  [skip] test_multi_replica_checkpoint: {} (set up Docker + Kafka + NATS to run)",
            infra.reason.unwrap_or("unknown")
        );
        return;
    }

    let outcome = tokio::time::timeout(
        ITERATION_TIMEOUT + Duration::from_mins(2),
        run_iteration("kill", KILL_FRACTION),
    )
    .await
    .unwrap_or_else(|_| panic!("multi-replica chaos timed out after {ITERATION_TIMEOUT:?}"));

    eprintln!(
        "\n  [summary] visible={v}/{EXPECTED_OUTPUT_COUNT}, devices={d}/{NUM_DEVICES}, correct_count={cc}/{NUM_DEVICES}, correct_boundaries={cb}/{NUM_DEVICES}, kill={ke}, kill_target={kt:?}, elapsed={el:.1}s",
        v = outcome.visible_count,
        d = outcome.devices_with_output,
        cc = outcome.devices_with_correct_count,
        cb = outcome.devices_with_correct_boundaries,
        ke = outcome.kill_executed,
        kt = outcome.kill_target_worker,
        el = outcome.elapsed.as_secs_f64(),
    );

    // Assertion 1: total committed output equals expected (no duplicates, no
    // missing windows).
    assert_eq!(
        outcome.visible_count, EXPECTED_OUTPUT_COUNT,
        "visible output count != expected — see iteration log"
    );

    // Assertion 2: every device produced output.
    assert_eq!(
        outcome.devices_with_output,
        NUM_DEVICES,
        "{} of {NUM_DEVICES} devices missing from output",
        NUM_DEVICES - outcome.devices_with_output
    );

    // Assertion 3: every device's total event count == EVENTS_PER_DEVICE.
    assert_eq!(
        outcome.devices_with_correct_count, NUM_DEVICES,
        "{} of {NUM_DEVICES} devices have a wrong total count (windows*WINDOW_SIZE != EVENTS_PER_DEVICE)",
        NUM_DEVICES - outcome.devices_with_correct_count
    );

    // Assertion 4: every device's set of `max_seq` boundaries matches the
    // expected `{WINDOW-1, 2*WINDOW-1, ...}` set — proves no window was
    // duplicated, dropped, or emitted with a wrong boundary.
    assert_eq!(
        outcome.devices_with_correct_boundaries,
        NUM_DEVICES,
        "{} of {NUM_DEVICES} devices have wrong max_seq boundary set",
        NUM_DEVICES - outcome.devices_with_correct_boundaries
    );
}

/// Smoke variant — runs a single iteration with no chaos kill so the harness
/// can be validated without the full kill-and-migrate flow.
#[tokio::test]
#[ignore]
async fn test_multi_replica_checkpoint_smoke() {
    let infra = probe_infra().await;
    if !infra.available {
        eprintln!(
            "  [skip] test_multi_replica_checkpoint_smoke: {} (set up Docker + Kafka + NATS to run)",
            infra.reason.unwrap_or("unknown")
        );
        return;
    }

    // 1.0 = wait until full drain before "killing" → effectively no kill.
    let outcome = run_iteration("smoke", 1.0).await;
    assert_eq!(
        outcome.visible_count, EXPECTED_OUTPUT_COUNT,
        "smoke iteration: visible={}, expected={EXPECTED_OUTPUT_COUNT}",
        outcome.visible_count
    );
    assert_eq!(
        outcome.devices_with_output,
        NUM_DEVICES,
        "smoke iteration: {} of {NUM_DEVICES} devices missing from output",
        NUM_DEVICES - outcome.devices_with_output
    );
}

// ---------------------------------------------------------------------------
// Unit-level sanity checks (no infra required, run on every `cargo test`)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn expected_boundaries_match_window_arithmetic() {
        let set = expected_boundaries();
        assert_eq!(set.len(), WINDOWS_PER_DEVICE);
        // First boundary = WINDOW-1, last = EVENTS_PER_DEVICE-1.
        assert!(set.contains(&((WINDOW_SIZE as i64) - 1)));
        assert!(set.contains(&((EVENTS_PER_DEVICE as i64) - 1)));
    }

    #[test]
    fn per_device_summary_handles_top_level_shape() {
        let lines = vec![
            r#"{"device_id":"device_007","cnt":10,"max_seq":9}"#.to_string(),
            r#"{"device_id":"device_007","cnt":10,"max_seq":19}"#.to_string(),
            r#"{"device_id":"device_042","cnt":10,"max_seq":9}"#.to_string(),
        ];
        let s = PerDeviceSummary::from_lines(&lines);
        assert_eq!(s.parse_errors, 0);
        assert_eq!(s.missing_device_id, 0);
        assert_eq!(s.boundaries.get("device_007").unwrap().len(), 2);
        assert_eq!(*s.total_count.get("device_007").unwrap(), 20);
        assert_eq!(s.boundaries.get("device_042").unwrap().len(), 1);
    }

    #[test]
    fn per_device_summary_handles_nested_data_shape() {
        // Some sinks wrap fields under `data` (the engine's typed-event shape).
        let lines = vec![
            r#"{"event_type":"Out","data":{"device_id":"device_001","cnt":10,"max_seq":9}}"#
                .to_string(),
            r#"{"event_type":"Out","fields":{"device_id":"device_002","cnt":10,"max_seq":19}}"#
                .to_string(),
        ];
        let s = PerDeviceSummary::from_lines(&lines);
        assert_eq!(s.missing_device_id, 0);
        assert_eq!(s.parse_errors, 0);
        assert!(s.boundaries.contains_key("device_001"));
        assert!(s.boundaries.contains_key("device_002"));
    }

    #[test]
    fn per_device_summary_counts_parse_errors() {
        let lines = vec![
            "not-json".to_string(),
            r#"{"device_id":"d","cnt":1,"max_seq":0}"#.to_string(),
        ];
        let s = PerDeviceSummary::from_lines(&lines);
        assert_eq!(s.parse_errors, 1);
        assert_eq!(s.boundaries.len(), 1);
    }

    #[test]
    fn per_device_summary_flags_missing_device_id() {
        let lines = vec![r#"{"cnt":10,"max_seq":9}"#.to_string()];
        let s = PerDeviceSummary::from_lines(&lines);
        assert_eq!(s.missing_device_id, 1);
        assert!(s.boundaries.is_empty());
    }
}
