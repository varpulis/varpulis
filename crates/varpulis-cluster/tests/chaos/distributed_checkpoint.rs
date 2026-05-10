//! End-to-End Distributed Exactly-Once Test (Task 4.1 — Flink-Parity Phase 4).
//!
//! Verifies that the distributed checkpoint protocol + 2PC commit deliver
//! exactly-once semantics across worker failures. Each test run:
//!
//!   1. Pre-populates a Kafka *input* topic with `INPUT_EVENT_COUNT` events.
//!   2. Deploys a 3-worker pipeline group:
//!      `kafka-source -> .window(count, N) -> .aggregate(...) -> kafka-sink`
//!      with `exactly_once: true` on the sink.
//!   3. Kills a worker at a scheduled point in the run (10%, 20%, ... 100%
//!      through the expected processing window).
//!   4. Waits for the surviving workers + recovered pipeline to drain.
//!   5. Reads the *output* topic with `isolation.level=read_committed`.
//!   6. Asserts the visible count equals the **exact** expected number — no
//!      duplicates from re-emission, no gaps from lost windows.
//!
//! Repeated 10× with different kill timings to stress the protocol against a
//! range of failure points (early consume, mid-checkpoint, post-commit, etc.).
//!
//! ## Prerequisites
//!
//! - `varpulis` binary built with `kafka` + `nats-transport` +
//!   `distributed-checkpoint` features (set `VARPULIS_BIN` to point at it):
//!   ```sh
//!   cargo build --release \
//!     -p varpulis-cli \
//!     --features 'kafka nats-transport distributed-checkpoint raft'
//!   export VARPULIS_BIN=$PWD/target/release/varpulis
//!   ```
//! - Kafka broker on `localhost:9092` (or set `KAFKA_BOOTSTRAP`):
//!   ```sh
//!   docker compose -f tests/integration/docker-compose.kafka.yml \
//!     up -d zookeeper kafka kafka-setup
//!   ```
//! - NATS broker on `localhost:4222`:
//!   ```sh
//!   docker run -d --name varpulis-nats -p 4222:4222 nats:latest
//!   ```
//! - Kafka container name (default `varpulis-kafka`) reachable via `docker
//!   exec` for topic creation and seeding (override with `KAFKA_CONTAINER`).
//!
//! ## Running
//!
//! ```sh
//! cargo test --release \
//!     --test chaos \
//!     --features distributed-checkpoint \
//!     test_distributed_exactly_once \
//!     -- --ignored --nocapture
//! ```
//!
//! ## Skip behaviour
//!
//! Each iteration probes for required services and emits a `[skip]` log line
//! when any prerequisite is missing (so CI without infra is non-fatal). Tests
//! are `#[ignore]`d so they only run on explicit opt-in.

#![cfg(all(unix, feature = "distributed-checkpoint"))]
#![allow(clippy::too_many_lines)]

use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command as TokioCommand;
use tokio::time::sleep;

use super::ProcessCluster;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Total events injected into the input topic per iteration.
const INPUT_EVENT_COUNT: usize = 100_000;

/// Window size for the aggregate step. The output count per iteration is
/// `INPUT_EVENT_COUNT / WINDOW_SIZE`.
const WINDOW_SIZE: usize = 100;

/// Expected exact number of windowed-aggregate output events per iteration.
const EXPECTED_OUTPUT_COUNT: usize = INPUT_EVENT_COUNT / WINDOW_SIZE;

/// Number of chaos iterations.
const ITERATIONS: usize = 10;

/// Per-iteration soft timeout. A 100K-event drain through a 3-worker cluster
/// with one mid-run kill should fit comfortably; this guards against hangs.
const ITERATION_TIMEOUT: Duration = Duration::from_mins(3);

/// Polling cadence while waiting for the output topic to fill.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Number of workers in the chaos cluster.
const NUM_WORKERS: usize = 3;

// ---------------------------------------------------------------------------
// Environment helpers
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

/// `docker --version` — confirms docker CLI is on PATH.
async fn docker_available() -> bool {
    TokioCommand::new("docker")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success())
}

/// Probes a TCP port — used to confirm Kafka/NATS sockets are open before
/// running an iteration.
async fn tcp_reachable(addr: &str) -> bool {
    let target = addr.to_string();
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&target),
    )
    .await
    .is_ok_and(|r| r.is_ok())
}

/// Returns the first host:port pair from a comma-separated bootstrap string.
fn first_host_port(bootstrap: &str) -> &str {
    bootstrap.split(',').next().unwrap_or(bootstrap).trim()
}

/// Strip the `nats://` prefix to get a TCP target for `tcp_reachable`.
fn nats_tcp_target(url: &str) -> String {
    url.trim_start_matches("nats://")
        .trim_end_matches('/')
        .to_string()
}

// ---------------------------------------------------------------------------
// Kafka admin helpers (driven via `docker exec`)
// ---------------------------------------------------------------------------

/// Create a Kafka topic with the given name, partition count, and replication.
/// Idempotent — uses `--if-not-exists`.
async fn kafka_create_topic(topic: &str, partitions: u32, replication: u32) -> bool {
    let container = kafka_container();
    let bootstrap_intra = "kafka:29092"; // intra-container listener
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

/// Delete a Kafka topic. Best-effort — ignores failures so cleanup never
/// aborts a test run.
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

/// Pre-populate a topic with `count` JSON-line events using the in-container
/// `kafka-console-producer`. Each event has a sequential `seq` field so the
/// output's `max_seq` aggregate can verify completeness even if a window
/// boundary is misaligned.
async fn kafka_seed_input(topic: &str, count: usize) -> bool {
    let container = kafka_container();
    let bootstrap_intra = "kafka:29092";

    // Build the JSON-line corpus. One event per line.
    let mut payload = String::with_capacity(count * 64);
    for i in 0..count {
        payload.push_str(&format!(
            "{{\"event_type\":\"Tick\",\"sensor\":\"s1\",\"value\":{i},\"seq\":{i}}}\n"
        ));
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
        // Drop to close stdin so the producer exits.
    }

    child.wait().await.is_ok_and(|s| s.success())
}

/// Drain a topic with `isolation.level=read_committed` until either the
/// expected count is reached or `timeout` elapses. Returns the line count.
///
/// Uses `kafka-console-consumer --from-beginning --max-messages` for
/// determinism, with `--consumer-property isolation.level=read_committed`
/// so aborted-transaction events are filtered. The `cap` is set above
/// `expected` so duplicate emissions would be detected (count > expected).
async fn kafka_count_committed(topic: &str, expected: usize, timeout: Duration) -> Option<usize> {
    let container = kafka_container();
    let bootstrap_intra = "kafka:29092";

    // Allow a slack window on top of `expected` to detect duplicates.
    let cap = expected.saturating_mul(2).max(expected + 10);
    let timeout_ms = timeout.as_millis() as u64;

    let output = TokioCommand::new("docker")
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
        .ok()?;

    // `--max-messages` returns non-zero when the timeout fires before the cap;
    // we still want the partial count so we count whatever made it through.
    let stdout = String::from_utf8_lossy(&output.stdout);
    Some(stdout.lines().filter(|l| !l.trim().is_empty()).count())
}

/// Returns true when the input topic has at least `count` committed messages.
/// Used as a readiness gate so we don't kill workers before any work has
/// happened.
async fn input_topic_filled(topic: &str, count: usize) -> bool {
    kafka_count_committed(topic, count, Duration::from_secs(10))
        .await
        .is_some_and(|c| c >= count)
}

// ---------------------------------------------------------------------------
// Cluster wiring helpers
// ---------------------------------------------------------------------------

/// Register the kafka source connector on the coordinator.
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

/// Register a transactional kafka sink connector. The transactional_id is
/// derived per-iteration so concurrent test runs do not fence each other.
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

/// VPL source for the test pipeline. `KafkaIn` and `KafkaOut` are resolved
/// against the cluster connector registry on deploy.
fn pipeline_vpl(input_topic: &str, output_topic: &str) -> String {
    format!(
        r#"
event Tick:
    sensor: str
    value: int
    seq: int

stream Out = Tick
    .from(KafkaIn, topic: "{input_topic}")
    .window(count, {WINDOW_SIZE})
    .aggregate(count() as cnt, max(seq) as max_seq)
    .to(KafkaOut, topic: "{output_topic}")
"#,
    )
}

/// Look up the worker hosting a pipeline within a topology snapshot.
fn locate_pipeline_worker(
    topology: &serde_json::Value,
    group_id: &str,
    pipeline_name: &str,
) -> Option<String> {
    let groups = topology["groups"].as_array()?;
    for group in groups {
        if group["group_id"].as_str() == Some(group_id) {
            let pipelines = group["pipelines"].as_array()?;
            for p in pipelines {
                if p["name"].as_str() == Some(pipeline_name) {
                    return p["worker_id"].as_str().map(String::from);
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Skip-on-missing-infra check
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
// Single chaos iteration
// ---------------------------------------------------------------------------

/// Outcome of a single chaos iteration.
#[derive(Debug)]
struct IterationResult {
    /// Iteration index (0-based).
    index: usize,
    /// Output count read with `isolation.level=read_committed`.
    visible_count: usize,
    /// Whether the assertion passed.
    exactly_once_held: bool,
    /// Wall-clock elapsed.
    elapsed: Duration,
    /// Whether the chaos kill executed (false = pipeline ended before the
    /// scheduled kill point, which is still a valid run).
    kill_executed: bool,
}

/// Run a single chaos iteration with the given kill timing. `kill_fraction`
/// is the fraction of `EXPECTED_OUTPUT_COUNT` to wait for before killing the
/// hosting worker (e.g. 0.5 = kill at the halfway mark).
async fn run_iteration(index: usize, kill_fraction: f64) -> IterationResult {
    let started = Instant::now();
    let pid = std::process::id();
    let input_topic = format!("eo-chaos-in-{pid}-{index}");
    let output_topic = format!("eo-chaos-out-{pid}-{index}");
    let txn_id = format!("eo-chaos-txn-{pid}-{index}");
    let consumer_group = format!("eo-chaos-grp-{pid}-{index}");

    eprintln!(
        "  [iter {index}/{total}] kill_fraction={kill_fraction:.2}, input={input_topic}, output={output_topic}",
        total = ITERATIONS,
    );

    // ---- Topic prep ----------------------------------------------------
    assert!(
        kafka_create_topic(&input_topic, 3, 1).await,
        "[iter {index}] failed to create input topic"
    );
    assert!(
        kafka_create_topic(&output_topic, 3, 1).await,
        "[iter {index}] failed to create output topic"
    );

    // Pre-populate the input topic. We seed the full corpus *before*
    // deploying the pipeline so the source can be assumed available the
    // moment the pipeline starts.
    assert!(
        kafka_seed_input(&input_topic, INPUT_EVENT_COUNT).await,
        "[iter {index}] failed to seed input topic with {INPUT_EVENT_COUNT} events"
    );

    // Confirm the seeding actually landed before we move on.
    assert!(
        input_topic_filled(&input_topic, INPUT_EVENT_COUNT).await,
        "[iter {index}] input topic appears under-filled"
    );

    // ---- Cluster startup -----------------------------------------------
    let mut cluster = ProcessCluster::start(NUM_WORKERS).await;

    register_kafka_source_connector(&cluster, "KafkaIn", &consumer_group).await;
    register_kafka_sink_connector(&cluster, "KafkaOut", &txn_id).await;

    let pipeline_name = format!("eo-pipe-{index}");
    let group_spec = serde_json::json!({
        "name": format!("eo-chaos-{index}"),
        "pipelines": [{
            "name": pipeline_name.clone(),
            "source": pipeline_vpl(&input_topic, &output_topic),
        }],
        "routes": []
    });

    let group_id = cluster.deploy_group(group_spec).await;

    // ---- Wait for initial drain progress -------------------------------
    //
    // Kill threshold: `kill_fraction` × expected output count. Setting
    // `kill_fraction == 0.0` kills before any output, `1.0` waits until
    // everything is committed (effectively a no-op kill).
    let kill_target = ((EXPECTED_OUTPUT_COUNT as f64) * kill_fraction).round() as usize;
    let mut kill_executed = false;
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
                "  [iter {index}] drained {count}/{EXPECTED_OUTPUT_COUNT} (kill_target={kill_target})"
            );
            last_seen = count;
        }

        // Trigger the chaos kill once we cross the threshold (and only once).
        if !kill_executed && count >= kill_target {
            let topology = cluster.get_topology().await;
            if let Some(host_worker) = locate_pipeline_worker(&topology, &group_id, &pipeline_name)
            {
                eprintln!(
                    "  [iter {index}] killing worker {host_worker} at count={count} (target {kill_target})"
                );
                cluster.kill_worker(&host_worker);
                kill_executed = true;
            } else if count > 0 {
                // Pipeline is producing output but topology hasn't surfaced
                // its worker yet — extreme race; just skip the kill.
                eprintln!(
                    "  [iter {index}] kill threshold reached but no host worker visible — skipping kill"
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
    sleep(Duration::from_secs(3)).await;

    let visible_count = kafka_count_committed(
        &output_topic,
        EXPECTED_OUTPUT_COUNT,
        Duration::from_secs(15),
    )
    .await
    .unwrap_or(0);

    cluster.shutdown().await;

    // ---- Cleanup -------------------------------------------------------
    kafka_delete_topic(&input_topic).await;
    kafka_delete_topic(&output_topic).await;

    let exactly_once_held = visible_count == EXPECTED_OUTPUT_COUNT;
    let elapsed = started.elapsed();

    eprintln!(
        "  [iter {index}] DONE — visible={visible_count}, expected={EXPECTED_OUTPUT_COUNT}, \
         exactly_once={exactly_once_held}, elapsed={:.1}s, kill_executed={kill_executed}",
        elapsed.as_secs_f64()
    );

    IterationResult {
        index,
        visible_count,
        exactly_once_held,
        elapsed,
        kill_executed,
    }
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

/// Full 10-iteration distributed exactly-once chaos test.
///
/// Skips when prerequisites (Docker / Kafka / NATS) are unavailable so it can
/// stay in the chaos test harness without forcing infra in CI. Run with
/// `--ignored --nocapture` to opt in.
#[tokio::test]
#[ignore]
async fn test_distributed_exactly_once() {
    let infra = probe_infra().await;
    if !infra.available {
        eprintln!(
            "  [skip] test_distributed_exactly_once: {} (set up Docker + Kafka + NATS to run)",
            infra.reason.unwrap_or("unknown")
        );
        return;
    }

    let kill_fractions: [f64; ITERATIONS] =
        [0.05, 0.15, 0.25, 0.40, 0.55, 0.70, 0.80, 0.90, 0.95, 1.00];

    let mut results: Vec<IterationResult> = Vec::with_capacity(ITERATIONS);

    for (i, fraction) in kill_fractions.iter().enumerate() {
        let outcome = tokio::time::timeout(
            ITERATION_TIMEOUT + Duration::from_mins(1),
            run_iteration(i, *fraction),
        )
        .await
        .unwrap_or_else(|_| panic!("[iter {i}] timed out after {ITERATION_TIMEOUT:?}"));

        results.push(outcome);
    }

    // ---- Aggregate report ------------------------------------------------
    let passed = results.iter().filter(|r| r.exactly_once_held).count();
    let failed = ITERATIONS - passed;
    let total_elapsed: Duration = results.iter().map(|r| r.elapsed).sum();

    eprintln!(
        "\n  [summary] {passed}/{ITERATIONS} iterations exactly-once, total={:.1}s",
        total_elapsed.as_secs_f64()
    );
    for r in &results {
        eprintln!(
            "    iter {i}: visible={v}, expected={e}, exactly_once={ok}, kill={k}, {:.1}s",
            r.elapsed.as_secs_f64(),
            i = r.index,
            v = r.visible_count,
            e = EXPECTED_OUTPUT_COUNT,
            ok = r.exactly_once_held,
            k = r.kill_executed,
        );
    }

    assert_eq!(
        failed, 0,
        "{failed}/{ITERATIONS} iterations failed exactly-once: see iteration logs above"
    );
}

/// Smoke variant — runs a single iteration (no chaos) so the harness can be
/// validated without the full 10× soak.
#[tokio::test]
#[ignore]
async fn test_distributed_exactly_once_smoke() {
    let infra = probe_infra().await;
    if !infra.available {
        eprintln!(
            "  [skip] test_distributed_exactly_once_smoke: {} (set up Docker + Kafka + NATS to run)",
            infra.reason.unwrap_or("unknown")
        );
        return;
    }

    // 1.0 = wait until full drain before "killing" → effectively no kill.
    let result = run_iteration(0, 1.0).await;
    assert!(
        result.exactly_once_held,
        "smoke iteration failed: visible={}, expected={EXPECTED_OUTPUT_COUNT}",
        result.visible_count
    );
}
