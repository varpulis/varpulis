//! Connector Failure Mode Tests (Task 0.4 — Flink-Parity Phase 0)
//!
//! These integration tests verify that managed connectors **reconnect and
//! resume** after the broker becomes unreachable mid-flight. They simulate
//! broker outages by `docker pause`-ing the broker container, then `docker
//! unpause`-ing it, and assert that the connector picks up where it left off.
//!
//! ## Why pause, not stop?
//!
//! `docker pause` freezes the container's processes via the `freezer` cgroup.
//! Network sockets remain open from the kernel's view but no application code
//! runs — so the broker stops servicing requests without dropping TCP
//! connections. This is the closest single-host approximation to a transient
//! broker hang (network partition, GC pause, leader election) and lets
//! us exercise the connector-side reconnect path without rebuilding broker
//! state on each run.
//!
//! ## Per-broker semantics under outage
//!
//! - **Kafka** (at-least-once, default): librdkafka producer queues records
//!   locally and retries delivery once the broker reappears. Verified: zero
//!   data loss for events sent during the outage.
//! - **MQTT** at QoS=1 with persistent session: rumqttc retries publishes
//!   after reconnect; broker buffers undelivered messages for offline
//!   subscribers. Verified: subscriptions reattach and post-recovery events
//!   are received.
//! - **NATS** core: at-most-once. async-nats reconnects automatically but
//!   messages published during the outage are dropped. Verified: subscriptions
//!   reattach and post-recovery events are received.
//!
//! ## Running
//!
//! ```bash
//! # MQTT — uses the existing managed-connectors compose
//! docker compose -f tests/integration/docker-compose.managed-connectors.yml up -d mosquitto
//! cargo test -p varpulis-runtime --features mqtt \
//!     managed_connector_failure -- --ignored --nocapture
//!
//! # Kafka — uses the kafka compose
//! docker compose -f tests/integration/docker-compose.kafka.yml up -d zookeeper kafka kafka-setup
//! cargo test -p varpulis-runtime --features kafka \
//!     managed_connector_failure -- --ignored --nocapture
//!
//! # NATS — needs a containerized nats-server. Set NATS_CONTAINER to its name.
//! docker run -d --name varpulis-nats -p 4222:4222 nats:latest
//! NATS_CONTAINER=varpulis-nats cargo test -p varpulis-runtime --features nats \
//!     managed_connector_failure -- --ignored --nocapture
//! ```
//!
//! All tests skip cleanly when:
//! - `docker` is not on PATH, OR
//! - the target container is not running, OR
//! - the broker is not reachable on the expected port.
//!
//! ## Acceptance (Task 0.4)
//!
//! Each test asserts that, after the outage window:
//!   1. The connector re-establishes its broker connection automatically.
//!   2. Events sent post-recovery are received end-to-end.
//!   3. (Kafka only) No events sent during the outage are lost.

#![cfg(any(feature = "mqtt", feature = "nats", feature = "kafka"))]
#![cfg(unix)]

use std::time::Duration;

use tokio::process::Command;

// ---------------------------------------------------------------------------
// Shared docker control helpers
// ---------------------------------------------------------------------------

/// Quick PATH probe — `docker --version` should return 0 if docker is usable.
async fn docker_available() -> bool {
    Command::new("docker")
        .arg("--version")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Returns true when `docker inspect` reports the container as running. Used
/// to skip tests when the user hasn't started the broker yet.
async fn container_running(name: &str) -> bool {
    let Ok(output) = Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", name])
        .output()
        .await
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    String::from_utf8_lossy(&output.stdout).trim() == "true"
}

/// `docker pause <name>` — freeze all processes in the container via the
/// freezer cgroup. Returns Ok only on a successful pause.
async fn docker_pause(name: &str) -> Result<(), String> {
    let output = Command::new("docker")
        .args(["pause", name])
        .output()
        .await
        .map_err(|e| format!("docker pause spawn: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "docker pause {name} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

/// `docker unpause <name>` — counterpart to [`docker_pause`]. We try this in
/// test cleanup paths even on failure, so it's important that it never panics.
async fn docker_unpause(name: &str) -> Result<(), String> {
    let output = Command::new("docker")
        .args(["unpause", name])
        .output()
        .await
        .map_err(|e| format!("docker unpause spawn: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "docker unpause {name} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

/// RAII guard that unpauses the container on drop, so a panic mid-test
/// can't leave the broker frozen.
struct PausedContainer {
    name: String,
}

impl PausedContainer {
    async fn pause(name: &str) -> Result<Self, String> {
        docker_pause(name).await?;
        Ok(Self {
            name: name.to_string(),
        })
    }

    async fn unpause(self) -> Result<(), String> {
        let result = docker_unpause(&self.name).await;
        // Forget the guard so Drop doesn't double-unpause.
        std::mem::forget(self);
        result
    }
}

impl Drop for PausedContainer {
    fn drop(&mut self) {
        // Best-effort: spawn a blocking unpause if the test panicked. The
        // test will already be failing — we just don't want to leave the
        // broker frozen for the next test run.
        let name = self.name.clone();
        let _ = std::process::Command::new("docker")
            .args(["unpause", &name])
            .output();
    }
}

// ===========================================================================
// MQTT broker outage test
// ===========================================================================

#[cfg(feature = "mqtt")]
mod mqtt_outage {
    use std::sync::Arc;

    use rumqttc::{AsyncClient, MqttOptions, QoS};
    use rustc_hash::FxHashMap;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use varpulis_runtime::connector::{ConnectorConfig, ManagedConnectorRegistry};
    use varpulis_runtime::event::Event;
    use varpulis_runtime::sink::Sink;

    use super::*;

    fn mqtt_port() -> u16 {
        std::env::var("MQTT_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(11883)
    }

    fn mqtt_container() -> String {
        std::env::var("MQTT_CONTAINER").unwrap_or_else(|_| "varpulis-managed-mqtt".to_string())
    }

    /// Probe Mosquitto by opening a connection and polling once.
    async fn mqtt_is_available(port: u16) -> bool {
        let client_id = format!("failure-probe-{}", std::process::id());
        let mut opts = MqttOptions::new(&client_id, "localhost", port);
        opts.set_keep_alive(5);
        let (_client, mut eventloop) = AsyncClient::new(opts, 10);
        matches!(
            timeout(Duration::from_secs(3), eventloop.poll()).await,
            Ok(Ok(_))
        )
    }

    /// Publish a JSON event over QoS=1 from a fresh client. Returns true when
    /// the publish call succeeds — for QoS=1 the broker ack is not awaited
    /// here, but the event is at least handed to rumqttc's queue.
    async fn publish_qos1(port: u16, topic: &str, json: &serde_json::Value) -> Result<(), String> {
        let client_id = format!(
            "failure-pub-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        );
        let mut opts = MqttOptions::new(&client_id, "localhost", port);
        opts.set_keep_alive(5);
        let (client, mut eventloop) = AsyncClient::new(opts, 100);

        let handle = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
        tokio::time::sleep(Duration::from_millis(150)).await;

        let payload = serde_json::to_vec(json).map_err(|e| e.to_string())?;
        let res = client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await
            .map_err(|e| e.to_string());

        // Drain the ack
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = client.disconnect().await;
        handle.abort();
        res
    }

    /// Send an event through the managed sink — used to verify the sink
    /// continues to enqueue post-recovery.
    async fn sink_send(sink: &Arc<dyn Sink>, ev: &Event) -> Result<(), String> {
        sink.send(ev).await.map_err(|e| e.to_string())
    }

    /// Drain up to `max` batches from the source channel within `dur`.
    async fn drain(rx: &mut mpsc::Receiver<Vec<Event>>, max: usize, dur: Duration) -> Vec<Event> {
        let mut events = Vec::new();
        let deadline = tokio::time::Instant::now() + dur;
        while events.len() < max {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, rx.recv()).await {
                Ok(Some(batch)) => events.extend(batch),
                _ => break,
            }
        }
        events
    }

    /// Verify a managed MQTT source + sink survive a broker outage and
    /// continue to operate after the broker is unpaused.
    ///
    /// Steps:
    ///   1. Subscribe via the managed source on `e2e/failure/in`.
    ///   2. Publish baseline events; assert all are received.
    ///   3. `docker pause` the broker.
    ///   4. Sleep through the outage window.
    ///   5. `docker unpause` the broker; allow time for rumqttc reconnect
    ///      and re-subscription.
    ///   6. Publish post-recovery events; assert they all arrive.
    ///
    /// Acceptance (Task 0.4): the source's eventloop reconnects and the
    /// post-recovery events flow end-to-end without operator intervention.
    #[tokio::test]
    #[ignore]
    async fn test_mqtt_broker_outage_reconnects() {
        let container = mqtt_container();
        let port = mqtt_port();

        if !docker_available().await {
            eprintln!("Skipping: docker not available");
            return;
        }
        if !container_running(&container).await {
            eprintln!("Skipping: container '{container}' not running");
            return;
        }
        if !mqtt_is_available(port).await {
            eprintln!("Skipping: Mosquitto not reachable on port {port}");
            return;
        }

        // ---- Setup: managed source + sink at QoS=1 ----
        let mut props = FxHashMap::default();
        let config = ConnectorConfig::new("mqtt", "localhost")
            .with_property("port", &port.to_string())
            .with_property("client_id", "failure-mqtt-mgr")
            .with_property("qos", "1");
        props.insert("Broker".to_string(), config);

        let mut registry = ManagedConnectorRegistry::from_configs(&props).unwrap();

        let (tx, mut rx) = mpsc::channel(256);
        registry
            .start_source(
                "Broker",
                "e2e/failure/in",
                tx,
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();

        let mut sink_params = std::collections::HashMap::new();
        sink_params.insert("qos".to_string(), "1".to_string());
        let sink: Arc<dyn Sink> = registry
            .create_sink("Broker", "e2e/failure/out", &sink_params)
            .unwrap();

        // Let the subscription propagate before publishing.
        tokio::time::sleep(Duration::from_millis(800)).await;

        // ---- Phase 1: baseline traffic ----
        const PRE: usize = 5;
        for i in 0..PRE {
            publish_qos1(
                port,
                "e2e/failure/in",
                &serde_json::json!({"event_type":"Pre","seq":i}),
            )
            .await
            .expect("pre-outage publish");
        }
        let pre_events = drain(&mut rx, PRE, Duration::from_secs(10)).await;
        assert_eq!(
            pre_events.len(),
            PRE,
            "all baseline events must arrive (got {})",
            pre_events.len()
        );
        // Verify sink still works pre-outage so we know the connection is good.
        sink_send(&sink, &Event::new("PreOutage").with_field("phase", "pre"))
            .await
            .expect("pre-outage sink send");

        // ---- Phase 2: pause broker, hold the outage ----
        let guard = PausedContainer::pause(&container)
            .await
            .expect("pause broker");
        eprintln!("  [mqtt-failure] broker paused — holding outage 4s");
        tokio::time::sleep(Duration::from_secs(4)).await;

        // ---- Phase 3: unpause and let the eventloop reconnect ----
        guard.unpause().await.expect("unpause broker");
        eprintln!("  [mqtt-failure] broker unpaused — waiting for reconnect");
        // rumqttc's exponential backoff caps at ~30s; the connector's circuit
        // breaker uses 1<<N seconds. Give the loop ~10s to reconnect AND
        // re-subscribe (the SUBACK has to round-trip after CONNECT).
        tokio::time::sleep(Duration::from_secs(10)).await;

        // ---- Phase 4: post-recovery traffic ----
        const POST: usize = 5;
        let mut post_publish_failures = 0;
        for i in 0..POST {
            // Some publishes may fail on the first attempt right after the
            // unpause if the broker hasn't fully accepted the new client yet.
            // Retry up to 3 times before counting as a failure.
            let mut sent = false;
            for attempt in 0..3 {
                if publish_qos1(
                    port,
                    "e2e/failure/in",
                    &serde_json::json!({"event_type":"Post","seq":i,"attempt":attempt}),
                )
                .await
                .is_ok()
                {
                    sent = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            if !sent {
                post_publish_failures += 1;
            }
        }
        assert!(
            post_publish_failures == 0,
            "all post-recovery publishes must succeed; failed {}/{POST}",
            post_publish_failures
        );

        // The managed source should reconnect & resubscribe; verify the
        // post-recovery events flow through.
        let post_events = drain(&mut rx, POST, Duration::from_secs(15)).await;
        let post_count = post_events
            .iter()
            .filter(|e| e.event_type.as_ref() == "Post")
            .count();
        assert!(
            post_count >= POST,
            "post-recovery events must reach the source (expected ≥{POST}, got {post_count} of \
             {} total events)",
            post_events.len()
        );

        // The managed sink also re-uses its rumqttc client across the outage;
        // verify it didn't get stuck.
        sink_send(&sink, &Event::new("PostOutage").with_field("phase", "post"))
            .await
            .expect("post-recovery sink send");

        registry.shutdown().await;
        eprintln!(
            "  [mqtt-failure] PASS — pre={PRE}, post={post_count}, sink-send-after-outage=ok"
        );
    }
}

// ===========================================================================
// NATS reconnection test
// ===========================================================================

#[cfg(feature = "nats")]
mod nats_outage {
    use std::collections::HashMap;
    use std::time::Instant;

    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use uuid::Uuid;
    use varpulis_runtime::connector::{ManagedConnector, ManagedNatsConnector, NatsConfig};
    use varpulis_runtime::Event;

    use super::*;

    const NATS_URL: &str = "nats://localhost:4222";

    fn nats_container() -> Option<String> {
        std::env::var("NATS_CONTAINER").ok()
    }

    async fn nats_is_available() -> bool {
        async_nats::connect(NATS_URL).await.is_ok()
    }

    async fn raw_publish(subject: &str, payload: &str) -> Result<(), String> {
        let client = async_nats::connect(NATS_URL)
            .await
            .map_err(|e| e.to_string())?;
        client
            .publish(subject.to_string(), payload.to_string().into())
            .await
            .map_err(|e| e.to_string())?;
        client.flush().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Verify a managed NATS source survives a broker outage and continues
    /// receiving messages after the broker is unpaused.
    ///
    /// NATS Core is at-most-once: messages published while disconnected are
    /// dropped. So we verify (a) reconnection works and (b) post-recovery
    /// events flow — not zero-loss across the outage window.
    ///
    /// Steps:
    ///   1. Subscribe via the managed source.
    ///   2. Publish baseline events; assert all received.
    ///   3. `docker pause` the broker container.
    ///   4. Sleep, then `docker unpause`.
    ///   5. Wait for async-nats's internal reconnect (it has a built-in
    ///      reconnect loop with exponential backoff).
    ///   6. Publish post-recovery events; assert they arrive.
    #[tokio::test]
    #[ignore]
    async fn test_nats_reconnects_after_broker_restart() {
        let Some(container) = nats_container() else {
            eprintln!("Skipping: NATS_CONTAINER env var not set");
            return;
        };
        if !docker_available().await {
            eprintln!("Skipping: docker not available");
            return;
        }
        if !container_running(&container).await {
            eprintln!("Skipping: container '{container}' not running");
            return;
        }
        if !nats_is_available().await {
            eprintln!("Skipping: NATS not reachable on {NATS_URL}");
            return;
        }

        let subject = format!("test.failure.{}", Uuid::new_v4());
        let config = NatsConfig::new(NATS_URL, &subject);
        let mut managed = ManagedNatsConnector::new("failure-nats-mgr", config);

        let (tx, mut rx) = mpsc::channel(256);
        managed
            .start_source(&subject, tx, &HashMap::new())
            .await
            .unwrap();

        // Let the subscription propagate.
        tokio::time::sleep(Duration::from_millis(400)).await;

        // ---- Phase 1: baseline traffic ----
        const PRE: usize = 5;
        for i in 0..PRE {
            raw_publish(&subject, &format!(r#"{{"event_type":"Pre","seq":{i}}}"#))
                .await
                .expect("pre publish");
        }

        let mut pre_events: Vec<Event> = Vec::new();
        let pre_deadline = Instant::now() + Duration::from_secs(5);
        while pre_events.len() < PRE {
            let remaining = pre_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, rx.recv()).await {
                Ok(Some(batch)) => pre_events.extend(batch),
                _ => break,
            }
        }
        assert_eq!(
            pre_events.len(),
            PRE,
            "all baseline NATS events must arrive (got {})",
            pre_events.len()
        );

        // ---- Phase 2: outage window ----
        let guard = PausedContainer::pause(&container)
            .await
            .expect("pause nats");
        eprintln!("  [nats-failure] broker paused — holding outage 3s");
        tokio::time::sleep(Duration::from_secs(3)).await;
        guard.unpause().await.expect("unpause nats");
        eprintln!("  [nats-failure] broker unpaused — waiting for reconnect");

        // async-nats has an internal reconnect loop; give it ample time.
        tokio::time::sleep(Duration::from_secs(5)).await;

        // ---- Phase 3: post-recovery traffic ----
        // Drain anything stale that may still be in the channel from the pre
        // phase before we measure post events.
        while rx.try_recv().is_ok() {}

        const POST: usize = 5;
        let mut publish_failures = 0;
        for i in 0..POST {
            // Allow a couple of retries for the raw publisher to reconnect.
            let mut sent = false;
            for _ in 0..3 {
                if raw_publish(&subject, &format!(r#"{{"event_type":"Post","seq":{i}}}"#))
                    .await
                    .is_ok()
                {
                    sent = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if !sent {
                publish_failures += 1;
            }
        }
        assert_eq!(
            publish_failures, 0,
            "all post-recovery NATS publishes must succeed"
        );

        let mut post_events: Vec<Event> = Vec::new();
        let post_deadline = Instant::now() + Duration::from_secs(10);
        while post_events.len() < POST {
            let remaining = post_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, rx.recv()).await {
                Ok(Some(batch)) => post_events.extend(batch),
                _ => break,
            }
        }
        let post_count = post_events
            .iter()
            .filter(|e| e.event_type.as_ref() == "Post")
            .count();
        assert!(
            post_count >= POST,
            "managed NATS source must resume after reconnect (expected ≥{POST}, got {post_count})"
        );

        managed.shutdown().await.unwrap();
        eprintln!("  [nats-failure] PASS — pre={PRE}, post={post_count}");
    }
}

// ===========================================================================
// Kafka broker restart test (zero-loss)
// ===========================================================================

#[cfg(feature = "kafka")]
mod kafka_outage {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use varpulis_runtime::connector::{KafkaConfig, ManagedConnector, ManagedKafkaConnector};
    use varpulis_runtime::event::Event;
    use varpulis_runtime::Sink;

    use super::*;

    fn kafka_bootstrap() -> String {
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
    }

    fn kafka_container() -> String {
        std::env::var("KAFKA_CONTAINER").unwrap_or_else(|_| "varpulis-kafka".to_string())
    }

    async fn kafka_is_available() -> bool {
        let bs = kafka_bootstrap();
        timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(&bs))
            .await
            .is_ok_and(|r| r.is_ok())
    }

    /// Run one consumer that reads `expected` events with `auto.offset.reset
    /// = earliest`, returns them as Events. Used after the test to verify
    /// nothing was lost.
    async fn drain_topic(topic: &str, group: &str, expected: usize) -> Vec<Event> {
        let mut config = KafkaConfig::new(&kafka_bootstrap(), topic).with_group_id(group);
        config
            .properties
            .insert("auto_offset_reset".to_string(), "earliest".to_string());
        let mut connector = ManagedKafkaConnector::new("drain", config);
        let (tx, mut rx) = mpsc::channel::<Vec<Event>>(4096);
        let mut params = HashMap::new();
        params.insert("auto_offset_reset".to_string(), "earliest".to_string());
        connector
            .start_source(topic, tx, &params)
            .await
            .expect("drain start_source");

        let mut events = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        while let Ok(Some(batch)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            events.extend(batch);
            if events.len() >= expected {
                break;
            }
        }
        // Straggler window
        let extra = tokio::time::Instant::now() + Duration::from_secs(2);
        while let Ok(Some(batch)) = tokio::time::timeout_at(extra, rx.recv()).await {
            events.extend(batch);
        }
        connector.shutdown().await.ok();
        events
    }

    /// Verify a managed Kafka producer survives a broker outage **without
    /// data loss**. librdkafka queues records locally during the outage and
    /// retries delivery when the broker reappears, so a `read_committed`
    /// consumer should still see every event we sent.
    ///
    /// Steps:
    ///   1. Create a sink, send PRE events, flush.
    ///   2. `docker pause` Kafka.
    ///   3. Send MID events while the broker is paused — they accumulate in
    ///      the producer's local queue.
    ///   4. `docker unpause` Kafka.
    ///   5. Wait for librdkafka to drain the backlog and acknowledge.
    ///   6. Send POST events.
    ///   7. Final flush, then drain the topic and assert PRE+MID+POST events
    ///      are present with correct seqs.
    ///
    /// Acceptance (Task 0.4): zero events lost, regardless of whether they
    /// were sent before, during, or after the outage.
    #[tokio::test]
    #[ignore]
    async fn test_kafka_broker_restart_no_data_loss() {
        let container = kafka_container();
        if !docker_available().await {
            eprintln!("Skipping: docker not available");
            return;
        }
        if !container_running(&container).await {
            eprintln!("Skipping: container '{container}' not running");
            return;
        }
        if !kafka_is_available().await {
            eprintln!("Skipping: Kafka not reachable at {}", kafka_bootstrap());
            return;
        }

        let pid = std::process::id();
        let topic = format!("failure-kafka-{pid}");
        let run_id = format!("run-{pid}");

        // ---- Setup ----
        let config = KafkaConfig::new(&kafka_bootstrap(), &topic);
        let mut connector = ManagedKafkaConnector::new("failure-kafka-mgr", config);
        let sink: Arc<dyn Sink> = connector
            .create_sink(&topic, &HashMap::new())
            .expect("create_sink");

        // ---- Phase 1: baseline send + flush so PRE events are durable ----
        const PRE: usize = 20;
        for i in 0..PRE {
            sink.send(
                &Event::new("FailureK")
                    .with_field("run_id", run_id.as_str())
                    .with_field("seq", i as i64),
            )
            .await
            .expect("pre send");
        }
        sink.flush().await.expect("pre flush");

        // ---- Phase 2: pause Kafka ----
        let guard = PausedContainer::pause(&container)
            .await
            .expect("pause kafka");
        eprintln!("  [kafka-failure] broker paused — sending mid-outage events");

        // ---- Phase 3: send during outage (buffered locally) ----
        // librdkafka's send_result queues the record into the local producer
        // buffer; it should not error during a brief broker pause.
        const MID: usize = 20;
        for i in PRE..PRE + MID {
            // The shared sink uses send_result (fire-and-forget enqueue) so
            // these calls return immediately even with no broker. If the local
            // queue overflows we'd see SinkError; expect success at this size.
            sink.send(
                &Event::new("FailureK")
                    .with_field("run_id", run_id.as_str())
                    .with_field("seq", i as i64),
            )
            .await
            .expect("mid send (should enqueue locally)");
        }

        // Hold the outage for a few seconds so we exercise the broker-down
        // retry path inside librdkafka (broker.address.ttl, retry.backoff).
        tokio::time::sleep(Duration::from_secs(4)).await;

        // ---- Phase 4: unpause and wait for delivery to drain ----
        guard.unpause().await.expect("unpause kafka");
        eprintln!("  [kafka-failure] broker unpaused — waiting for retries to drain");
        // librdkafka resumes delivery on the metadata-refresh interval. Give
        // it a healthy window and then a flush to confirm.
        tokio::time::sleep(Duration::from_secs(6)).await;
        sink.flush().await.expect("post-outage flush");

        // ---- Phase 5: post-recovery send ----
        const POST: usize = 20;
        for i in PRE + MID..PRE + MID + POST {
            sink.send(
                &Event::new("FailureK")
                    .with_field("run_id", run_id.as_str())
                    .with_field("seq", i as i64),
            )
            .await
            .expect("post send");
        }
        sink.flush().await.expect("final flush");
        connector.shutdown().await.ok();

        // ---- Phase 6: verification — drain & assert no gaps ----
        let total = PRE + MID + POST;
        let received = drain_topic(&topic, &format!("failure-kafka-verify-{pid}"), total).await;
        let seqs: Vec<i64> = received
            .iter()
            .filter(|e| e.data.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str()))
            .filter_map(|e| e.data.get("seq").and_then(|v| v.as_int()))
            .collect();

        let unique: HashSet<i64> = seqs.iter().copied().collect();
        let missing: Vec<i64> = (0..total as i64).filter(|i| !unique.contains(i)).collect();

        assert!(
            missing.is_empty(),
            "Kafka producer must not lose events across a broker outage. \
             Missing {} of {total}: first 10 = {:?}",
            missing.len(),
            &missing[..missing.len().min(10)]
        );

        eprintln!(
            "  [kafka-failure] PASS — sent {total} (PRE={PRE} + MID={MID} + POST={POST}), \
             received {} unique, 0 missing",
            unique.len()
        );
    }
}
