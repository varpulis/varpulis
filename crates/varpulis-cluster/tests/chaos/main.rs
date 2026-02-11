//! Process-based chaos test harness for Varpulis cluster.
//!
//! Spawns REAL `varpulis` binary processes (coordinator + workers) and exercises
//! the cluster REST API. No mocking — every test hits real processes communicating
//! over TCP on localhost.

pub mod edge_cases;
pub mod functional;
pub mod perf;
pub mod sustained;

use serde_json::Value as Json;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

/// Starting port for test clusters. Each cluster consumes 1 (coordinator) + N (workers).
static NEXT_PORT: AtomicU16 = AtomicU16::new(19100);

const API_KEY: &str = "test-chaos-key";
const WORKER_REGISTER_TIMEOUT: Duration = Duration::from_secs(10);
const COORDINATOR_STARTUP_DELAY: Duration = Duration::from_millis(500);

/// A worker process managed by the test harness.
pub struct WorkerProcess {
    pub id: String,
    pub process: Child,
    pub port: u16,
}

/// A test cluster consisting of a coordinator and zero or more worker processes.
///
/// All processes are spawned from the real `varpulis` binary (no mocking).
pub struct ProcessCluster {
    coordinator: Child,
    pub workers: Vec<WorkerProcess>,
    coordinator_port: u16,
    pub api_key: String,
    pub http_client: reqwest::Client,
    /// Counter for generating unique worker IDs within this cluster.
    next_worker_idx: usize,
}

// ---------------------------------------------------------------------------
// Binary discovery
// ---------------------------------------------------------------------------

/// Locate the `varpulis` binary. Checks `VARPULIS_BIN` env, then
/// `target/release/varpulis`, then `target/debug/varpulis`.
fn find_binary() -> String {
    if let Ok(bin) = std::env::var("VARPULIS_BIN") {
        return bin;
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // Walk up from crates/varpulis-cluster to the workspace root.
    let workspace_root = std::path::Path::new(manifest_dir)
        .parent() // crates/
        .and_then(|p| p.parent()) // workspace root
        .expect("Failed to determine workspace root");

    let release = workspace_root.join("target/release/varpulis");
    if release.exists() {
        return release.to_string_lossy().into_owned();
    }

    let debug = workspace_root.join("target/debug/varpulis");
    if debug.exists() {
        return debug.to_string_lossy().into_owned();
    }

    panic!(
        "Could not find the `varpulis` binary. Build the project first (`cargo build`) \
         or set the VARPULIS_BIN environment variable."
    );
}

/// Allocate `count` unique, sequential ports for this test run.
fn allocate_ports(count: u16) -> u16 {
    NEXT_PORT.fetch_add(count, Ordering::SeqCst)
}

// ---------------------------------------------------------------------------
// ProcessCluster implementation
// ---------------------------------------------------------------------------

impl ProcessCluster {
    /// Spawn a coordinator and `num_workers` worker processes, then wait for
    /// every worker to register with the coordinator.
    ///
    /// Panics if the binary is not found or if workers fail to register within
    /// the timeout.
    pub async fn start(num_workers: usize) -> Self {
        let bin = find_binary();
        let port_count = 1 + num_workers as u16;
        let base_port = allocate_ports(port_count);
        let coordinator_port = base_port;

        // -- Spawn coordinator ------------------------------------------------
        let coordinator = Command::new(&bin)
            .args([
                "coordinator",
                "--port",
                &coordinator_port.to_string(),
                "--api-key",
                API_KEY,
                "--bind",
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to spawn coordinator process");

        tokio::time::sleep(COORDINATOR_STARTUP_DELAY).await;

        // -- Spawn workers ----------------------------------------------------
        let mut workers = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            let worker_port = base_port + 1 + i as u16;
            let worker_id = format!("chaos-w{}", i);

            let process = Command::new(&bin)
                .args([
                    "server",
                    "--port",
                    &worker_port.to_string(),
                    "--coordinator",
                    &format!("http://127.0.0.1:{}", coordinator_port),
                    "--worker-id",
                    &worker_id,
                    "--api-key",
                    API_KEY,
                    "--bind",
                    "127.0.0.1",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap_or_else(|e| panic!("Failed to spawn worker {}: {}", worker_id, e));

            workers.push(WorkerProcess {
                id: worker_id,
                process,
                port: worker_port,
            });
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");

        let cluster = Self {
            coordinator,
            workers,
            coordinator_port,
            api_key: API_KEY.to_string(),
            http_client,
            next_worker_idx: num_workers,
        };

        // -- Wait for all workers to register ---------------------------------
        if num_workers > 0 {
            cluster
                .wait_for_workers(num_workers, WORKER_REGISTER_TIMEOUT)
                .await;
        }

        cluster
    }

    // -----------------------------------------------------------------------
    // Worker lifecycle
    // -----------------------------------------------------------------------

    /// Kill a worker process by its worker ID.
    ///
    /// Sends SIGKILL (on Unix) and removes the worker from the local tracking
    /// list. The coordinator will eventually detect the worker as unhealthy.
    pub fn kill_worker(&mut self, worker_id: &str) {
        if let Some(pos) = self.workers.iter().position(|w| w.id == worker_id) {
            let mut wp = self.workers.remove(pos);
            let _ = wp.process.kill();
            let _ = wp.process.wait();
        } else {
            panic!("kill_worker: no worker with id '{}'", worker_id);
        }
    }

    /// Spawn a new worker, wait for it to register, and return its worker ID.
    pub async fn add_worker(&mut self) -> String {
        let bin = find_binary();
        let worker_port = allocate_ports(1);
        let worker_id = format!("chaos-w{}", self.next_worker_idx);
        self.next_worker_idx += 1;

        let process = Command::new(&bin)
            .args([
                "server",
                "--port",
                &worker_port.to_string(),
                "--coordinator",
                &format!("http://127.0.0.1:{}", self.coordinator_port),
                "--worker-id",
                &worker_id,
                "--api-key",
                &self.api_key,
                "--bind",
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to spawn worker {}: {}", worker_id, e));

        self.workers.push(WorkerProcess {
            id: worker_id.clone(),
            process,
            port: worker_port,
        });

        // Wait until the new worker appears in the coordinator's worker list.
        let expected_total = self.workers.len();
        self.wait_for_workers(expected_total, WORKER_REGISTER_TIMEOUT)
            .await;

        worker_id
    }

    // -----------------------------------------------------------------------
    // Coordinator REST API helpers
    // -----------------------------------------------------------------------

    /// Deploy a pipeline group. Returns the group ID.
    pub async fn deploy_group(&self, spec: Json) -> String {
        let resp = self
            .http_client
            .post(self.api_url("/pipeline-groups"))
            .header("x-api-key", &self.api_key)
            .json(&spec)
            .send()
            .await
            .expect("deploy_group: request failed");

        let status = resp.status();
        let body: Json = resp.json().await.expect("deploy_group: invalid JSON");
        assert!(
            status.is_success(),
            "deploy_group failed with {}: {}",
            status,
            body
        );

        body["id"]
            .as_str()
            .expect("deploy_group: missing 'id' in response")
            .to_string()
    }

    /// Inject a batch of events into a pipeline group.
    pub async fn inject_batch(&self, group_id: &str, events: Vec<Json>) -> Json {
        let resp = self
            .http_client
            .post(self.api_url(&format!("/pipeline-groups/{}/inject-batch", group_id)))
            .header("x-api-key", &self.api_key)
            .json(&serde_json::json!({ "events": events }))
            .send()
            .await
            .expect("inject_batch: request failed");

        let status = resp.status();
        let body: Json = resp.json().await.expect("inject_batch: invalid JSON");
        assert!(
            status.is_success(),
            "inject_batch failed with {}: {}",
            status,
            body
        );
        body
    }

    /// Inject a single event into a pipeline group. Returns the full JSON
    /// response body on success.
    pub async fn inject_event(&self, group_id: &str, event: Json) -> Json {
        let resp = self
            .http_client
            .post(self.api_url(&format!("/pipeline-groups/{}/inject", group_id)))
            .header("x-api-key", &self.api_key)
            .json(&event)
            .send()
            .await
            .expect("inject_event: request failed");

        let status = resp.status();
        let body: Json = resp.json().await.expect("inject_event: invalid JSON");
        assert!(
            status.is_success(),
            "inject_event failed with {}: {}",
            status,
            body
        );
        body
    }

    /// Try to inject a single event. Returns `Ok(body)` on 2xx, `Err(status)`
    /// otherwise. Does not panic on HTTP errors.
    pub async fn try_inject_event(&self, group_id: &str, event: Json) -> Result<Json, u16> {
        let resp = self
            .http_client
            .post(self.api_url(&format!("/pipeline-groups/{}/inject", group_id)))
            .header("x-api-key", &self.api_key)
            .json(&event)
            .send()
            .await
            .map_err(|_| 0u16)?;

        let status = resp.status().as_u16();
        let body: Json = resp.json().await.unwrap_or(Json::Null);
        if (200..300).contains(&status) {
            Ok(body)
        } else {
            Err(status)
        }
    }

    /// Get cluster topology.
    pub async fn get_topology(&self) -> Json {
        self.get("/topology").await
    }

    /// List workers from the coordinator.
    pub async fn list_workers(&self) -> Json {
        self.get("/workers").await
    }

    /// Get details of a specific pipeline group.
    pub async fn get_group(&self, group_id: &str) -> Json {
        self.get(&format!("/pipeline-groups/{}", group_id)).await
    }

    /// List active migrations.
    pub async fn list_migrations(&self) -> Json {
        self.get("/migrations").await
    }

    /// Drain a worker (move all its pipelines elsewhere).
    pub async fn drain_worker(&self, worker_id: &str) -> Json {
        let resp = self
            .http_client
            .post(self.api_url(&format!("/workers/{}/drain", worker_id)))
            .header("x-api-key", &self.api_key)
            .json(&serde_json::json!({ "timeout_secs": null }))
            .send()
            .await
            .expect("drain_worker: request failed");

        let status = resp.status();
        let body: Json = resp.json().await.expect("drain_worker: invalid JSON");
        assert!(
            status.is_success(),
            "drain_worker failed with {}: {}",
            status,
            body
        );
        body
    }

    /// Trigger a rebalance across the cluster.
    pub async fn rebalance(&self) -> Json {
        let resp = self
            .http_client
            .post(self.api_url("/rebalance"))
            .header("x-api-key", &self.api_key)
            .send()
            .await
            .expect("rebalance: request failed");

        let status = resp.status();
        let body: Json = resp.json().await.expect("rebalance: invalid JSON");
        assert!(
            status.is_success(),
            "rebalance failed with {}: {}",
            status,
            body
        );
        body
    }

    /// Manually migrate a pipeline to a specific worker.
    pub async fn manual_migrate(
        &self,
        group_id: &str,
        pipeline: &str,
        target_worker: &str,
    ) -> Json {
        let resp = self
            .http_client
            .post(self.api_url(&format!("/pipelines/{}/{}/migrate", group_id, pipeline)))
            .header("x-api-key", &self.api_key)
            .json(&serde_json::json!({ "target_worker_id": target_worker }))
            .send()
            .await
            .expect("manual_migrate: request failed");

        let status = resp.status();
        let body: Json = resp.json().await.expect("manual_migrate: invalid JSON");
        assert!(
            status.is_success(),
            "manual_migrate failed with {}: {}",
            status,
            body
        );
        body
    }

    /// Get cluster metrics.
    pub async fn get_metrics(&self) -> Json {
        self.get("/metrics").await
    }

    /// Delete (teardown) a pipeline group.
    pub async fn delete_group(&self, group_id: &str) -> Json {
        let resp = self
            .http_client
            .delete(self.api_url(&format!("/pipeline-groups/{}", group_id)))
            .header("x-api-key", &self.api_key)
            .send()
            .await
            .expect("delete_group: request failed");

        let status = resp.status();
        let body: Json = resp.json().await.expect("delete_group: invalid JSON");
        assert!(
            status.is_success(),
            "delete_group failed with {}: {}",
            status,
            body
        );
        body
    }

    // -----------------------------------------------------------------------
    // Polling / waiting helpers
    // -----------------------------------------------------------------------

    /// Poll `condition` with exponential backoff until it returns `true` or
    /// `timeout` elapses. Panics on timeout.
    pub async fn wait_for<F, Fut>(&self, condition: F, timeout: Duration)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = std::time::Instant::now();
        let mut interval = Duration::from_millis(50);
        let max_interval = Duration::from_millis(500);

        loop {
            if condition().await {
                return;
            }
            if start.elapsed() > timeout {
                panic!("wait_for: condition not met within {:?}", timeout);
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(max_interval);
        }
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    /// Gracefully stop all managed processes (workers first, then coordinator).
    pub async fn shutdown(mut self) {
        // Kill workers first.
        for wp in &mut self.workers {
            let _ = wp.process.kill();
        }
        for wp in &mut self.workers {
            let _ = wp.process.wait();
        }
        self.workers.clear();

        // Kill coordinator.
        let _ = self.coordinator.kill();
        let _ = self.coordinator.wait();
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Build the full URL for a coordinator API path.
    pub fn api_url(&self, path: &str) -> String {
        format!(
            "http://127.0.0.1:{}/api/v1/cluster{}",
            self.coordinator_port, path
        )
    }

    /// Simple authenticated GET request returning parsed JSON.
    async fn get(&self, path: &str) -> Json {
        let resp = self
            .http_client
            .get(self.api_url(path))
            .header("x-api-key", &self.api_key)
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {} failed: {}", path, e));

        let status = resp.status();
        let body: Json = resp
            .json()
            .await
            .unwrap_or_else(|e| panic!("GET {} invalid JSON: {}", path, e));
        assert!(
            status.is_success(),
            "GET {} returned {}: {}",
            path,
            status,
            body
        );
        body
    }

    /// Poll the coordinator until the worker count reaches `expected`.
    async fn wait_for_workers(&self, expected: usize, timeout: Duration) {
        let start = std::time::Instant::now();
        let mut interval = Duration::from_millis(100);
        let max_interval = Duration::from_millis(500);

        loop {
            // Tolerate connection failures while the coordinator is still starting.
            if let Ok(resp) = self
                .http_client
                .get(self.api_url("/workers"))
                .header("x-api-key", &self.api_key)
                .send()
                .await
            {
                if let Ok(body) = resp.json::<Json>().await {
                    if let Some(total) = body["total"].as_u64() {
                        if total as usize >= expected {
                            return;
                        }
                    }
                }
            }

            if start.elapsed() > timeout {
                panic!(
                    "Timed out waiting for {} workers to register (elapsed {:?})",
                    expected,
                    start.elapsed()
                );
            }

            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(max_interval);
        }
    }
}

// ---------------------------------------------------------------------------
// Drop safety net — kill all child processes even if a test panics
// ---------------------------------------------------------------------------

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        for wp in &mut self.workers {
            let _ = wp.process.kill();
            let _ = wp.process.wait();
        }
        let _ = self.coordinator.kill();
        let _ = self.coordinator.wait();
    }
}
