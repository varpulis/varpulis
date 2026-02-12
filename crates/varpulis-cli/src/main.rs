//! Varpulis CLI - Command line interface for Varpulis streaming analytics engine

use anyhow::Result;
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use warp::Filter;

use varpulis_core::ast::{Program, Stmt};
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::{EventFileParser, EventFilePlayer, StreamingEventReader};
use varpulis_runtime::metrics::{Metrics, MetricsServer};
use varpulis_runtime::simulator::{Simulator, SimulatorConfig};
use varpulis_runtime::SharedEvent; // PERF: Zero-copy event sharing

// Import our new modules
use varpulis_cli::api;
use varpulis_cli::auth::{self, AuthConfig};
use varpulis_cli::client::VarpulisClient;
use varpulis_cli::config::Config;
use varpulis_cli::rate_limit;
use varpulis_cli::security;
use varpulis_cli::websocket::{self, ServerState};

#[derive(Parser)]
#[command(name = "varpulis")]
#[command(author = "Varpulis Contributors")]
#[command(version = "0.1.0")]
#[command(about = "Varpulis - Modern streaming analytics engine", long_about = None)]
struct Cli {
    /// Path to configuration file (YAML or TOML)
    #[arg(short, long, global = true, env = "VARPULIS_CONFIG")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a VPL program
    Run {
        /// Path to the .vpl file
        #[arg(short, long)]
        file: Option<PathBuf>,

        /// Inline VPL code
        #[arg(short, long)]
        code: Option<String>,
    },

    /// Parse a VPL file and show the AST
    Parse {
        /// Path to the .vpl file
        file: PathBuf,
    },

    /// Run the HVAC demo
    Demo {
        /// Duration in seconds (default: 60)
        #[arg(short, long, default_value = "60")]
        duration: u64,

        /// Enable anomaly simulation
        #[arg(long)]
        anomalies: bool,

        /// Enable degradation simulation
        #[arg(long)]
        degradation: bool,

        /// Enable Prometheus metrics endpoint
        #[arg(long)]
        metrics: bool,

        /// Metrics endpoint port
        #[arg(long, default_value = "9090")]
        metrics_port: u16,
    },

    /// Check syntax of a VPL file
    Check {
        /// Path to the .vpl file
        file: PathBuf,
    },

    /// Start Varpulis server with WebSocket API
    Server {
        /// Server port
        #[arg(short, long, default_value = "9000")]
        port: u16,

        /// Enable Prometheus metrics
        #[arg(long)]
        metrics: bool,

        /// Metrics port
        #[arg(long, default_value = "9090")]
        metrics_port: u16,

        /// Bind address (default: 127.0.0.1 for security)
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// Working directory for file operations (default: current directory)
        #[arg(long)]
        workdir: Option<PathBuf>,

        /// API key for WebSocket authentication (optional, disables auth if not set)
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,

        /// Path to TLS certificate file (PEM format). Enables WSS when provided with --tls-key
        #[arg(long, env = "VARPULIS_TLS_CERT")]
        tls_cert: Option<PathBuf>,

        /// Path to TLS private key file (PEM format). Required when --tls-cert is provided
        #[arg(long, env = "VARPULIS_TLS_KEY")]
        tls_key: Option<PathBuf>,

        /// Rate limit in requests per second per client (0 = disabled)
        #[arg(long, env = "VARPULIS_RATE_LIMIT", default_value = "0")]
        rate_limit: u32,

        /// Directory for persistent state (enables state recovery on restart)
        #[arg(long, env = "VARPULIS_STATE_DIR")]
        state_dir: Option<PathBuf>,

        /// Coordinator URL to register with (e.g., http://localhost:9100)
        #[arg(long, env = "VARPULIS_COORDINATOR")]
        coordinator: Option<String>,

        /// Worker identifier (auto-generated if not set)
        #[arg(long, env = "VARPULIS_WORKER_ID")]
        worker_id: Option<String>,

        /// Address to advertise to the coordinator (e.g., http://worker-0:9000).
        /// Defaults to http://<bind>:<port>. Use this when the bind address (0.0.0.0)
        /// is not reachable from the coordinator (e.g., in Docker networks).
        #[arg(long, env = "VARPULIS_ADVERTISE_ADDRESS")]
        advertise_address: Option<String>,
    },

    /// Simulate events from an event file (.evt)
    Simulate {
        /// Path to the VPL program (.vpl)
        #[arg(short, long)]
        program: PathBuf,

        /// Path to the event file (.evt)
        #[arg(short, long)]
        events: PathBuf,

        /// Run without timing delays (immediate mode)
        #[arg(long)]
        immediate: bool,

        /// Verbose output (show each event)
        #[arg(short, long)]
        verbose: bool,

        /// Preload all events into memory (faster but uses more memory)
        #[arg(long)]
        preload: bool,

        /// Number of worker threads for parallel processing (default: number of CPU cores)
        #[arg(long, short = 'w')]
        workers: Option<usize>,

        /// Field to use for partitioning events (default: first string field)
        #[arg(long)]
        partition_by: Option<String>,

        /// Quiet/benchmark mode - only count outputs, don't collect them (faster)
        #[arg(long, short = 'q')]
        quiet: bool,

        /// Enable auto-checkpointing to this directory
        #[arg(long, env = "VARPULIS_CHECKPOINT_DIR")]
        checkpoint_dir: Option<PathBuf>,

        /// Checkpoint interval in seconds (default: 60)
        #[arg(long, default_value = "60")]
        checkpoint_interval: u64,
    },

    /// Generate example configuration file
    ConfigGen {
        /// Output format (yaml, toml)
        #[arg(short, long, default_value = "yaml")]
        format: String,

        /// Output file path (prints to stdout if not specified)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Deploy a pipeline to a remote Varpulis server
    Deploy {
        /// Server URL (e.g. http://localhost:9000)
        #[arg(long, env = "VARPULIS_SERVER")]
        server: String,

        /// Tenant API key
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: String,

        /// Path to the .vpl file
        #[arg(short, long)]
        file: PathBuf,

        /// Pipeline name
        #[arg(short, long)]
        name: String,
    },

    /// List pipelines on a remote Varpulis server
    Pipelines {
        /// Server URL (e.g. http://localhost:9000)
        #[arg(long, env = "VARPULIS_SERVER")]
        server: String,

        /// Tenant API key
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: String,
    },

    /// Delete a pipeline from a remote Varpulis server
    Undeploy {
        /// Server URL (e.g. http://localhost:9000)
        #[arg(long, env = "VARPULIS_SERVER")]
        server: String,

        /// Tenant API key
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: String,

        /// Pipeline ID to delete
        #[arg(long)]
        pipeline_id: String,
    },

    /// Show usage statistics from a remote Varpulis server
    Status {
        /// Server URL (e.g. http://localhost:9000)
        #[arg(long, env = "VARPULIS_SERVER")]
        server: String,

        /// Tenant API key
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: String,
    },

    /// Start cluster coordinator (control plane for distributed execution)
    Coordinator {
        /// Coordinator port
        #[arg(short, long, default_value = "9100")]
        port: u16,

        /// Bind address
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// API key for coordinator authentication
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,

        /// Heartbeat interval in seconds (workers send heartbeats this often)
        #[arg(long, default_value = "5", env = "VARPULIS_HEARTBEAT_INTERVAL")]
        heartbeat_interval: u64,

        /// Heartbeat timeout in seconds (mark worker unhealthy after this)
        #[arg(long, default_value = "15", env = "VARPULIS_HEARTBEAT_TIMEOUT")]
        heartbeat_timeout: u64,

        /// Minimum number of workers for auto-scaling (0 = disabled)
        #[arg(long, default_value = "0", env = "VARPULIS_SCALING_MIN_WORKERS")]
        scaling_min_workers: usize,

        /// Maximum number of workers for auto-scaling
        #[arg(long, default_value = "100", env = "VARPULIS_SCALING_MAX_WORKERS")]
        scaling_max_workers: usize,

        /// Scale-up threshold: avg pipelines per worker
        #[arg(long, default_value = "5.0", env = "VARPULIS_SCALING_UP_THRESHOLD")]
        scaling_up_threshold: f64,

        /// Scale-down threshold: avg pipelines per worker
        #[arg(long, default_value = "1.0", env = "VARPULIS_SCALING_DOWN_THRESHOLD")]
        scaling_down_threshold: f64,

        /// Webhook URL for scaling notifications
        #[arg(long, env = "VARPULIS_SCALING_WEBHOOK_URL")]
        scaling_webhook_url: Option<String>,

        /// Enable coordinator HA with K8s Lease-based leader election
        #[arg(long, env = "VARPULIS_HA_ENABLED")]
        ha: bool,

        /// Coordinator identity (defaults to POD_NAME env or hostname)
        #[arg(long, env = "POD_NAME")]
        coordinator_id: Option<String>,

        /// K8s namespace for pod watching (auto-detected in cluster)
        #[arg(long, env = "POD_NAMESPACE")]
        pod_namespace: Option<String>,

        /// K8s label selector for worker pods
        #[arg(
            long,
            default_value = "app.kubernetes.io/component=worker",
            env = "VARPULIS_WORKER_LABEL_SELECTOR"
        )]
        worker_label_selector: String,

        /// Enable Raft consensus cluster mode
        #[arg(long, env = "VARPULIS_RAFT")]
        raft: bool,

        /// This node's Raft ID (1, 2, 3, ...)
        #[arg(long, env = "VARPULIS_RAFT_NODE_ID")]
        raft_node_id: Option<u64>,

        /// Comma-separated peer addresses including self
        /// (e.g., "http://coord-1:9100,http://coord-2:9100,http://coord-3:9100")
        #[arg(long, env = "VARPULIS_RAFT_PEERS")]
        raft_peers: Option<String>,

        /// Directory for persistent Raft storage (RocksDB).
        /// When set with --raft, state survives coordinator restarts.
        #[arg(long, env = "VARPULIS_RAFT_DATA_DIR")]
        raft_data_dir: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Run { file, code } => {
            let (source, base_path) = if let Some(ref path) = file {
                (
                    std::fs::read_to_string(path)?,
                    path.parent().map(|p| p.to_path_buf()),
                )
            } else if let Some(code) = code {
                (code, None)
            } else {
                anyhow::bail!("Either --file or --code must be provided");
            };

            run_program(&source, base_path.as_ref()).await?;
        }

        Commands::Parse { file } => {
            let source = std::fs::read_to_string(&file)?;
            parse_and_show(&source)?;
        }

        Commands::Demo {
            duration,
            anomalies,
            degradation,
            metrics,
            metrics_port,
        } => {
            run_demo(duration, anomalies, degradation, metrics, metrics_port).await?;
        }

        Commands::Check { file } => {
            let source = std::fs::read_to_string(&file)?;
            check_syntax(&source)?;
        }

        Commands::Server {
            port,
            metrics,
            metrics_port,
            bind,
            workdir,
            api_key,
            tls_cert,
            tls_key,
            rate_limit,
            state_dir,
            coordinator,
            worker_id,
            advertise_address,
        } => {
            // Use security module to validate workdir - NO unwrap()!
            let workdir =
                security::validate_workdir(workdir).map_err(|e| anyhow::anyhow!("{}", e))?;

            // Create auth config from CLI argument or environment variable
            let auth_config = match api_key {
                Some(key) => AuthConfig::with_api_key(key),
                None => AuthConfig::disabled(),
            };

            // Create rate limit config
            let rate_limit_config = if rate_limit > 0 {
                rate_limit::RateLimitConfig::new(rate_limit)
            } else {
                rate_limit::RateLimitConfig::disabled()
            };

            // Validate TLS configuration
            let tls_config = match (tls_cert, tls_key) {
                (Some(cert), Some(key)) => Some((cert, key)),
                (None, None) => None,
                (Some(_), None) => {
                    anyhow::bail!("--tls-cert requires --tls-key to be specified");
                }
                (None, Some(_)) => {
                    anyhow::bail!("--tls-key requires --tls-cert to be specified");
                }
            };

            run_server(
                port,
                metrics,
                metrics_port,
                &bind,
                workdir,
                auth_config,
                tls_config,
                rate_limit_config,
                state_dir,
                coordinator,
                worker_id,
                advertise_address,
            )
            .await?;
        }

        Commands::Simulate {
            program,
            events,
            immediate,
            verbose,
            preload,
            workers,
            partition_by,
            quiet,
            checkpoint_dir,
            checkpoint_interval,
        } => {
            // Load config file if specified
            let config = if let Some(ref config_path) = cli.config {
                Some(Config::load(config_path).map_err(|e| anyhow::anyhow!("{}", e))?)
            } else {
                None
            };

            // Merge config file settings with CLI arguments
            let workers = workers.or(config.as_ref().and_then(|c| c.processing.workers));
            let partition_by = partition_by.or(config
                .as_ref()
                .and_then(|c| c.processing.partition_by.clone()));

            run_simulation(
                &program,
                &events,
                immediate,
                verbose,
                preload,
                workers,
                partition_by.as_deref(),
                quiet,
                checkpoint_dir,
                checkpoint_interval,
            )
            .await?;
        }

        Commands::ConfigGen { format, output } => {
            let content = match format.to_lowercase().as_str() {
                "yaml" | "yml" => Config::example_yaml(),
                "toml" => Config::example_toml(),
                _ => anyhow::bail!("Unsupported format: {}. Use 'yaml' or 'toml'", format),
            };

            if let Some(path) = output {
                std::fs::write(&path, &content)?;
                println!("Configuration written to: {}", path.display());
            } else {
                println!("{}", content);
            }
        }

        Commands::Deploy {
            server,
            api_key,
            file,
            name,
        } => {
            let source = std::fs::read_to_string(&file)?;
            let client = VarpulisClient::new(&server, &api_key);
            match client.deploy_pipeline(&name, &source).await {
                Ok(resp) => {
                    println!("Pipeline deployed successfully!");
                    println!("  ID:     {}", resp.id);
                    println!("  Name:   {}", resp.name);
                    println!("  Status: {}", resp.status);
                }
                Err(e) => {
                    anyhow::bail!("Deploy failed: {}", e);
                }
            }
        }

        Commands::Pipelines { server, api_key } => {
            let client = VarpulisClient::new(&server, &api_key);
            match client.list_pipelines().await {
                Ok(resp) => {
                    println!("Pipelines ({} total):", resp.total);
                    if resp.pipelines.is_empty() {
                        println!("  (none)");
                    }
                    for p in &resp.pipelines {
                        println!("  {} | {} | {}", p.id, p.name, p.status);
                    }
                }
                Err(e) => {
                    anyhow::bail!("Failed to list pipelines: {}", e);
                }
            }
        }

        Commands::Undeploy {
            server,
            api_key,
            pipeline_id,
        } => {
            let client = VarpulisClient::new(&server, &api_key);
            match client.delete_pipeline(&pipeline_id).await {
                Ok(()) => {
                    println!("Pipeline {} deleted.", pipeline_id);
                }
                Err(e) => {
                    anyhow::bail!("Undeploy failed: {}", e);
                }
            }
        }

        Commands::Status { server, api_key } => {
            let client = VarpulisClient::new(&server, &api_key);
            match client.get_usage().await {
                Ok(usage) => {
                    println!("Tenant: {}", usage.tenant_id);
                    println!("  Events processed:  {}", usage.events_processed);
                    println!("  Output events emitted: {}", usage.output_events_emitted);
                    println!("  Active pipelines:  {}", usage.active_pipelines);
                    println!("  Quota:");
                    println!("    Max pipelines:          {}", usage.quota.max_pipelines);
                    println!(
                        "    Max events/sec:         {}",
                        usage.quota.max_events_per_second
                    );
                    println!(
                        "    Max streams/pipeline:   {}",
                        usage.quota.max_streams_per_pipeline
                    );
                }
                Err(e) => {
                    anyhow::bail!("Failed to get status: {}", e);
                }
            }
        }

        Commands::Coordinator {
            port,
            bind,
            api_key,
            heartbeat_interval,
            heartbeat_timeout,
            scaling_min_workers,
            scaling_max_workers,
            scaling_up_threshold,
            scaling_down_threshold,
            scaling_webhook_url,
            ha,
            coordinator_id,
            pod_namespace,
            worker_label_selector,
            raft,
            raft_node_id,
            raft_peers,
            raft_data_dir,
        } => {
            let scaling_policy = if scaling_min_workers > 0 {
                Some(varpulis_cluster::ScalingPolicy {
                    min_workers: scaling_min_workers,
                    max_workers: scaling_max_workers,
                    scale_up_threshold: scaling_up_threshold,
                    scale_down_threshold: scaling_down_threshold,
                    cooldown_secs: 60,
                    webhook_url: scaling_webhook_url,
                })
            } else {
                None
            };
            run_coordinator(
                port,
                &bind,
                api_key,
                heartbeat_interval,
                heartbeat_timeout,
                scaling_policy,
                ha,
                coordinator_id,
                pod_namespace,
                worker_label_selector,
                raft,
                raft_node_id,
                raft_peers,
                raft_data_dir,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_program(source: &str, base_path: Option<&PathBuf>) -> Result<()> {
    use varpulis_runtime::connector::ManagedConnectorRegistry;
    use varpulis_runtime::ContextOrchestrator;

    info!("Parsing VPL program...");
    let mut program = parse(source).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    info!("Parsed {} statements", program.statements.len());

    // Resolve imports
    resolve_imports(&mut program, base_path)?;

    // Create output event channel — clone tx before giving it to the engine
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(10_000);
    let output_tx_for_ctx = output_tx.clone();

    // Create engine
    let mut engine = Engine::new(output_tx);
    engine
        .load_with_source(source, &program)
        .map_err(|e| anyhow::anyhow!("Load error:\n{}", e))?;

    let metrics = engine.metrics();
    println!("\nProgram loaded successfully!");
    println!("   Streams registered: {}", metrics.streams_count);

    // Build context orchestrator if the program declares contexts
    let has_contexts = engine.has_contexts();
    let mut orchestrator = if has_contexts {
        match ContextOrchestrator::build(engine.context_map(), &program, output_tx_for_ctx, 1000) {
            Ok(orch) => {
                println!(
                    "   Contexts: {} ({:?})",
                    orch.context_names().len(),
                    orch.context_names()
                );
                Some(orch)
            }
            Err(e) => anyhow::bail!("Failed to build context orchestrator: {}", e),
        }
    } else {
        None
    };

    // Check for deprecated config mqtt block
    if engine.get_config("mqtt").is_some() {
        eprintln!(
            "WARNING: 'config mqtt {{ ... }}' is deprecated. \
             Use the connector syntax instead:\n\
             \n\
             connector MyMqtt = mqtt (\n\
                 host: \"localhost\",\n\
                 port: 1883\n\
             )\n\
             \n\
             stream Events = SensorReading\n\
                 .from(MyMqtt, topic: \"sensors/#\")\n"
        );
    }

    let bindings = engine.source_bindings().to_vec();

    if !bindings.is_empty() {
        // Create shared event channel for all source connectors
        let (event_tx, mut event_rx) = mpsc::channel::<Event>(10_000);

        // Build managed connector registry — one connection per connector
        let mut registry = ManagedConnectorRegistry::from_configs(engine.connector_configs())
            .map_err(|e| anyhow::anyhow!("Registry build error: {}", e))?;

        // Start sources (connector-type-agnostic)
        for binding in &bindings {
            let config = engine.get_connector(&binding.connector_name).cloned();
            let Some(config) = config else {
                anyhow::bail!(
                    "Connector '{}' referenced in .from() but not declared",
                    binding.connector_name
                );
            };

            let topic = binding
                .topic_override
                .as_deref()
                .or(config.topic.as_deref())
                .unwrap_or("varpulis/events/#");

            println!(
                "\n{} source: {} topic={}",
                config.connector_type, binding.connector_name, topic
            );

            registry
                .start_source(
                    &binding.connector_name,
                    topic,
                    event_tx.clone(),
                    &binding.extra_params,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Source start error: {}", e))?;
        }

        // Create shared sinks and inject into engine
        for binding in &bindings {
            let sink_keys = engine.sink_keys_for_connector(&binding.connector_name);
            if sink_keys.is_empty() {
                continue;
            }

            let default_topic = engine
                .get_connector(&binding.connector_name)
                .and_then(|c| c.topic.clone())
                .unwrap_or_else(|| format!("{}-output", binding.connector_name));

            for sink_key in &sink_keys {
                let sink_topic = if let Some(topic) =
                    sink_key.strip_prefix(&format!("{}::", binding.connector_name))
                {
                    topic.to_string()
                } else {
                    default_topic.clone()
                };

                let empty_params = std::collections::HashMap::new();
                match registry.create_sink(&binding.connector_name, &sink_topic, &empty_params) {
                    Ok(sink) => {
                        engine.inject_sink(sink_key, sink);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to create sink for {}: {}", sink_key, e);
                    }
                }
            }

            if !sink_keys.is_empty() {
                println!("  Sharing connection for {} sink(s)", sink_keys.len(),);
            }
        }

        // Connect any remaining sinks that aren't registry-managed
        if engine.has_sink_operations() {
            engine
                .connect_sinks()
                .await
                .map_err(|e| anyhow::anyhow!("Sink connection error: {}", e))?;
        }

        // Spawn output event handler
        tokio::spawn(async move {
            while let Some(output_event) = output_rx.recv().await {
                println!(
                    "OUTPUT EVENT: {} - {:?}",
                    output_event.event_type, output_event.data
                );
            }
        });

        println!("\nListening for events...");
        println!("   Press Ctrl+C to stop\n");

        let start = std::time::Instant::now();
        let mut last_report = std::time::Instant::now();
        let mut event_count = 0u64;

        // Process events from all sources — batch when multiple are available
        loop {
            tokio::select! {
                Some(event) = event_rx.recv() => {
                    event_count += 1;

                    if let Some(ref mut orchestrator) = orchestrator {
                        let shared = std::sync::Arc::new(event);
                        match orchestrator.try_process(shared) {
                            Ok(()) => {}
                            Err(varpulis_runtime::DispatchError::ChannelFull(msg)) => {
                                if let varpulis_runtime::ContextMessage::Event(event) = msg {
                                    if let Err(e) = orchestrator.process(event).await {
                                        tracing::warn!("Orchestrator error: {}", e);
                                    }
                                }
                            }
                            Err(varpulis_runtime::DispatchError::ChannelClosed(_)) => {
                                tracing::warn!("Context channel closed");
                            }
                        }
                    } else {
                        // Check if more events are immediately available for batch processing
                        if event_rx.is_empty() {
                            // Single event — fast path
                            if let Err(e) = engine.process(event).await {
                                tracing::warn!("Engine error: {}", e);
                            }
                        } else {
                            // Multiple events buffered — drain and batch
                            let mut batch = vec![event];
                            while let Ok(extra) = event_rx.try_recv() {
                                batch.push(extra);
                            }
                            event_count += (batch.len() - 1) as u64;
                            if let Err(e) = engine.process_batch(batch).await {
                                tracing::warn!("Engine batch error: {}", e);
                            }
                        }
                    }

                    // Progress report every 2 seconds
                    if last_report.elapsed() >= std::time::Duration::from_secs(2) {
                        let metrics = engine.metrics();
                        print!("\rEvents: {} | Output events: {} | Rate: {:.0}/s    ",
                            metrics.events_processed,
                            metrics.output_events_emitted,
                            event_count as f64 / start.elapsed().as_secs_f64()
                        );
                        std::io::Write::flush(&mut std::io::stdout())?;
                        last_report = std::time::Instant::now();

                        // Periodic checkpoint tick (piggybacks on progress interval)
                        if let Some(ref mut orch) = orchestrator {
                            if let Err(e) = orch.checkpoint_tick() {
                                tracing::warn!("Checkpoint error: {}", e);
                            }
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\n\nStopping...");
                    registry.shutdown().await;
                    break;
                }
            }
        }

        // Shut down context orchestrator if active
        if let Some(orch) = orchestrator {
            orch.shutdown();
        }

        // Final stats
        let metrics = engine.metrics();
        println!("\nFinal Statistics:");
        println!("   Events processed: {}", metrics.events_processed);
        println!(
            "   Output events emitted: {}",
            metrics.output_events_emitted
        );
        println!("   Runtime: {:.1}s", start.elapsed().as_secs_f64());
    } else {
        // No source bindings - just show that we loaded successfully
        println!("\nNo source connector bindings found (.from() declarations).");
        println!("   To connect to MQTT, use the connector syntax:");
        println!("   ");
        println!("   connector MyMqtt = mqtt (");
        println!("       host: \"localhost\",");
        println!("       port: 1883");
        println!("   )");
        println!("   ");
        println!("   stream Events = SensorReading");
        println!("       .from(MyMqtt, topic: \"sensors/#\")");

        // Spawn output event handler anyway for demo mode
        tokio::spawn(async move {
            while let Some(output_event) = output_rx.recv().await {
                println!("\nOUTPUT EVENT: {}", output_event.event_type);
                for (key, value) in &output_event.data {
                    println!("   {}: {}", key, value);
                }
            }
        });
    }

    Ok(())
}

fn parse_and_show(source: &str) -> Result<()> {
    println!("Parsing VPL...\n");

    match parse(source) {
        Ok(program) => {
            println!("Parse successful!\n");
            println!("AST:");
            println!("{:#?}", program);
        }
        Err(e) => {
            println!("Parse error: {}", e);
        }
    }

    Ok(())
}

fn check_syntax(source: &str) -> Result<()> {
    match parse(source) {
        Ok(program) => {
            // Run semantic validation
            let validation = varpulis_core::validate::validate(source, &program);
            let errors: Vec<_> = validation
                .diagnostics
                .iter()
                .filter(|d| d.severity == varpulis_core::validate::Severity::Error)
                .collect();
            let warnings: Vec<_> = validation
                .diagnostics
                .iter()
                .filter(|d| d.severity == varpulis_core::validate::Severity::Warning)
                .collect();

            if errors.is_empty() {
                println!("Syntax OK");
                println!("   Statements: {}", program.statements.len());
                if !warnings.is_empty() {
                    println!("   Warnings:   {}", warnings.len());
                    for w in &warnings {
                        let (line, col) =
                            varpulis_core::validate::diagnostic_position(source, w.span.start);
                        let code_str = w.code.map(|c| format!("[{}] ", c)).unwrap_or_default();
                        println!("   {}:{}: warning: {}{}", line, col, code_str, w.message);
                        if let Some(ref hint) = w.hint {
                            println!("      hint: {}", hint);
                        }
                    }
                }
            } else {
                println!("Validation failed:");
                println!("   Errors:   {}", errors.len());
                for e in &errors {
                    let (line, col) =
                        varpulis_core::validate::diagnostic_position(source, e.span.start);
                    let code_str = e.code.map(|c| format!("[{}] ", c)).unwrap_or_default();
                    println!("   {}:{}: error: {}{}", line, col, code_str, e.message);
                    if let Some(ref hint) = e.hint {
                        println!("      hint: {}", hint);
                    }
                }
                if !warnings.is_empty() {
                    println!("   Warnings: {}", warnings.len());
                    for w in &warnings {
                        let (line, col) =
                            varpulis_core::validate::diagnostic_position(source, w.span.start);
                        let code_str = w.code.map(|c| format!("[{}] ", c)).unwrap_or_default();
                        println!("   {}:{}: warning: {}{}", line, col, code_str, w.message);
                        if let Some(ref hint) = w.hint {
                            println!("      hint: {}", hint);
                        }
                    }
                }
                std::process::exit(1);
            }
        }
        Err(e) => {
            println!("Syntax error: {}", e);

            // Show context around the error if we have a Located error
            if let varpulis_parser::ParseError::Located {
                line, column, hint, ..
            } = &e
            {
                // Show hint if available
                if let Some(h) = hint {
                    println!("   Hint: {}", h);
                }

                // Show the problematic line from source
                if let Some(error_line) = source.lines().nth(line - 1) {
                    println!("   |");
                    println!("   | {}", error_line);
                    println!("   | {}^", " ".repeat(column.saturating_sub(1)));
                }
            }

            std::process::exit(1);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_simulation(
    program_path: &PathBuf,
    events_path: &PathBuf,
    immediate: bool,
    verbose: bool,
    preload: bool,
    workers: Option<usize>,
    partition_by: Option<&str>,
    quiet: bool,
    checkpoint_dir: Option<PathBuf>,
    checkpoint_interval: u64,
) -> Result<()> {
    // Determine number of workers
    let num_workers = workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });

    // Configure rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .build_global()
        .ok(); // Ignore if already initialized

    let mode_str = if !immediate {
        "timed"
    } else if preload {
        if num_workers > 1 {
            "immediate (preload, parallel)"
        } else {
            "immediate (preload)"
        }
    } else if num_workers > 1 {
        "immediate (streaming, parallel)"
    } else {
        "immediate (streaming)"
    };

    println!("Varpulis Event Simulation");
    println!("============================");
    println!("Program: {}", program_path.display());
    println!("Events:  {}", events_path.display());
    println!("Mode:    {}", mode_str);
    println!("Workers: {}", num_workers);
    if let Some(key) = partition_by {
        println!("Partition: {}", key);
    }
    println!();

    // Load and parse program
    let program_source = std::fs::read_to_string(program_path)?;
    let program = parse(&program_source).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    let program = Arc::new(program);
    info!(
        "Loaded program with {} statements",
        program.statements.len()
    );

    // Create output event channel (shared across all engines)
    // PERF: Use SharedEvent (Arc<Event>) channel for zero-copy event passing
    // Use larger buffer in quiet mode since we're benchmarking throughput
    let channel_size = if quiet { 100_000 } else { 1000 * num_workers };
    let (output_tx, mut output_rx) = mpsc::channel::<SharedEvent>(channel_size);

    // Create initial engine to show metrics
    // In quiet mode, use benchmark engine to skip channel overhead entirely
    // PERF: Use new_shared() for zero-copy SharedEvent channel
    let mut engine = if quiet {
        Engine::new_benchmark()
    } else {
        Engine::new_shared(output_tx.clone())
    };
    engine
        .load_with_source(&program_source, &program)
        .map_err(|e| anyhow::anyhow!("Load error:\n{}", e))?;

    // Enable auto-checkpointing if requested (must be after load())
    if let Some(ref cp_dir) = checkpoint_dir {
        std::fs::create_dir_all(cp_dir)?;
        let store: std::sync::Arc<dyn varpulis_runtime::persistence::StateStore> =
            std::sync::Arc::new(
                varpulis_runtime::persistence::FileStore::open(cp_dir)
                    .map_err(|e| anyhow::anyhow!("Checkpoint store error: {}", e))?,
            );
        let config = varpulis_runtime::persistence::CheckpointConfig {
            interval: std::time::Duration::from_secs(checkpoint_interval),
            max_checkpoints: 3,
            checkpoint_on_shutdown: true,
            key_prefix: "varpulis-simulate".to_string(),
        };
        engine
            .enable_checkpointing(store, config)
            .map_err(|e| anyhow::anyhow!("Checkpoint init error: {}", e))?;
        println!(
            "Checkpointing: {} (every {}s)",
            cp_dir.display(),
            checkpoint_interval
        );
    }

    let metrics = engine.metrics();
    println!("Program loaded: {} streams", metrics.streams_count);
    println!();

    // Output event handling - use atomic counter in quiet mode for performance
    let output_count = Arc::new(AtomicU64::new(0));
    let output_emitted_count = Arc::new(AtomicU64::new(0)); // Track from engine metrics in quiet mode
    let output_events: Option<Arc<RwLock<Vec<SharedEvent>>>> = if quiet {
        None
    } else {
        Some(Arc::new(RwLock::new(Vec::new())))
    };

    let output_count_clone = output_count.clone();
    let output_events_clone = output_events.clone();

    tokio::spawn(async move {
        while let Some(output_event) = output_rx.recv().await {
            output_count_clone.fetch_add(1, Ordering::Relaxed);
            if verbose {
                println!(
                    "OUTPUT EVENT: {} - {:?}",
                    output_event.event_type, output_event.data
                );
            }
            // Only collect events if not in quiet mode
            // PERF: Zero-copy - just store the Arc reference
            if let Some(ref events_vec) = output_events_clone {
                events_vec.write().await.push(output_event);
            }
        }
    });

    println!("Starting simulation...\n");
    let start = std::time::Instant::now();

    // Shared counter for total events processed across all workers
    let total_events_processed = Arc::new(AtomicU64::new(0));

    // Process events based on mode
    if immediate && preload && num_workers > 1 {
        // Parallel preload mode - partition events and process in parallel
        let events_source = std::fs::read_to_string(events_path)?;
        let events = EventFileParser::parse(&events_source)
            .map_err(|e| anyhow::anyhow!("Event file error: {}", e))?;
        let total_events = events.len();
        info!("Preloaded {} events from file", total_events);

        // Probe pipeline to decide distribution strategy
        let mut probe_engine = Engine::new_benchmark();
        probe_engine
            .load(&program)
            .map_err(|e| anyhow::anyhow!("Probe engine load error: {}", e))?;
        let stateless = probe_engine.is_stateless();
        let auto_partition_key = probe_engine.partition_key();
        drop(probe_engine);

        // Extract owned events
        let mut all_events: Vec<Event> = events.into_iter().map(|te| te.event).collect();

        let partitions: Vec<Vec<Event>> = if stateless {
            // Round-robin: split owned Vec into N chunks (zero-copy via drain)
            let chunk_size = all_events.len().div_ceil(num_workers);
            let mut chunks = Vec::with_capacity(num_workers);
            while !all_events.is_empty() {
                let end = chunk_size.min(all_events.len());
                chunks.push(all_events.drain(..end).collect());
            }
            chunks
        } else {
            // Hash-partition by key for stateful pipelines
            let partition_key = partition_by
                .map(|s| s.to_string())
                .or(auto_partition_key)
                .unwrap_or_else(|| "symbol".to_string());
            if verbose {
                println!("  Partition key: {}", partition_key);
            }
            let mut buckets: Vec<Vec<Event>> = (0..num_workers).map(|_| Vec::new()).collect();
            for event in all_events {
                let key_hash = if let Some(val) = event.get(&partition_key) {
                    use std::hash::{Hash, Hasher};
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    val.hash(&mut hasher);
                    hasher.finish() % num_workers as u64
                } else {
                    use std::hash::{Hash, Hasher};
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    event.event_type.hash(&mut hasher);
                    hasher.finish() % num_workers as u64
                };
                buckets[key_hash as usize].push(event);
            }
            buckets
        };

        if verbose {
            let mode = if stateless {
                "round-robin"
            } else {
                "hash-partition"
            };
            println!(
                "  Distributed {} events into {} partitions ({})",
                total_events,
                partitions.len(),
                mode,
            );
            for (i, p) in partitions.iter().enumerate() {
                println!("    Worker {}: {} events", i, p.len());
            }
        }

        // Process partitions in parallel using spawn_blocking + rayon
        let program_arc = program.clone();
        let output_tx_arc = if quiet { None } else { Some(output_tx.clone()) };
        let total_counter = total_events_processed.clone();
        let output_counter = output_emitted_count.clone();

        tokio::task::spawn_blocking(move || {
            partitions
                .into_par_iter()
                .enumerate()
                .for_each(|(worker_id, partition_events)| {
                    if partition_events.is_empty() {
                        return;
                    }
                    let mut worker_engine = match &output_tx_arc {
                        Some(tx) => Engine::new_shared(tx.clone()),
                        None => Engine::new_benchmark(),
                    };
                    if let Err(e) = worker_engine.load(&program_arc) {
                        eprintln!("Worker {}: Failed to load program: {}", worker_id, e);
                        return;
                    }

                    // PERF: Pass entire partition as single batch — no extra copy
                    if let Err(e) = worker_engine.process_batch_sync(partition_events) {
                        eprintln!("Worker {}: Process error: {}", worker_id, e);
                        return;
                    }

                    let worker_metrics = worker_engine.metrics();
                    total_counter.fetch_add(worker_metrics.events_processed, Ordering::Relaxed);
                    output_counter
                        .fetch_add(worker_metrics.output_events_emitted, Ordering::Relaxed);
                });
        })
        .await
        .map_err(|e| anyhow::anyhow!("Spawn blocking failed: {}", e))?;
    } else if immediate && preload {
        // Single-threaded preload mode
        let events_source = std::fs::read_to_string(events_path)?;
        let events = EventFileParser::parse(&events_source)
            .map_err(|e| anyhow::anyhow!("Event file error: {}", e))?;
        info!("Preloaded {} events from file", events.len());

        // PERF: Extract owned events (zero-clone), use sync path when no sinks
        let use_sync = !engine.has_sink_operations();
        let mut all_events: Vec<Event> = events.into_iter().map(|te| te.event).collect();
        let total_events = all_events.len();

        const BATCH_SIZE: usize = 10000;

        if use_sync {
            // PERF: Sync path avoids async overhead (tokio runtime, .await suspension)
            let mut batch_idx = 0;
            while !all_events.is_empty() {
                let end = BATCH_SIZE.min(all_events.len());
                let batch: Vec<Event> = all_events.drain(..end).collect();
                if verbose {
                    let start_idx = batch_idx * BATCH_SIZE + 1;
                    let end_idx = (start_idx + batch.len() - 1).min(total_events);
                    println!(
                        "  [batch {}-{}] processing {} events (sync)",
                        start_idx,
                        end_idx,
                        batch.len()
                    );
                }
                engine
                    .process_batch_sync(batch)
                    .map_err(|e| anyhow::anyhow!("Process error: {}", e))?;
                engine
                    .checkpoint_tick()
                    .map_err(|e| anyhow::anyhow!("Checkpoint error: {}", e))?;
                batch_idx += 1;
            }
        } else {
            // Async path needed for .to() sink operations
            let mut batch_idx = 0;
            while !all_events.is_empty() {
                let end = BATCH_SIZE.min(all_events.len());
                let batch: Vec<Event> = all_events.drain(..end).collect();
                if verbose {
                    let start_idx = batch_idx * BATCH_SIZE + 1;
                    let end_idx = (start_idx + batch.len() - 1).min(total_events);
                    println!(
                        "  [batch {}-{}] processing {} events",
                        start_idx,
                        end_idx,
                        batch.len()
                    );
                }
                engine
                    .process_batch(batch)
                    .await
                    .map_err(|e| anyhow::anyhow!("Process error: {}", e))?;
                engine
                    .checkpoint_tick()
                    .map_err(|e| anyhow::anyhow!("Checkpoint error: {}", e))?;
                batch_idx += 1;
            }
        }
    } else if immediate && num_workers > 1 {
        // Parallel streaming mode
        const BATCH_SIZE: usize = 50000;

        let mut event_reader = StreamingEventReader::from_file(events_path)
            .map_err(|e| anyhow::anyhow!("Failed to open event file: {}", e))?;

        // Probe pipeline to decide distribution strategy
        let mut probe_engine = Engine::new_benchmark();
        probe_engine
            .load(&program)
            .map_err(|e| anyhow::anyhow!("Probe engine load error: {}", e))?;
        let stateless = probe_engine.is_stateless();
        let auto_partition_key = probe_engine.partition_key();
        drop(probe_engine);

        let partition_key = partition_by
            .map(|s| s.to_string())
            .or(auto_partition_key)
            .unwrap_or_else(|| "symbol".to_string());
        let mut batch_count = 0;

        // Pre-create engines for each worker (load program once, reuse across batches)
        use std::sync::Mutex;
        let worker_engines: Vec<Mutex<Engine>> = (0..num_workers)
            .map(|_| {
                let mut w_engine = if quiet {
                    Engine::new_benchmark()
                } else {
                    Engine::new_shared(output_tx.clone())
                };
                if let Err(e) = w_engine.load(&program) {
                    eprintln!("Failed to load program: {}", e);
                }
                Mutex::new(w_engine)
            })
            .collect();
        let worker_engines = Arc::new(worker_engines);

        loop {
            let mut events: Vec<Event> = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                match event_reader.next() {
                    Some(Ok(event)) => events.push(event),
                    Some(Err(e)) => return Err(anyhow::anyhow!("Parse error: {}", e)),
                    None => break,
                }
            }

            if events.is_empty() {
                break;
            }

            batch_count += 1;
            if verbose {
                println!(
                    "  [batch {}] processing {} events (total read: {})",
                    batch_count,
                    events.len(),
                    event_reader.events_read()
                );
            }

            // Distribute events to workers
            let partitions: Vec<Vec<Event>> = if stateless {
                // Round-robin: split owned Vec into N chunks (zero-copy via drain)
                let chunk_size = events.len().div_ceil(num_workers);
                let mut chunks = Vec::with_capacity(num_workers);
                while !events.is_empty() {
                    let end = chunk_size.min(events.len());
                    chunks.push(events.drain(..end).collect());
                }
                chunks
            } else {
                // Hash-partition by key
                let mut buckets: Vec<Vec<Event>> = (0..num_workers).map(|_| Vec::new()).collect();
                for event in events {
                    let key_hash = if let Some(val) = event.get(&partition_key) {
                        use std::hash::{Hash, Hasher};
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        val.hash(&mut hasher);
                        hasher.finish() % num_workers as u64
                    } else {
                        use std::hash::{Hash, Hasher};
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        event.event_type.hash(&mut hasher);
                        hasher.finish() % num_workers as u64
                    };
                    buckets[key_hash as usize].push(event);
                }
                buckets
            };

            let engines = worker_engines.clone();

            tokio::task::spawn_blocking(move || {
                partitions
                    .into_par_iter()
                    .enumerate()
                    .for_each(|(worker_id, partition_events)| {
                        if partition_events.is_empty() {
                            return;
                        }
                        let mut engine_guard = engines[worker_id].lock().unwrap();
                        if let Err(e) = engine_guard.process_batch_sync(partition_events) {
                            eprintln!("Worker {}: Process error: {}", worker_id, e);
                        }
                    });
            })
            .await
            .ok();
        }

        // Collect final metrics from all worker engines
        for (i, engine_mutex) in worker_engines.iter().enumerate() {
            let w_engine = engine_mutex.lock().unwrap();
            let worker_metrics = w_engine.metrics();
            total_events_processed.fetch_add(worker_metrics.events_processed, Ordering::Relaxed);
            output_emitted_count.fetch_add(worker_metrics.output_events_emitted, Ordering::Relaxed);
            if verbose {
                info!(
                    "Worker {}: processed {} events, emitted {}",
                    i, worker_metrics.events_processed, worker_metrics.output_events_emitted
                );
            }
        }

        info!("Streamed {} events from file", event_reader.events_read());
    } else if immediate {
        // Single-threaded streaming mode
        const BATCH_SIZE: usize = 10000;
        let use_sync = !engine.has_sink_operations();

        let mut event_reader = StreamingEventReader::from_file(events_path)
            .map_err(|e| anyhow::anyhow!("Failed to open event file: {}", e))?;

        let mut batch: Vec<Event> = Vec::with_capacity(BATCH_SIZE);
        let mut batch_count = 0;

        loop {
            // Fill batch from streaming reader (batch is empty after std::mem::take)
            batch.reserve(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                match event_reader.next() {
                    Some(Ok(event)) => batch.push(event),
                    Some(Err(e)) => return Err(anyhow::anyhow!("Parse error: {}", e)),
                    None => break,
                }
            }

            if batch.is_empty() {
                break;
            }

            batch_count += 1;
            if verbose {
                println!(
                    "  [batch {}] processing {} events (total read: {})",
                    batch_count,
                    batch.len(),
                    event_reader.events_read()
                );
            }

            // PERF: Use sync path when no .to() sinks to avoid async overhead
            if use_sync {
                engine
                    .process_batch_sync(std::mem::take(&mut batch))
                    .map_err(|e| anyhow::anyhow!("Process error: {}", e))?;
            } else {
                engine
                    .process_batch(std::mem::take(&mut batch))
                    .await
                    .map_err(|e| anyhow::anyhow!("Process error: {}", e))?;
            }
            engine
                .checkpoint_tick()
                .map_err(|e| anyhow::anyhow!("Checkpoint error: {}", e))?;
        }

        info!("Streamed {} events from file", event_reader.events_read());
    } else {
        // Timed mode - load all events for timing control
        let events_source = std::fs::read_to_string(events_path)?;
        let events = EventFileParser::parse(&events_source)
            .map_err(|e| anyhow::anyhow!("Event file error: {}", e))?;
        info!("Loaded {} events from file (timed mode)", events.len());

        let (event_tx, mut event_rx) = mpsc::channel(100);
        let player = EventFilePlayer::new(events.clone(), event_tx);

        // Spawn player
        let player_handle = tokio::spawn(async move { player.play().await });

        // Process events as they come
        let mut count = 0;
        while let Some(event) = event_rx.recv().await {
            count += 1;
            if verbose {
                println!(
                    "  [{:3}] @{:>6}ms {} {{ ... }}",
                    count,
                    start.elapsed().as_millis(),
                    event.event_type
                );
            }
            engine
                .process(event)
                .await
                .map_err(|e| anyhow::anyhow!("Process error: {}", e))?;
        }

        player_handle
            .await?
            .map_err(|e| anyhow::anyhow!("Player error: {}", e))?;
    }

    // Flush any remaining session windows after all events are processed
    if engine.has_session_windows() {
        engine
            .flush_expired_sessions()
            .await
            .map_err(|e| anyhow::anyhow!("Session flush error: {}", e))?;
    }

    // Final checkpoint on shutdown
    if engine.has_checkpointing() {
        engine
            .force_checkpoint()
            .map_err(|e| anyhow::anyhow!("Final checkpoint error: {}", e))?;
    }

    // Wait a bit for any pending output events
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Summary
    let elapsed = start.elapsed();

    // Get total events processed (from parallel counter or single engine)
    let events_processed = if num_workers > 1 && immediate {
        total_events_processed.load(Ordering::Relaxed)
    } else {
        engine.metrics().events_processed
    };

    // Use atomic counter for output count
    // In quiet mode, use the engine metrics counter; otherwise use channel counter
    let output_events_count = if quiet {
        // For parallel mode, use the aggregated counter; for single-threaded, use engine metrics
        let parallel_count = output_emitted_count.load(Ordering::Relaxed);
        let single_count = engine.metrics().output_events_emitted;
        (parallel_count + single_count) as usize
    } else {
        output_count.load(Ordering::Relaxed) as usize
    };

    println!("\nSimulation Complete");
    println!("======================");
    println!("Duration:         {:?}", elapsed);
    println!("Events processed: {}", events_processed);
    println!("Workers used:     {}", num_workers);
    println!("Output events emitted: {}", output_events_count);
    println!(
        "Event rate:       {:.1} events/sec",
        events_processed as f64 / elapsed.as_secs_f64()
    );

    // Show output events summary (only if collected, not in quiet mode)
    if output_events_count > 0 {
        if let Some(ref events_vec) = output_events {
            println!("\nOutput Events Summary:");
            for output_event in events_vec.read().await.iter() {
                println!("  - {}: {:?}", output_event.event_type, output_event.data);
            }
        }
    }

    Ok(())
}

async fn run_demo(
    duration_secs: u64,
    anomalies: bool,
    degradation: bool,
    enable_metrics: bool,
    metrics_port: u16,
) -> Result<()> {
    println!("Varpulis HVAC Building Demo");
    println!("================================");
    println!("Duration: {} seconds", duration_secs);
    println!(
        "Anomalies: {}",
        if anomalies { "enabled" } else { "disabled" }
    );
    println!(
        "Degradation: {}",
        if degradation { "enabled" } else { "disabled" }
    );
    if enable_metrics {
        println!("Metrics: http://127.0.0.1:{}/metrics", metrics_port);
    }
    println!();

    // Create event channel
    let (event_tx, mut event_rx) = mpsc::channel(1000);

    // Create simulator config
    let mut config = SimulatorConfig::default();
    if anomalies {
        config.anomaly_probability = 0.05;
    }
    if degradation {
        config.degradation_enabled = true;
    }

    // Create simulator
    let mut simulator = Simulator::new(config, event_tx);

    // Create output event channel
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(100);

    // Create engine with a simple stream
    let demo_program = r#"
        stream TemperatureReadings = TemperatureReading
        stream HumidityReadings = HumidityReading
        stream HVACStatuses = HVACStatus
    "#;

    let program = parse(demo_program).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;

    // Create prometheus metrics if enabled
    let prom_metrics = enable_metrics.then(Metrics::new);

    let mut engine = Engine::new(output_tx);
    if let Some(ref m) = prom_metrics {
        engine = engine.with_metrics(m.clone());
    }
    engine.load(&program).map_err(|e| anyhow::anyhow!(e))?;

    // Spawn metrics server if enabled
    if let Some(ref metrics) = prom_metrics {
        let server = MetricsServer::new(metrics.clone(), format!("127.0.0.1:{}", metrics_port));
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                tracing::error!("Metrics server error: {}", e);
            }
        });
    }

    // Spawn simulator
    let simulator_handle = tokio::spawn(async move {
        simulator.run().await;
    });

    // Spawn output event handler
    tokio::spawn(async move {
        while let Some(output_event) = output_rx.recv().await {
            println!("{}: {:?}", output_event.event_type, output_event.data);
        }
    });

    // Process events
    let start = std::time::Instant::now();
    let duration = std::time::Duration::from_secs(duration_secs);
    let mut event_count = 0u64;
    let mut last_report = std::time::Instant::now();

    println!("Starting event processing...\n");

    while start.elapsed() < duration {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                event_count += 1;

                // Process through engine
                if let Err(e) = engine.process(event.clone()).await {
                    tracing::warn!("Engine error: {}", e);
                }

                // Progress report every second
                if last_report.elapsed() >= std::time::Duration::from_secs(1) {
                    let metrics = engine.metrics();
                    print!("\rEvents: {} | Output events: {} | Rate: {:.0}/s    ",
                        metrics.events_processed,
                        metrics.output_events_emitted,
                        event_count as f64 / start.elapsed().as_secs_f64()
                    );
                    std::io::Write::flush(&mut std::io::stdout())?;
                    last_report = std::time::Instant::now();
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // Timeout, continue loop
            }
        }

        if start.elapsed() >= duration {
            break;
        }
    }

    // Final report
    println!("\n\nDemo Complete!");
    println!("================");
    let metrics = engine.metrics();
    println!("Total events processed: {}", metrics.events_processed);
    println!(
        "Total output events emitted: {}",
        metrics.output_events_emitted
    );
    println!(
        "Average rate: {:.0} events/sec",
        event_count as f64 / start.elapsed().as_secs_f64()
    );

    // Cleanup
    simulator_handle.abort();

    Ok(())
}

// =============================================================================
// Server Mode - WebSocket API for VSCode Extension
// =============================================================================

#[allow(clippy::too_many_arguments)]
async fn run_server(
    port: u16,
    enable_metrics: bool,
    metrics_port: u16,
    bind: &str,
    workdir: PathBuf,
    auth_config: AuthConfig,
    tls_config: Option<(PathBuf, PathBuf)>,
    rate_limit_config: rate_limit::RateLimitConfig,
    state_dir: Option<PathBuf>,
    coordinator_url: Option<String>,
    worker_id: Option<String>,
    advertise_address: Option<String>,
) -> Result<()> {
    let tls_enabled = tls_config.is_some();
    let protocol = if tls_enabled { "wss" } else { "ws" };

    let http_protocol = if tls_enabled { "https" } else { "http" };
    println!("Varpulis Server");
    println!("==================");
    println!("WebSocket: {}://{}:{}/ws", protocol, bind, port);
    println!("REST API:  {}://{}:{}/api/v1/", http_protocol, bind, port);
    println!("Workdir:   {}", workdir.display());
    println!(
        "TLS:       {}",
        if tls_enabled { "enabled" } else { "disabled" }
    );
    println!(
        "Auth:      {}",
        if auth_config.is_required() {
            "enabled (API key required)"
        } else {
            "disabled"
        }
    );
    println!(
        "Rate Limit: {}",
        if rate_limit_config.enabled {
            format!("{} req/s per client", rate_limit_config.requests_per_second)
        } else {
            "disabled".to_string()
        }
    );
    println!(
        "State:     {}",
        match &state_dir {
            Some(dir) => format!("{}", dir.display()),
            None => "in-memory (no persistence)".to_string(),
        }
    );
    if enable_metrics {
        println!("Metrics:   http://{}:{}/metrics", bind, metrics_port);
    }
    println!();

    // Create output event channel
    let (output_tx, output_rx) = mpsc::channel::<Event>(100);

    // Create shared state using websocket module
    let state = Arc::new(RwLock::new(ServerState::new(output_tx.clone(), workdir)));

    // Create metrics if enabled
    let prom_metrics = enable_metrics.then(|| {
        let metrics = Metrics::new();
        let server = MetricsServer::new(metrics.clone(), format!("{}:{}", bind, metrics_port));
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                tracing::error!("Metrics server error: {}", e);
            }
        });
        metrics
    });

    // Broadcast channel for output events to all connected clients
    let (broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(100);
    let broadcast_tx = Arc::new(broadcast_tx);

    // Spawn output event forwarder using websocket module
    websocket::forward_output_events_to_websocket(output_rx, broadcast_tx.clone());

    // Create auth config Arc for sharing
    let auth_config = Arc::new(auth_config);

    // Create rate limiter
    let rate_limiter = Arc::new(rate_limit::RateLimiter::new(rate_limit_config));

    // WebSocket route
    let state_filter = warp::any().map({
        let state = state.clone();
        move || state.clone()
    });

    let broadcast_filter = warp::any().map({
        let broadcast_tx = broadcast_tx.clone();
        move || broadcast_tx.clone()
    });

    // WebSocket route with authentication and rate limiting
    let ws_route = warp::path("ws")
        .and(auth::with_auth(auth_config.clone()))
        .and(rate_limit::with_rate_limit(rate_limiter.clone()))
        .and(warp::ws())
        .and(state_filter)
        .and(broadcast_filter)
        .map(
            |_auth: (),
             _rate_limit: rate_limit::RateLimitHeaders,
             ws: warp::ws::Ws,
             state: Arc<RwLock<ServerState>>,
             broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>| {
                ws.on_upgrade(move |socket| {
                    websocket::handle_connection(socket, state, broadcast_tx)
                })
            },
        );

    // Health check routes (no auth required) - for Kubernetes probes

    // Liveness probe: /health - is the process alive?
    let health_state = state.clone();
    let health_route = warp::path("health").and(warp::get()).and_then(move || {
        let state = health_state.clone();
        async move {
            let state = state.read().await;
            let uptime = state.start_time.elapsed().as_secs_f64();
            let response = serde_json::json!({
                "status": "healthy",
                "uptime_seconds": uptime,
                "version": env!("CARGO_PKG_VERSION"),
            });
            Ok::<_, warp::Rejection>(warp::reply::json(&response))
        }
    });

    // Readiness probe: /ready - is the engine ready to process events?
    let ready_state = state.clone();
    let ready_route = warp::path("ready").and(warp::get()).and_then(move || {
        let state = ready_state.clone();
        async move {
            let state = state.read().await;
            let streams_count = state.streams.len();

            if let Some(engine) = state.engine.as_ref() {
                let metrics = engine.metrics();
                let response = serde_json::json!({
                    "status": "ready",
                    "engine_loaded": true,
                    "streams_count": streams_count,
                    "events_processed": metrics.events_processed,
                    "output_events_emitted": metrics.output_events_emitted,
                });
                Ok::<_, warp::Rejection>(warp::reply::with_status(
                    warp::reply::json(&response),
                    warp::http::StatusCode::OK,
                ))
            } else {
                let response = serde_json::json!({
                    "status": "not_ready",
                    "engine_loaded": false,
                    "reason": "No VPL program loaded",
                });
                Ok::<_, warp::Rejection>(warp::reply::with_status(
                    warp::reply::json(&response),
                    warp::http::StatusCode::SERVICE_UNAVAILABLE,
                ))
            }
        }
    });

    // REST API routes (multi-tenant pipeline management)
    let tenant_manager = if let Some(ref state_dir) = state_dir {
        std::fs::create_dir_all(state_dir)?;
        let store: Arc<dyn varpulis_runtime::StateStore> =
            Arc::new(varpulis_runtime::FileStore::open(state_dir)?);
        info!("State persistence enabled: {}", state_dir.display());
        varpulis_runtime::shared_tenant_manager_with_store(store)
    } else {
        varpulis_runtime::shared_tenant_manager()
    };

    // Pass Prometheus metrics to tenant manager for engine instrumentation
    if let Some(ref m) = prom_metrics {
        tenant_manager
            .write()
            .await
            .set_prometheus_metrics(m.clone());
    }

    // Auto-provision a default tenant if an API key is configured
    if auth_config.is_required() {
        if let Some(key) = auth_config.api_key() {
            let mut mgr = tenant_manager.write().await;
            if mgr.get_tenant_by_api_key(key).is_none() {
                let _ = mgr.create_tenant(
                    "default".to_string(),
                    key.to_string(),
                    varpulis_runtime::TenantQuota::enterprise(),
                );
                info!("Auto-provisioned default tenant with configured API key");
            }
        }
    }

    let admin_key = auth_config.api_key().map(|s| s.to_string());
    let tenant_manager_for_heartbeat = tenant_manager.clone();
    let api_routes = api::api_routes(tenant_manager, admin_key);

    // Combined routes
    let routes = ws_route
        .or(health_route)
        .or(ready_route)
        .or(api_routes)
        .recover(auth::handle_rejection);

    // Parse bind address - NO unwrap()!
    let bind_addr: std::net::IpAddr = bind
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid bind address '{}': {}", bind, e))?;

    // Optionally register with a coordinator
    if let Some(coordinator_url) = coordinator_url {
        let worker_id = worker_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let worker_addr =
            advertise_address.unwrap_or_else(|| format!("{}://{}:{}", http_protocol, bind, port));
        let worker_api_key = auth_config.api_key().unwrap_or("no-key").to_string();
        info!(
            "Registering with coordinator at {} as worker '{}'",
            coordinator_url, worker_id
        );
        tokio::spawn(varpulis_cluster::worker_registration_loop(
            coordinator_url,
            worker_id,
            worker_addr,
            worker_api_key,
            Some(tenant_manager_for_heartbeat.clone()),
        ));
    }

    info!("Server listening on {}:{}", bind, port);

    // Start server with or without TLS
    if let Some((cert_path, key_path)) = tls_config {
        info!("TLS enabled with cert: {}", cert_path.display());
        warp::serve(routes)
            .tls()
            .cert_path(&cert_path)
            .key_path(&key_path)
            .run((bind_addr, port))
            .await;
    } else {
        warp::serve(routes).run((bind_addr, port)).await;
    }

    Ok(())
}

// =============================================================================
// Coordinator Mode
// =============================================================================

#[allow(clippy::too_many_arguments)]
async fn run_coordinator(
    port: u16,
    bind: &str,
    api_key: Option<String>,
    heartbeat_interval_secs: u64,
    heartbeat_timeout_secs: u64,
    scaling_policy: Option<varpulis_cluster::ScalingPolicy>,
    _ha: bool,
    _coordinator_id: Option<String>,
    _pod_namespace: Option<String>,
    _worker_label_selector: String,
    _raft_enabled: bool,
    _raft_node_id: Option<u64>,
    _raft_peers: Option<String>,
    _raft_data_dir: Option<String>,
) -> Result<()> {
    let http_protocol = "http";
    println!("Varpulis Coordinator");
    println!("=======================");
    println!(
        "API:       {}://{}:{}/api/v1/cluster/",
        http_protocol, bind, port
    );
    println!("WebSocket: ws://{}:{}/api/v1/cluster/ws", bind, port);
    println!(
        "Auth:      {}",
        if api_key.is_some() {
            "enabled (API key required)"
        } else {
            "disabled"
        }
    );
    println!(
        "Heartbeat: {}s interval, {}s timeout",
        heartbeat_interval_secs, heartbeat_timeout_secs
    );
    if let Some(ref sp) = scaling_policy {
        println!(
            "Scaling:   min={}, max={}, up={:.1}, down={:.1}",
            sp.min_workers, sp.max_workers, sp.scale_up_threshold, sp.scale_down_threshold
        );
    }
    if _ha {
        let id = _coordinator_id.as_deref().unwrap_or("unknown");
        println!("HA:        enabled (id={})", id);
    }

    // -----------------------------------------------------------------------
    // Raft consensus bootstrap (when feature enabled + --raft flag set)
    // -----------------------------------------------------------------------
    #[cfg(feature = "raft")]
    let (raft_handle, raft_peer_addrs_map) = {
        let handle: Option<varpulis_cluster::raft::RaftBootstrapResult>;
        let peer_map: std::collections::BTreeMap<u64, String>;

        if _raft_enabled {
            let node_id = _raft_node_id
                .ok_or_else(|| anyhow::anyhow!("--raft-node-id is required when --raft is set"))?;

            let peers_str = _raft_peers
                .ok_or_else(|| anyhow::anyhow!("--raft-peers is required when --raft is set"))?;

            let peer_addrs: Vec<String> =
                peers_str.split(',').map(|s| s.trim().to_string()).collect();

            // Build NodeId → address map for leader forwarding
            peer_map = peer_addrs
                .iter()
                .enumerate()
                .map(|(i, addr)| ((i + 1) as u64, addr.clone()))
                .collect();

            println!("Raft:      node_id={}, peers={}", node_id, peers_str);

            let result = if let Some(ref data_dir) = _raft_data_dir {
                println!("Raft:      persistent storage at {}", data_dir);
                #[cfg(feature = "persistent")]
                {
                    varpulis_cluster::raft::bootstrap_persistent(
                        node_id,
                        &peer_addrs,
                        api_key.clone(),
                        data_dir,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("Raft bootstrap failed: {e}"))?
                }
                #[cfg(not(feature = "persistent"))]
                {
                    anyhow::bail!(
                        "--raft-data-dir requires the 'persistent' feature (build with --features persistent)"
                    );
                }
            } else {
                varpulis_cluster::raft::bootstrap(node_id, &peer_addrs, api_key.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("Raft bootstrap failed: {e}"))?
            };

            println!("Raft:      initialized (node {})", node_id);
            handle = Some(result);
        } else {
            handle = None;
            peer_map = std::collections::BTreeMap::new();
        }
        (handle, peer_map)
    };

    #[cfg(not(feature = "raft"))]
    let _ = (_raft_enabled, _raft_node_id, _raft_peers, _raft_data_dir);

    println!();

    let coordinator = varpulis_cluster::shared_coordinator();
    let ws_manager = varpulis_cluster::shared_ws_manager();
    {
        let mut coord = coordinator.write().await;
        coord.heartbeat_interval = std::time::Duration::from_secs(heartbeat_interval_secs);
        coord.heartbeat_timeout = std::time::Duration::from_secs(heartbeat_timeout_secs);
        coord.scaling_policy = scaling_policy;

        // Attach Raft handle to coordinator
        #[cfg(feature = "raft")]
        if let Some(ref rh) = raft_handle {
            coord.raft_handle = Some(varpulis_cluster::coordinator::RaftHandle {
                raft: rh.raft.clone(),
                store_state: rh.shared_state.clone(),
                peer_addrs: raft_peer_addrs_map.clone(),
                admin_key: api_key.clone(),
            });
        }
    }

    // Spawn periodic health sweep with automatic failover and rebalancing
    let health_coordinator = coordinator.clone();
    let sweep_interval = std::time::Duration::from_secs(heartbeat_interval_secs);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sweep_interval);
        loop {
            interval.tick().await;
            let mut coord = health_coordinator.write().await;

            // Update Raft role if enabled
            #[cfg(feature = "raft")]
            coord.update_raft_role();

            // Sync from Raft on ALL nodes: followers get updated state,
            // leader refreshes heartbeat timestamps for remote workers
            // (heartbeat proxy — prevents false unhealthy for workers
            // connected to other coordinators via WS).
            #[cfg(feature = "raft")]
            coord.sync_from_raft();

            // Only the leader (or standalone) runs health sweeps and failover
            if !coord.ha_role.is_writer() {
                continue;
            }

            let result = coord.health_sweep();

            // Trigger automatic failover for newly unhealthy workers
            if !result.workers_marked_unhealthy.is_empty() {
                let failed_workers: Vec<varpulis_cluster::WorkerId> =
                    result.workers_marked_unhealthy.clone();
                for wid in &failed_workers {
                    tracing::warn!("Worker {} marked unhealthy — triggering failover", wid);
                }

                // Propagate unhealthy status to Raft for cross-coordinator visibility
                #[cfg(feature = "raft")]
                if let Some(ref handle) = coord.raft_handle {
                    for wid in &failed_workers {
                        let cmd = varpulis_cluster::raft::ClusterCommand::WorkerStatusChanged {
                            id: wid.0.clone(),
                            status: "unhealthy".to_string(),
                        };
                        if let Err(e) = handle.raft.client_write(cmd).await {
                            tracing::warn!("Failed to propagate {} unhealthy to Raft: {e}", wid);
                        }
                    }
                }

                for wid in failed_workers {
                    coord.handle_worker_failure(&wid).await;
                }
            }

            // Check connector health — log warnings for dead connectors
            let unhealthy_connectors = coord.check_connector_health();
            for (pipeline_name, worker_id, connector_name) in &unhealthy_connectors {
                tracing::warn!(
                    "Connector '{}' on pipeline '{}' (worker {}) is disconnected",
                    connector_name,
                    pipeline_name,
                    worker_id
                );
            }

            // Clean up stale completed migrations (older than 1 hour)
            coord.cleanup_completed_migrations(std::time::Duration::from_secs(3600));

            // Trigger rebalance if a new worker was recently registered
            if coord.pending_rebalance {
                match coord.rebalance().await {
                    Ok(ids) if !ids.is_empty() => {
                        tracing::info!("Auto-rebalance: {} migration(s) started", ids.len());
                    }
                    Ok(_) => {} // nothing to rebalance
                    Err(e) => {
                        tracing::error!("Auto-rebalance failed: {}", e);
                    }
                }
            }

            // Evaluate auto-scaling and fire webhook if needed
            if let Some(rec) = coord.evaluate_scaling() {
                if rec.action != varpulis_cluster::ScalingAction::Stable {
                    tracing::info!(
                        "Scaling recommendation: {:?} (current={}, target={}, reason={})",
                        rec.action,
                        rec.current_workers,
                        rec.target_workers,
                        rec.reason
                    );
                }
            }
            coord.fire_scaling_webhook().await;
        }
    });

    // Health endpoint (no auth) — includes operational data
    let health_coordinator = coordinator.clone();
    let health_ws_manager = ws_manager.clone();
    let health_route = warp::path("health").and(warp::get()).and_then(move || {
        let coord = health_coordinator.clone();
        let ws_mgr = health_ws_manager.clone();
        async move {
            let coord = coord.read().await;
            let ws_mgr = ws_mgr.read().await;
            let total_workers = coord.workers.len();
            let healthy_workers = coord
                .workers
                .values()
                .filter(|w| w.status == varpulis_cluster::WorkerStatus::Ready)
                .count();
            let unhealthy_workers = coord
                .workers
                .values()
                .filter(|w| w.status == varpulis_cluster::WorkerStatus::Unhealthy)
                .count();
            let draining_workers = coord
                .workers
                .values()
                .filter(|w| w.status == varpulis_cluster::WorkerStatus::Draining)
                .count();
            let active_migrations = coord
                .active_migrations
                .values()
                .filter(|m| {
                    !matches!(
                        m.status,
                        varpulis_cluster::MigrationStatus::Completed
                            | varpulis_cluster::MigrationStatus::Failed(_)
                    )
                })
                .count();
            let total_events: u64 = coord.workers.values().map(|w| w.events_processed).sum();
            let ws_connections = ws_mgr.connected_count();
            let last_sweep = coord.last_health_sweep.as_ref().map(|s| {
                serde_json::json!({
                    "workers_checked": s.workers_checked,
                    "workers_marked_unhealthy": s.workers_marked_unhealthy.len(),
                })
            });

            let status = if unhealthy_workers == 0 && total_workers > 0 {
                "healthy"
            } else if unhealthy_workers > 0 && healthy_workers > 0 {
                "degraded"
            } else if total_workers == 0 {
                "no_workers"
            } else {
                "critical"
            };

            let response = serde_json::json!({
                "status": status,
                "role": "coordinator",
                "version": env!("CARGO_PKG_VERSION"),
                "workers": {
                    "total": total_workers,
                    "healthy": healthy_workers,
                    "unhealthy": unhealthy_workers,
                    "draining": draining_workers,
                },
                "ws_connections": ws_connections,
                "pipeline_groups": coord.pipeline_groups.len(),
                "active_migrations": active_migrations,
                "total_events_processed": total_events,
                "last_health_sweep": last_sweep,
            });
            Ok::<_, warp::Rejection>(warp::reply::json(&response))
        }
    });

    // Readiness probe — returns 200 only for standalone/leader coordinators
    let ready_route = warp::path("ready").and(warp::get()).and_then(|| async {
        let response = serde_json::json!({
            "status": "ready",
            "role": "coordinator",
        });
        Ok::<_, warp::Rejection>(warp::reply::json(&response))
    });

    let bind_addr: std::net::IpAddr = bind
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid bind address '{}': {}", bind, e))?;
    info!("Coordinator listening on {}:{}", bind, port);

    // Build and serve routes (split by cfg to avoid Reply type mismatch in if/else)
    #[cfg(feature = "raft")]
    {
        if let Some(ref rh) = raft_handle {
            let api_routes = varpulis_cluster::api::cluster_routes_with_raft(
                coordinator,
                api_key,
                ws_manager,
                rh.raft.clone(),
            );
            let routes = health_route
                .or(ready_route)
                .or(api_routes)
                .recover(varpulis_cluster::api::handle_rejection);
            warp::serve(routes).run((bind_addr, port)).await;
        } else {
            let api_routes = varpulis_cluster::cluster_routes(coordinator, api_key, ws_manager);
            let routes = health_route
                .or(ready_route)
                .or(api_routes)
                .recover(varpulis_cluster::api::handle_rejection);
            warp::serve(routes).run((bind_addr, port)).await;
        }
    }

    #[cfg(not(feature = "raft"))]
    {
        let api_routes = varpulis_cluster::cluster_routes(coordinator, api_key, ws_manager);
        let routes = health_route
            .or(ready_route)
            .or(api_routes)
            .recover(varpulis_cluster::api::handle_rejection);
        warp::serve(routes).run((bind_addr, port)).await;
    }

    Ok(())
}

// =============================================================================
// Import Resolution
// =============================================================================

/// Maximum depth for nested imports to prevent stack overflow
const MAX_IMPORT_DEPTH: usize = 10;

/// Resolve import statements by loading and parsing imported files
fn resolve_imports(program: &mut Program, base_path: Option<&PathBuf>) -> Result<()> {
    use std::collections::HashSet;
    let mut visited = HashSet::new();
    resolve_imports_inner(program, base_path, 0, &mut visited)
}

/// Inner implementation with depth tracking and cycle detection
fn resolve_imports_inner(
    program: &mut Program,
    base_path: Option<&PathBuf>,
    depth: usize,
    visited: &mut std::collections::HashSet<PathBuf>,
) -> Result<()> {
    // Check recursion depth limit
    if depth > MAX_IMPORT_DEPTH {
        anyhow::bail!(
            "Import depth limit exceeded (max {}). Check for circular imports.",
            MAX_IMPORT_DEPTH
        );
    }

    let mut imported_statements = Vec::new();
    let mut imports_to_process = Vec::new();

    // Collect import statements
    for stmt in &program.statements {
        if let Stmt::Import { path, .. } = &stmt.node {
            imports_to_process.push(path.clone());
        }
    }

    // Process each import
    for import_path in imports_to_process {
        let full_path = if let Some(base) = base_path {
            base.join(&import_path)
        } else {
            PathBuf::from(&import_path)
        };

        // Canonicalize to detect cycles with different relative paths
        let canonical_path = full_path.canonicalize().map_err(|e| {
            anyhow::anyhow!("Failed to resolve import '{}': {}", full_path.display(), e)
        })?;

        // Check for circular import
        if visited.contains(&canonical_path) {
            info!(
                "Skipping already imported file: {}",
                canonical_path.display()
            );
            continue;
        }
        visited.insert(canonical_path.clone());

        info!("Loading import: {}", full_path.display());

        let import_source = std::fs::read_to_string(&full_path).map_err(|e| {
            anyhow::anyhow!("Failed to read import '{}': {}", full_path.display(), e)
        })?;

        let import_program = parse(&import_source).map_err(|e| {
            anyhow::anyhow!("Parse error in import '{}': {}", full_path.display(), e)
        })?;

        info!(
            "Imported {} statements from {}",
            import_program.statements.len(),
            full_path.display()
        );

        // Recursively resolve imports in the imported file
        let import_base = full_path.parent().map(|p| p.to_path_buf());
        let mut imported = import_program;
        resolve_imports_inner(&mut imported, import_base.as_ref(), depth + 1, visited)?;

        imported_statements.extend(imported.statements);
    }

    // Remove import statements and prepend imported content
    program
        .statements
        .retain(|stmt| !matches!(&stmt.node, Stmt::Import { .. }));

    // Insert imported statements at the beginning (before the main file's statements)
    let mut new_statements = imported_statements;
    new_statements.append(&mut program.statements);
    program.statements = new_statements;

    Ok(())
}
