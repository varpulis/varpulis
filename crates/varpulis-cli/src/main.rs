#![allow(missing_docs)]
//! Varpulis CLI - Command line interface for Varpulis streaming analytics engine

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, Table};
use varpulis_cli::auth::AuthConfig;
use varpulis_cli::client::VarpulisClient;
use varpulis_cli::config::Config;
use varpulis_cli::output;
use varpulis_cli::{rate_limit, security, users};

mod commands;

use commands::connector::ConnectorAction;
use commands::federation::FederationAction;

#[derive(Parser)]
#[command(name = "varpulis")]
#[command(author = "Varpulis Contributors")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Varpulis - Modern streaming analytics engine", long_about = None)]
struct Cli {
    /// Path to configuration file (YAML or TOML)
    #[arg(short, long, global = true, env = "VARPULIS_CONFIG")]
    config: Option<PathBuf>,

    /// OpenTelemetry OTLP endpoint (e.g. http://localhost:4317). Requires 'otel' feature.
    #[arg(long, global = true, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otel_endpoint: Option<String>,

    /// Path to connector credentials file (YAML) for resolving security profiles
    #[arg(long, global = true, env = "VARPULIS_CREDENTIALS")]
    credentials: Option<PathBuf>,

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

        /// Allowed CORS origins (comma-separated). Default: localhost only.
        /// Use "*" to explicitly allow all origins, or specify domains like
        /// `https://app.example.com,https://admin.example.com`
        #[arg(long, env = "VARPULIS_CORS_ORIGINS", value_delimiter = ',')]
        cors_origins: Option<Vec<String>>,

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
        /// Defaults to http://`<bind>`:`<port>`. Use this when the bind address (0.0.0.0)
        /// is not reachable from the coordinator (e.g., in Docker networks).
        #[arg(long, env = "VARPULIS_ADVERTISE_ADDRESS")]
        advertise_address: Option<String>,

        /// NATS server URL for cluster communication (e.g., nats://localhost:4222).
        /// When set with --coordinator, uses NATS instead of HTTP/WebSocket for
        /// registration, heartbeats, and command dispatch.
        #[arg(long, env = "VARPULIS_NATS")]
        nats: Option<String>,

        /// Path to CA certificate for verifying the coordinator's TLS certificate.
        /// Required when connecting to a coordinator using HTTPS with a private CA.
        #[arg(long, env = "VARPULIS_TLS_CA_CERT")]
        tls_ca_cert: Option<PathBuf>,

        /// Path to client certificate for mTLS authentication with the coordinator (PEM format)
        #[arg(long, env = "VARPULIS_TLS_CLIENT_CERT")]
        tls_client_cert: Option<PathBuf>,

        /// Path to client private key for mTLS authentication (PEM format)
        #[arg(long, env = "VARPULIS_TLS_CLIENT_KEY")]
        tls_client_key: Option<PathBuf>,

        /// Maximum queue depth before rejecting events with HTTP 429 (0 = unlimited)
        #[arg(long, env = "VARPULIS_MAX_QUEUE_DEPTH", default_value = "50000")]
        max_queue_depth: u64,

        /// Default admin password for first-start bootstrapping.
        /// If set and no admin user exists in DB, the admin user is created with this password.
        #[arg(long, env = "VARPULIS_ADMIN_PASSWORD")]
        admin_password: Option<String>,

        /// Session idle timeout in minutes (default: 30)
        #[arg(long, env = "VARPULIS_SESSION_IDLE_TIMEOUT", default_value = "30")]
        session_idle_timeout: u64,

        /// Session absolute timeout in hours (default: 24)
        #[arg(long, env = "VARPULIS_SESSION_ABSOLUTE_TIMEOUT", default_value = "24")]
        session_absolute_timeout: u64,

        /// Maximum parallel sessions per user (default: 5)
        #[arg(long, env = "VARPULIS_MAX_SESSIONS", default_value = "5")]
        max_sessions: usize,
    },

    /// Simulate events from an event file (.evt)
    Simulate {
        /// Path to the VPL program (.vpl)
        #[arg(short, long)]
        program: PathBuf,

        /// Path to the event file (.evt)
        #[arg(short, long)]
        events: PathBuf,

        /// Replay events with real-time timing delays (respects @Ns and BATCH directives)
        #[arg(long)]
        timed: bool,

        /// Verbose output (show each event)
        #[arg(short, long)]
        verbose: bool,

        /// Stream events line-by-line instead of preloading (lower memory for huge files)
        #[arg(long)]
        streaming: bool,

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

        /// Watch for file changes and re-run simulation automatically
        #[arg(long)]
        watch: bool,

        /// Show detailed trace of event processing through the pipeline
        #[arg(long)]
        trace: bool,
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
        /// Server URL (e.g. http://localhost:9000). Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_SERVER")]
        server: Option<String>,

        /// Tenant API key. Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,

        /// Path to the .vpl file
        #[arg(short, long)]
        file: PathBuf,

        /// Pipeline name (defaults to `.varpulis.toml` `[deploy]` name or filename)
        #[arg(short, long)]
        name: Option<String>,
    },

    /// List pipelines on a remote Varpulis server
    Pipelines {
        /// Server URL (e.g. http://localhost:9000). Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_SERVER")]
        server: Option<String>,

        /// Tenant API key. Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,
    },

    /// Delete a pipeline from a remote Varpulis server
    Undeploy {
        /// Server URL (e.g. http://localhost:9000). Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_SERVER")]
        server: Option<String>,

        /// Tenant API key. Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,

        /// Pipeline ID to delete
        #[arg(long)]
        pipeline_id: String,
    },

    /// Show usage statistics from a remote Varpulis server
    Status {
        /// Server URL (e.g. http://localhost:9000). Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_SERVER")]
        server: Option<String>,

        /// Tenant API key. Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,
    },

    /// Initialize a .varpulis.toml project configuration file
    Init {
        /// Server URL to configure
        #[arg(long)]
        server: Option<String>,

        /// API key to configure
        #[arg(long)]
        api_key: Option<String>,
    },

    /// Stream live output events from a deployed pipeline (SSE)
    Logs {
        /// Server URL (e.g. http://localhost:9000). Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_SERVER")]
        server: Option<String>,

        /// Tenant API key. Also reads from .varpulis.toml
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,

        /// Pipeline ID to stream logs from
        #[arg(long)]
        pipeline_id: String,
    },

    /// Start cluster coordinator (control plane for distributed execution)
    Coordinator {
        /// Coordinator port
        #[arg(short, long, default_value = "9100")]
        port: u16,

        /// Bind address
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// API key for coordinator authentication (single admin key, backward-compatible)
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,

        /// Path to API keys JSON file for multi-key RBAC (overrides --api-key)
        #[arg(long, env = "VARPULIS_API_KEYS")]
        api_keys: Option<PathBuf>,

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

        /// LLM endpoint URL for AI chat assistant (e.g., http://ollama:11434/v1)
        #[arg(long, env = "VARPULIS_LLM_ENDPOINT")]
        llm_endpoint: Option<String>,

        /// LLM model name (e.g., qwen2.5:7b, claude-sonnet-4-5-20250929)
        #[arg(long, default_value = "qwen2.5:7b", env = "VARPULIS_LLM_MODEL")]
        llm_model: String,

        /// LLM API key (not needed for Ollama)
        #[arg(long, env = "VARPULIS_LLM_API_KEY")]
        llm_api_key: Option<String>,

        /// LLM provider: openai-compatible (default) or anthropic
        #[arg(
            long,
            default_value = "openai-compatible",
            env = "VARPULIS_LLM_PROVIDER"
        )]
        llm_provider: String,

        /// Path to TLS certificate file (PEM format). Enables HTTPS when provided with --tls-key
        #[arg(long, env = "VARPULIS_TLS_CERT")]
        tls_cert: Option<PathBuf>,

        /// Path to TLS private key file (PEM format). Required when --tls-cert is provided
        #[arg(long, env = "VARPULIS_TLS_KEY")]
        tls_key: Option<PathBuf>,

        /// Path to CA certificate for client verification (mTLS). When set, workers must present
        /// a valid client certificate signed by this CA to connect.
        #[arg(long, env = "VARPULIS_TLS_CA_CERT")]
        tls_ca_cert: Option<PathBuf>,

        /// NATS server URL for cluster communication (e.g., nats://localhost:4222).
        /// When set, the coordinator uses NATS for worker registration, heartbeats,
        /// and command dispatch instead of HTTP/WebSocket.
        #[arg(long, env = "VARPULIS_NATS")]
        nats: Option<String>,

        /// Rate limit in requests per second per client (0 = disabled).
        /// Applies to mutating API endpoints (POST/PUT/DELETE). Read-only GET
        /// endpoints and health/readiness probes are exempt.
        #[arg(long, env = "VARPULIS_COORDINATOR_RATE_LIMIT", default_value = "0")]
        rate_limit: u32,

        /// Allowed CORS origins (comma-separated). Default: localhost only.
        /// Use "*" to explicitly allow all origins, or specify domains like
        /// `https://app.example.com,https://admin.example.com`
        #[arg(long, env = "VARPULIS_CORS_ORIGINS", value_delimiter = ',')]
        cors_origins: Option<Vec<String>>,
    },

    /// Generate synthetic events to stdout
    Generate {
        /// Event schema to use (fraud, iot, trading)
        #[arg(long)]
        schema: varpulis_datagen::SchemaType,

        /// Events per second to generate
        #[arg(long, default_value = "1000")]
        rate: u64,

        /// Duration string (e.g. "60s", "5m", "1h")
        #[arg(long, default_value = "60s")]
        duration: String,

        /// Fraction of events that are anomalies (0.0-1.0)
        #[arg(long, default_value = "0.05")]
        anomaly_rate: f64,

        /// Random seed for reproducibility
        #[arg(long)]
        seed: Option<u64>,

        /// Output format: "json" (pretty) or "jsonl" (one JSON object per line)
        #[arg(long, default_value = "jsonl")]
        format: String,
    },

    /// Multi-region federation management
    Federation {
        #[command(subcommand)]
        action: FederationAction,

        /// Coordinator URL
        #[arg(
            long,
            default_value = "http://localhost:9100",
            env = "VARPULIS_COORDINATOR"
        )]
        coordinator: String,

        /// API key for authentication
        #[arg(long, env = "VARPULIS_API_KEY")]
        api_key: Option<String>,
    },

    /// Infer event type declarations from sample data
    Infer {
        /// Input event file (.evt or .jsonl)
        #[arg(short, long)]
        input: PathBuf,
        /// Output file (stdout if not specified)
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Number of events to sample (default: 100)
        #[arg(long, default_value = "100")]
        sample_size: usize,
    },

    /// Encrypt sensitive fields in a connector credentials file
    EncryptCredentials {
        /// Path to the credentials YAML file (will be overwritten with encrypted values)
        #[arg(short, long)]
        input: PathBuf,

        /// Output path (default: overwrite input file)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Generate a random 256-bit master key for encrypting connector credentials
    GenerateMasterKey,

    /// Manage and inspect connectors
    Connector {
        #[command(subcommand)]
        action: ConnectorAction,
    },

    /// Interactive VPL shell
    #[cfg(feature = "repl")]
    Repl {
        /// VPL program file to auto-load
        #[arg(short, long)]
        file: Option<PathBuf>,
    },

    /// Interactive streaming session
    ///
    /// Default: Python-interpreter-style shell (type VPL + events directly).
    /// --json: JSON-line protocol for agents/MCP.
    /// --tui: Split-pane terminal UI with topology, events, metrics.
    Interactive {
        /// Use JSON-line protocol on stdin/stdout (for agents/MCP)
        #[arg(long)]
        json: bool,

        /// Use split-pane TUI (topology, events, input, metrics)
        #[arg(long)]
        tui: bool,

        /// VPL program file to auto-load
        #[arg(short, long)]
        file: Option<PathBuf>,

        /// Start datagen on launch with given schema (fraud, iot, trading)
        #[arg(long)]
        generate: Option<String>,

        /// Datagen rate (events/sec)
        #[arg(long, default_value = "1000")]
        rate: u64,

        /// Enable trace mode
        #[arg(long)]
        trace: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider before any TLS connections (MQTT, HTTPS, etc.)
    // rumqttc's `use-rustls` pulls in rustls without a provider, so we must install one.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Parse CLI first so we can check otel_endpoint before initializing tracing
    let cli = Cli::parse();

    // Initialize logging with RUST_LOG support
    // Default: info level, can be overridden with RUST_LOG env var
    // Examples: RUST_LOG=debug, RUST_LOG=varpulis=trace,info
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    #[cfg(feature = "otel")]
    {
        if let Some(ref endpoint) = cli.otel_endpoint {
            use opentelemetry::trace::TracerProvider as _;
            use opentelemetry_otlp::WithExportConfig;
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;

            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .expect("Failed to create OTLP exporter");

            let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
                .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
                .with_resource(opentelemetry_sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", "varpulis"),
                ]))
                .build();

            let tracer = tracer_provider.tracer("varpulis");
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .with(otel_layer)
                .init();

            tracing::info!("OpenTelemetry tracing enabled, exporting to {}", endpoint);
        } else {
            tracing_subscriber::fmt().with_env_filter(env_filter).init();
        }
    }
    #[cfg(not(feature = "otel"))]
    {
        if cli.otel_endpoint.is_some() {
            eprintln!("Warning: --otel-endpoint requires the 'otel' feature. Rebuild with: cargo build --features otel");
        }
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    // Load credentials store if --credentials is provided
    let credentials_store = if let Some(ref creds_path) = cli.credentials {
        let store = varpulis_connectors::credentials::CredentialsStore::from_file(creds_path)
            .map_err(|e| anyhow::anyhow!("Failed to load credentials file: {e}"))?;
        Some(std::sync::Arc::new(store))
    } else {
        None
    };

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

            commands::run::run_program(&source, base_path.as_ref(), credentials_store).await?;
        }

        Commands::Parse { file } => {
            let source = std::fs::read_to_string(&file)?;
            commands::validate::parse_and_show(&source, &file.to_string_lossy())?;
        }

        Commands::Demo {
            duration,
            anomalies,
            degradation,
            metrics,
            metrics_port,
        } => {
            commands::demo::run_demo(duration, anomalies, degradation, metrics, metrics_port)
                .await?;
        }

        Commands::Check { file } => {
            let source = std::fs::read_to_string(&file)?;
            commands::validate::check_syntax(&source, &file.to_string_lossy())?;
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
            cors_origins,
            state_dir,
            coordinator,
            worker_id,
            advertise_address,
            nats,
            tls_ca_cert,
            tls_client_cert,
            tls_client_key,
            max_queue_depth,
            admin_password,
            session_idle_timeout,
            session_absolute_timeout,
            max_sessions,
        } => {
            // Use security module to validate workdir - NO unwrap()!
            let workdir =
                security::validate_workdir(workdir).map_err(|e| anyhow::anyhow!("{e}"))?;

            // Create auth config from CLI argument or environment variable
            let auth_config = match api_key {
                Some(key) => AuthConfig::with_api_key(key),
                None => AuthConfig::disabled(),
            };

            // Forward --admin-password to env var for server.rs DB bootstrap
            if let Some(ref pw) = admin_password {
                std::env::set_var("VARPULIS_ADMIN_PASSWORD", pw);
            }

            // Create session manager for local auth
            let session_config = users::SessionConfig {
                idle_timeout: std::time::Duration::from_secs(session_idle_timeout * 60),
                absolute_timeout: std::time::Duration::from_secs(session_absolute_timeout * 3600),
                max_parallel_sessions: max_sessions,
                ..Default::default()
            };
            let session_manager = users::SessionManager::new(session_config);
            let shared_session_manager: users::SharedSessionManager =
                std::sync::Arc::new(tokio::sync::RwLock::new(session_manager));

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

            // Build mTLS client config for worker->coordinator communication
            let mtls_client_config = commands::server::build_mtls_client_config(
                tls_ca_cert.as_deref(),
                tls_client_cert.as_deref(),
                tls_client_key.as_deref(),
            )?;

            commands::server::run_server(
                port,
                metrics,
                metrics_port,
                &bind,
                workdir,
                auth_config,
                tls_config,
                rate_limit_config,
                cors_origins,
                state_dir,
                coordinator,
                worker_id,
                advertise_address,
                nats,
                mtls_client_config,
                max_queue_depth,
                Some(shared_session_manager),
            )
            .await?;
        }

        Commands::Simulate {
            program,
            events,
            timed,
            verbose,
            streaming,
            workers,
            partition_by,
            quiet,
            checkpoint_dir,
            checkpoint_interval,
            watch,
            trace,
        } => {
            // Load config file if specified
            let config = if let Some(ref config_path) = cli.config {
                Some(Config::load(config_path).map_err(|e| anyhow::anyhow!("{e}"))?)
            } else {
                None
            };

            // Merge config file settings with CLI arguments
            let workers = workers.or(config.as_ref().and_then(|c| c.processing.workers));
            let partition_by = partition_by.or(config
                .as_ref()
                .and_then(|c| c.processing.partition_by.clone()));

            if watch {
                commands::simulate::run_watch_loop(
                    &program,
                    &events,
                    timed,
                    verbose,
                    streaming,
                    workers,
                    partition_by.as_deref(),
                    quiet,
                    checkpoint_dir,
                    checkpoint_interval,
                    credentials_store,
                    trace,
                )
                .await?;
            } else {
                commands::simulate::run_simulation(
                    &program,
                    &events,
                    timed,
                    verbose,
                    streaming,
                    workers,
                    partition_by.as_deref(),
                    quiet,
                    checkpoint_dir,
                    checkpoint_interval,
                    credentials_store,
                    trace,
                )
                .await?;
            }
        }

        Commands::ConfigGen { format, output } => {
            let content = match format.to_lowercase().as_str() {
                "yaml" | "yml" => Config::example_yaml(),
                "toml" => Config::example_toml(),
                _ => anyhow::bail!("Unsupported format: {format}. Use 'yaml' or 'toml'"),
            };

            if let Some(path) = output {
                std::fs::write(&path, &content)?;
                println!("Configuration written to: {}", path.display());
            } else {
                println!("{content}");
            }
        }

        Commands::Deploy {
            server,
            api_key,
            file,
            name,
        } => {
            let project = varpulis_cli::config::ProjectConfig::discover_cwd().unwrap_or_default();
            let server = project.resolve_url(server.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No server URL. Use --server, VARPULIS_SERVER env, or .varpulis.toml"
                )
            })?;
            let api_key = project.resolve_api_key(api_key.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No API key. Use --api-key, VARPULIS_API_KEY env, or .varpulis.toml"
                )
            })?;
            let name = name.or(project.deploy.name).unwrap_or_else(|| {
                file.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("pipeline")
                    .to_string()
            });

            let source = std::fs::read_to_string(&file)?;
            let client = VarpulisClient::new(&server, &api_key);
            match client.deploy_pipeline(&name, &source).await {
                Ok(resp) => {
                    output::success("Pipeline deployed successfully!");
                    println!("  ID:     {}", resp.id);
                    println!("  Name:   {}", resp.name);
                    println!("  Status: {}", resp.status);
                }
                Err(e) => {
                    output::error(&format!("Deploy failed: {e}"));
                    anyhow::bail!("Deploy failed: {e}");
                }
            }
        }

        Commands::Pipelines { server, api_key } => {
            let project = varpulis_cli::config::ProjectConfig::discover_cwd().unwrap_or_default();
            let server = project.resolve_url(server.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No server URL. Use --server, VARPULIS_SERVER env, or .varpulis.toml"
                )
            })?;
            let api_key = project.resolve_api_key(api_key.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No API key. Use --api-key, VARPULIS_API_KEY env, or .varpulis.toml"
                )
            })?;

            let client = VarpulisClient::new(&server, &api_key);
            match client.list_pipelines().await {
                Ok(resp) => {
                    println!("Pipelines ({} total):", resp.total);
                    if resp.pipelines.is_empty() {
                        println!("  (none)");
                    } else {
                        let mut table = Table::new();
                        table.load_preset(UTF8_FULL);
                        table.set_header(vec![
                            Cell::new("ID"),
                            Cell::new("Name"),
                            Cell::new("Status"),
                        ]);
                        for p in &resp.pipelines {
                            table.add_row(vec![
                                Cell::new(&p.id),
                                Cell::new(&p.name),
                                Cell::new(&p.status),
                            ]);
                        }
                        println!("{table}");
                    }
                }
                Err(e) => {
                    output::error(&format!("Failed to list pipelines: {e}"));
                    anyhow::bail!("Failed to list pipelines: {e}");
                }
            }
        }

        Commands::Undeploy {
            server,
            api_key,
            pipeline_id,
        } => {
            let project = varpulis_cli::config::ProjectConfig::discover_cwd().unwrap_or_default();
            let server = project.resolve_url(server.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No server URL. Use --server, VARPULIS_SERVER env, or .varpulis.toml"
                )
            })?;
            let api_key = project.resolve_api_key(api_key.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No API key. Use --api-key, VARPULIS_API_KEY env, or .varpulis.toml"
                )
            })?;

            let client = VarpulisClient::new(&server, &api_key);
            match client.delete_pipeline(&pipeline_id).await {
                Ok(()) => {
                    output::success(&format!("Pipeline {pipeline_id} deleted."));
                }
                Err(e) => {
                    output::error(&format!("Undeploy failed: {e}"));
                    anyhow::bail!("Undeploy failed: {e}");
                }
            }
        }

        Commands::Status { server, api_key } => {
            let project = varpulis_cli::config::ProjectConfig::discover_cwd().unwrap_or_default();
            let server = project.resolve_url(server.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No server URL. Use --server, VARPULIS_SERVER env, or .varpulis.toml"
                )
            })?;
            let api_key = project.resolve_api_key(api_key.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No API key. Use --api-key, VARPULIS_API_KEY env, or .varpulis.toml"
                )
            })?;

            let client = VarpulisClient::new(&server, &api_key);
            match client.get_usage().await {
                Ok(usage) => {
                    output::header(&format!("Status: {}", usage.tenant_id));
                    let mut table = Table::new();
                    table.load_preset(UTF8_FULL);
                    table.set_header(vec![Cell::new("Metric"), Cell::new("Value")]);
                    table.add_row(vec![
                        Cell::new("Events processed"),
                        Cell::new(usage.events_processed),
                    ]);
                    table.add_row(vec![
                        Cell::new("Output events emitted"),
                        Cell::new(usage.output_events_emitted),
                    ]);
                    table.add_row(vec![
                        Cell::new("Active pipelines"),
                        Cell::new(usage.active_pipelines),
                    ]);
                    table.add_row(vec![
                        Cell::new("Quota: max pipelines"),
                        Cell::new(usage.quota.max_pipelines),
                    ]);
                    table.add_row(vec![
                        Cell::new("Quota: max events/sec"),
                        Cell::new(usage.quota.max_events_per_second),
                    ]);
                    table.add_row(vec![
                        Cell::new("Quota: max streams/pipeline"),
                        Cell::new(usage.quota.max_streams_per_pipeline),
                    ]);
                    println!("{table}");
                }
                Err(e) => {
                    output::error(&format!("Failed to get status: {e}"));
                    anyhow::bail!("Failed to get status: {e}");
                }
            }
        }

        Commands::Init { server, api_key } => {
            let path = std::env::current_dir()?.join(".varpulis.toml");
            if path.exists() {
                anyhow::bail!(".varpulis.toml already exists in current directory");
            }

            let mut content = String::from("# Varpulis project configuration\n\n[remote]\n");
            if let Some(url) = server {
                content.push_str(&format!("url = \"{url}\"\n"));
            } else {
                content.push_str("url = \"http://localhost:9000\"\n");
            }
            if let Some(key) = api_key {
                content.push_str(&format!("api_key = \"{key}\"\n"));
            } else {
                content.push_str("# api_key = \"your-api-key-here\"\n");
            }
            content.push_str("\n[deploy]\n# name = \"my-pipeline\"\n");

            std::fs::write(&path, &content)?;
            println!("Created {}", path.display());
        }

        Commands::Logs {
            server,
            api_key,
            pipeline_id,
        } => {
            let project = varpulis_cli::config::ProjectConfig::discover_cwd().unwrap_or_default();
            let server = project.resolve_url(server.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No server URL. Use --server, VARPULIS_SERVER env, or .varpulis.toml"
                )
            })?;
            let api_key = project.resolve_api_key(api_key.as_deref()).ok_or_else(|| {
                anyhow::anyhow!(
                    "No API key. Use --api-key, VARPULIS_API_KEY env, or .varpulis.toml"
                )
            })?;

            let client = VarpulisClient::new(&server, &api_key);
            let url = client.logs_url(&pipeline_id);
            println!("Streaming logs for pipeline {pipeline_id}...");
            println!("(Press Ctrl+C to stop)\n");

            // Connect to SSE endpoint
            let resp = reqwest::Client::new()
                .get(&url)
                .header("x-api-key", client.api_key())
                .send()
                .await?;

            if !resp.status().is_success() {
                let status = resp.status().as_u16();
                let text = resp.text().await.unwrap_or_default();
                anyhow::bail!("Failed to connect to log stream ({status}): {text}");
            }

            // Stream SSE events line by line
            let mut stream = resp.bytes_stream();
            use futures_util::StreamExt;
            let mut buffer = String::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                buffer.push_str(&String::from_utf8_lossy(&chunk));

                // Process complete SSE events (double newline separated)
                while let Some(pos) = buffer.find("\n\n") {
                    let event_block = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();

                    for line in event_block.lines() {
                        if let Some(data) = line.strip_prefix("data:") {
                            let data = data.trim();
                            // Pretty-print the JSON event
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(data) {
                                if let Some(event_type) = parsed.get("event_type") {
                                    let ts = parsed
                                        .get("timestamp")
                                        .and_then(|t| t.as_str())
                                        .unwrap_or("?");
                                    print!("[{ts}] {event_type} ");
                                    if let Some(fields) = parsed.get("data") {
                                        println!("{fields}");
                                    } else {
                                        println!();
                                    }
                                } else {
                                    println!("{data}");
                                }
                            } else {
                                println!("{data}");
                            }
                        }
                    }
                }
            }
        }

        Commands::Generate {
            schema,
            rate,
            duration,
            anomaly_rate,
            seed,
            format,
        } => {
            commands::generate::run_generate(schema, rate, &duration, anomaly_rate, seed, &format)
                .await?;
        }

        Commands::Federation {
            action,
            coordinator,
            api_key,
        } => {
            commands::federation::run_federation(action, &coordinator, api_key.as_deref()).await?;
        }

        Commands::Coordinator {
            port,
            bind,
            api_key,
            api_keys,
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
            llm_endpoint,
            llm_model,
            llm_api_key,
            llm_provider,
            tls_cert,
            tls_key,
            tls_ca_cert,
            nats,
            rate_limit,
            cors_origins,
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
            // Build RBAC config: --api-keys file takes priority over --api-key
            let rbac_config = if let Some(ref keys_path) = api_keys {
                varpulis_cluster::RbacConfig::from_file(keys_path)
                    .map_err(|e| anyhow::anyhow!("{e}"))?
            } else if let Some(ref key) = api_key {
                varpulis_cluster::RbacConfig::single_key(key.clone())
            } else {
                varpulis_cluster::RbacConfig::disabled()
            };
            // Allow coordinator to validate JWTs issued by the worker's auth system
            let rbac_config = if let Ok(jwt_secret) = std::env::var("JWT_SECRET") {
                rbac_config.with_jwt_secret(jwt_secret)
            } else {
                rbac_config
            };
            let rbac_config = std::sync::Arc::new(rbac_config);

            // Validate TLS config: cert and key must come together
            let coordinator_tls = match (tls_cert, tls_key) {
                (Some(cert), Some(key)) => Some((cert, key)),
                (None, None) => None,
                _ => anyhow::bail!(
                    "Both --tls-cert and --tls-key must be provided together for coordinator TLS"
                ),
            };

            commands::coordinator::run_coordinator(
                port,
                &bind,
                rbac_config,
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
                llm_endpoint,
                llm_model,
                llm_api_key,
                llm_provider,
                coordinator_tls,
                tls_ca_cert,
                nats,
                rate_limit,
                cors_origins,
            )
            .await?;
        }

        Commands::Infer {
            input,
            output,
            sample_size,
        } => {
            commands::infer::run_infer(&input, output.as_deref(), sample_size)?;
        }

        Commands::EncryptCredentials { input, output } => {
            use varpulis_connectors::credentials;

            let master_key = credentials::load_master_key()?
                .ok_or_else(|| anyhow::anyhow!(
                    "Master key required. Set VARPULIS_MASTER_KEY (hex) or VARPULIS_MASTER_KEY_FILE."
                ))?;

            let contents = std::fs::read_to_string(&input)?;
            let mut creds: credentials::CredentialsFile = serde_yaml::from_str(&contents)
                .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", input.display(), e))?;

            #[allow(unused_mut)]
            let mut encrypted_count = 0u32;
            for (profile_name, profile) in &mut creds.profiles {
                for (key, value) in &mut profile.properties {
                    if credentials::is_sensitive_field(key) && !credentials::is_encrypted(value) {
                        #[cfg(feature = "encryption")]
                        {
                            *value = credentials::encrypt_value(value, &master_key)?;
                            encrypted_count += 1;
                            println!("  Encrypted: {profile_name}.{key}");
                        }
                        #[cfg(not(feature = "encryption"))]
                        {
                            let _ = (master_key, &encrypted_count, &profile_name, &key);
                            anyhow::bail!(
                                "Encryption feature not enabled. Build with --features encryption"
                            );
                        }
                    }
                }
            }

            let output_path = output.as_ref().unwrap_or(&input);
            let yaml = serde_yaml::to_string(&creds)?;
            std::fs::write(output_path, &yaml)?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(output_path, std::fs::Permissions::from_mode(0o600))?;
            }

            if encrypted_count > 0 {
                println!(
                    "\nEncrypted {} sensitive field(s) in {}",
                    encrypted_count,
                    output_path.display()
                );
            } else {
                println!("No unencrypted sensitive fields found.");
            }
        }

        Commands::GenerateMasterKey => {
            use std::fmt::Write;
            let mut key = [0u8; 32];
            getrandom::fill(&mut key)
                .map_err(|e| anyhow::anyhow!("Failed to generate random key: {}", e))?;
            let mut hex = String::with_capacity(64);
            for byte in &key {
                write!(hex, "{:02x}", byte)?;
            }
            println!("{}", hex);
        }

        Commands::Connector { action } => {
            commands::connector::run_connector(action)?;
        }

        #[cfg(feature = "repl")]
        Commands::Repl { file } => {
            commands::repl::run_repl(file.as_deref())?;
        }

        Commands::Interactive {
            json,
            tui,
            file,
            generate,
            rate,
            trace,
        } => {
            if json {
                // Agent mode: structured JSON-line protocol
                commands::interactive::jsonl::run_jsonl_session(
                    file.as_deref(),
                    generate.as_deref(),
                    rate,
                    trace,
                )
                .await?;
            } else if tui {
                // TUI mode: split-pane terminal UI
                commands::interactive::tui::run_tui_session(
                    file.as_deref(),
                    generate.as_deref(),
                    rate,
                    trace,
                )
                .await?;
            } else {
                // Default: Python-interpreter-style shell
                commands::interactive::shell::run_shell(
                    file.as_deref(),
                    generate.as_deref(),
                    rate,
                    trace,
                )?;
            }
        }
    }

    Ok(())
}
