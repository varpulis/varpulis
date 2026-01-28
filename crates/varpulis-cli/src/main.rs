//! Varpulis CLI - Command line interface for Varpulis streaming analytics engine

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use warp::ws::{Message, WebSocket};
use warp::Filter;

use varpulis_core::ast::{Program, Stmt};
use varpulis_parser::parse;
use varpulis_runtime::engine::{Alert, Engine};
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::{EventFileParser, EventFilePlayer};
use varpulis_runtime::metrics::{Metrics, MetricsServer};
use varpulis_runtime::simulator::{Simulator, SimulatorConfig};

#[derive(Parser)]
#[command(name = "varpulis")]
#[command(author = "Varpulis Contributors")]
#[command(version = "0.1.0")]
#[command(about = "Varpulis - Modern streaming analytics engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a VarpulisQL program
    Run {
        /// Path to the .vpl file
        #[arg(short, long)]
        file: Option<PathBuf>,

        /// Inline VarpulisQL code
        #[arg(short, long)]
        code: Option<String>,
    },

    /// Parse a VarpulisQL file and show the AST
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

    /// Check syntax of a VarpulisQL file
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
    },

    /// Simulate events from an event file (.evt)
    Simulate {
        /// Path to the VarpulisQL program (.vpl)
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
        } => {
            let workdir = workdir
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
            run_server(port, metrics, metrics_port, &bind, workdir).await?;
        }

        Commands::Simulate {
            program,
            events,
            immediate,
            verbose,
        } => {
            run_simulation(&program, &events, immediate, verbose).await?;
        }
    }

    Ok(())
}

async fn run_program(source: &str, base_path: Option<&PathBuf>) -> Result<()> {
    use varpulis_runtime::connector::{
        MqttConfig, MqttSink, MqttSource, SinkConnector, SourceConnector,
    };

    info!("Parsing VarpulisQL program...");
    let mut program = parse(source).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    info!("Parsed {} statements", program.statements.len());

    // Resolve imports
    resolve_imports(&mut program, base_path)?;

    // Create alert channel
    let (alert_tx, mut alert_rx) = mpsc::channel::<Alert>(100);

    // Create engine
    let mut engine = Engine::new(alert_tx);
    engine
        .load(&program)
        .map_err(|e| anyhow::anyhow!("Load error: {}", e))?;

    let metrics = engine.metrics();
    println!("\n✅ Program loaded successfully!");
    println!("   Streams registered: {}", metrics.streams_count);

    // Check for MQTT config
    let mqtt_config = engine.get_config("mqtt");

    if let Some(config) = mqtt_config {
        let broker = config
            .values
            .get("broker")
            .and_then(|v| v.as_string())
            .unwrap_or("localhost");
        let port: u16 = config
            .values
            .get("port")
            .and_then(|v| v.as_int())
            .map(|i| i as u16)
            .unwrap_or(1883);
        let input_topic = config
            .values
            .get("input_topic")
            .and_then(|v| v.as_string())
            .unwrap_or("varpulis/events/#");
        let output_topic = config
            .values
            .get("output_topic")
            .and_then(|v| v.as_string())
            .unwrap_or("varpulis/alerts");
        let client_id = config
            .values
            .get("client_id")
            .and_then(|v| v.as_string())
            .unwrap_or("varpulis-engine");

        println!("\n🔌 MQTT Configuration:");
        println!("   Broker: {}:{}", broker, port);
        println!("   Input:  {}", input_topic);
        println!("   Output: {}", output_topic);

        // Create MQTT source
        let mqtt_source_config = MqttConfig::new(broker, input_topic)
            .with_port(port)
            .with_client_id(&format!("{}-source", client_id));
        let mut mqtt_source = MqttSource::new("mqtt-source", mqtt_source_config);

        // Create MQTT sink for alerts
        let mqtt_sink_config = MqttConfig::new(broker, output_topic)
            .with_port(port)
            .with_client_id(&format!("{}-sink", client_id));
        let mut mqtt_sink = MqttSink::new("mqtt-sink", mqtt_sink_config);
        mqtt_sink
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("MQTT sink connection error: {}", e))?;
        let mqtt_sink = Arc::new(mqtt_sink);

        // Create event channel
        let (event_tx, mut event_rx) = mpsc::channel::<Event>(1000);

        // Spawn alert handler that publishes to MQTT
        let sink_clone = mqtt_sink.clone();
        tokio::spawn(async move {
            while let Some(alert) = alert_rx.recv().await {
                println!("📢 ALERT: {} - {}", alert.alert_type, alert.message);

                // Publish alert to MQTT with full data
                let mut alert_event = Event::new(&alert.alert_type);
                alert_event.data.insert(
                    "message".to_string(),
                    varpulis_core::Value::Str(alert.message.clone()),
                );
                alert_event.data.insert(
                    "severity".to_string(),
                    varpulis_core::Value::Str(alert.severity.clone()),
                );
                for (key, value) in &alert.data {
                    alert_event.data.insert(key.clone(), value.clone());
                }
                match sink_clone.as_ref().send(&alert_event).await {
                    Ok(_) => tracing::debug!("Published alert to MQTT: {}", alert.alert_type),
                    Err(e) => tracing::warn!("Failed to publish alert to MQTT: {}", e),
                }
            }
        });

        // Start MQTT source
        println!("\n🚀 Starting MQTT source...");
        mqtt_source
            .start(event_tx)
            .await
            .map_err(|e| anyhow::anyhow!("MQTT source error: {}", e))?;

        println!("📡 Listening for events on {}...\n", input_topic);
        println!("   Press Ctrl+C to stop\n");

        let start = std::time::Instant::now();
        let mut last_report = std::time::Instant::now();
        let mut event_count = 0u64;

        // Process events from MQTT
        loop {
            tokio::select! {
                Some(event) = event_rx.recv() => {
                    event_count += 1;

                    // Process through engine
                    if let Err(e) = engine.process(event.clone()).await {
                        tracing::warn!("Engine error: {}", e);
                    }

                    // Progress report every 2 seconds
                    if last_report.elapsed() >= std::time::Duration::from_secs(2) {
                        let metrics = engine.metrics();
                        print!("\r📊 Events: {} | Alerts: {} | Rate: {:.0}/s    ",
                            metrics.events_processed,
                            metrics.alerts_generated,
                            event_count as f64 / start.elapsed().as_secs_f64()
                        );
                        std::io::Write::flush(&mut std::io::stdout())?;
                        last_report = std::time::Instant::now();
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\n\n⏹️  Stopping...");
                    mqtt_source.stop().await.ok();
                    break;
                }
            }
        }

        // Final stats
        let metrics = engine.metrics();
        println!("\n📈 Final Statistics:");
        println!("   Events processed: {}", metrics.events_processed);
        println!("   Alerts generated: {}", metrics.alerts_generated);
        println!("   Runtime: {:.1}s", start.elapsed().as_secs_f64());
    } else {
        // No MQTT config - just show that we loaded successfully
        println!("\n⚠️  No 'config mqtt' block found in program.");
        println!("   Add a config block to connect to MQTT:");
        println!("   ");
        println!("   config mqtt {{");
        println!("       broker: \"localhost\",");
        println!("       port: 1883,");
        println!("       input_topic: \"varpulis/events/#\",");
        println!("       output_topic: \"varpulis/alerts\"");
        println!("   }}");

        // Spawn alert handler anyway for demo mode
        tokio::spawn(async move {
            while let Some(alert) = alert_rx.recv().await {
                println!("\n📢 ALERT: {} ({})", alert.alert_type, alert.severity);
                println!("   Message: {}", alert.message);
                for (key, value) in &alert.data {
                    println!("   {}: {}", key, value);
                }
            }
        });
    }

    Ok(())
}

fn parse_and_show(source: &str) -> Result<()> {
    println!("Parsing VarpulisQL...\n");

    match parse(source) {
        Ok(program) => {
            println!("✅ Parse successful!\n");
            println!("AST:");
            println!("{:#?}", program);
        }
        Err(e) => {
            println!("❌ Parse error: {}", e);
        }
    }

    Ok(())
}

fn check_syntax(source: &str) -> Result<()> {
    match parse(source) {
        Ok(program) => {
            println!("✅ Syntax OK");
            println!("   Statements: {}", program.statements.len());
        }
        Err(e) => {
            println!("❌ Syntax error: {}", e);

            // Show context around the error if we have a Located error
            if let varpulis_parser::ParseError::Located {
                line, column, hint, ..
            } = &e
            {
                // Show hint if available
                if let Some(h) = hint {
                    println!("   💡 Hint: {}", h);
                }

                // Show the problematic line from source
                if let Some(error_line) = source.lines().nth(line - 1) {
                    println!("   │");
                    println!("   │ {}", error_line);
                    println!("   │ {}^", " ".repeat(column.saturating_sub(1)));
                }
            }

            std::process::exit(1);
        }
    }
    Ok(())
}

async fn run_simulation(
    program_path: &PathBuf,
    events_path: &PathBuf,
    immediate: bool,
    verbose: bool,
) -> Result<()> {
    println!("🎬 Varpulis Event Simulation");
    println!("============================");
    println!("Program: {}", program_path.display());
    println!("Events:  {}", events_path.display());
    println!("Mode:    {}", if immediate { "immediate" } else { "timed" });
    println!();

    // Load and parse program
    let program_source = std::fs::read_to_string(program_path)?;
    let program = parse(&program_source).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    info!(
        "Loaded program with {} statements",
        program.statements.len()
    );

    // Load and parse events
    let events_source = std::fs::read_to_string(events_path)?;
    let events = EventFileParser::parse(&events_source)
        .map_err(|e| anyhow::anyhow!("Event file error: {}", e))?;
    info!("Loaded {} events from file", events.len());

    // Create alert channel
    let (alert_tx, mut alert_rx) = mpsc::channel::<Alert>(100);

    // Create and load engine
    let mut engine = Engine::new(alert_tx);
    engine
        .load(&program)
        .map_err(|e| anyhow::anyhow!("Load error: {}", e))?;

    let metrics = engine.metrics();
    println!("✅ Program loaded: {} streams", metrics.streams_count);
    println!();

    // Collect alerts
    let alerts = Arc::new(RwLock::new(Vec::<Alert>::new()));
    let alerts_clone = alerts.clone();

    tokio::spawn(async move {
        while let Some(alert) = alert_rx.recv().await {
            println!("📢 ALERT: {} - {}", alert.alert_type, alert.message);
            for (k, v) in &alert.data {
                println!("   {}: {}", k, v);
            }
            alerts_clone.write().await.push(alert);
        }
    });

    println!("🚀 Starting simulation...\n");
    let start = std::time::Instant::now();

    // Process events
    if immediate {
        // Immediate mode - no timing
        for (i, timed_event) in events.iter().enumerate() {
            if verbose {
                println!("  [{:3}] {} {{ ... }}", i + 1, timed_event.event.event_type);
            }
            engine
                .process(timed_event.event.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Process error: {}", e))?;
        }
    } else {
        // Timed mode - respect BATCH delays
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

    // Wait a bit for any pending alerts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Summary
    let elapsed = start.elapsed();
    let final_metrics = engine.metrics();
    let alerts_count = alerts.read().await.len();

    println!("\n📊 Simulation Complete");
    println!("======================");
    println!("Duration:         {:?}", elapsed);
    println!("Events processed: {}", final_metrics.events_processed);
    println!("Alerts generated: {}", alerts_count);
    println!(
        "Event rate:       {:.1} events/sec",
        final_metrics.events_processed as f64 / elapsed.as_secs_f64()
    );

    if alerts_count > 0 {
        println!("\n📢 Alerts Summary:");
        for alert in alerts.read().await.iter() {
            println!("  - {}: {}", alert.alert_type, alert.message);
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
    println!("🏢 Varpulis HVAC Building Demo");
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

    // Create alert channel
    let (alert_tx, mut alert_rx) = mpsc::channel::<Alert>(100);

    // Create engine with a simple stream
    let demo_program = r#"
        stream TemperatureReadings from TemperatureReading
        stream HumidityReadings from HumidityReading  
        stream HVACStatuses from HVACStatus
    "#;

    let program = parse(demo_program).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;

    // Create prometheus metrics if enabled
    let prom_metrics = if enable_metrics {
        Some(Metrics::new())
    } else {
        None
    };

    let mut engine = Engine::new(alert_tx);
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

    // Spawn alert handler
    tokio::spawn(async move {
        while let Some(alert) = alert_rx.recv().await {
            println!("📢 {}: {}", alert.alert_type, alert.message);
        }
    });

    // Process events
    let start = std::time::Instant::now();
    let duration = std::time::Duration::from_secs(duration_secs);
    let mut event_count = 0u64;
    let mut last_report = std::time::Instant::now();

    println!("🚀 Starting event processing...\n");

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
                    print!("\r📊 Events: {} | Alerts: {} | Rate: {:.0}/s    ",
                        metrics.events_processed,
                        metrics.alerts_generated,
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
    println!("\n\n📈 Demo Complete!");
    println!("================");
    let metrics = engine.metrics();
    println!("Total events processed: {}", metrics.events_processed);
    println!("Total alerts generated: {}", metrics.alerts_generated);
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsMessage {
    // Client -> Server
    LoadFile {
        path: String,
    },
    InjectEvent {
        event_type: String,
        data: serde_json::Value,
    },
    GetStreams,
    GetMetrics,

    // Server -> Client
    LoadResult {
        success: bool,
        streams_loaded: usize,
        error: Option<String>,
    },
    Streams {
        data: Vec<StreamInfoMsg>,
    },
    Event {
        id: String,
        event_type: String,
        timestamp: String,
        data: serde_json::Value,
    },
    Alert {
        id: String,
        alert_type: String,
        severity: String,
        message: String,
        timestamp: String,
        data: serde_json::Value,
    },
    Metrics {
        events_processed: u64,
        alerts_generated: u64,
        active_streams: usize,
        uptime: f64,
        memory_usage: u64,
        cpu_usage: f64,
    },
    EventInjected {
        event_type: String,
        success: bool,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamInfoMsg {
    name: String,
    source: String,
    operations: Vec<String>,
    events_per_second: f64,
    status: String,
}

struct ServerState {
    engine: Option<Engine>,
    streams: Vec<StreamInfoMsg>,
    start_time: std::time::Instant,
    alert_tx: mpsc::Sender<Alert>,
    /// Allowed working directory for file operations (prevents path traversal)
    workdir: PathBuf,
}

/// Validate that a path is within the allowed working directory.
/// Returns the canonical path if valid, or an error message if path traversal detected.
fn validate_path(path: &str, workdir: &std::path::Path) -> Result<PathBuf, String> {
    let requested = PathBuf::from(path);

    // Resolve to absolute path
    let absolute = if requested.is_absolute() {
        requested
    } else {
        workdir.join(&requested)
    };

    // Canonicalize to resolve .. and symlinks
    let canonical = absolute.canonicalize().map_err(|e| {
        format!("Invalid path: {}", e)
    })?;

    // Ensure the canonical path starts with workdir
    let workdir_canonical = workdir.canonicalize().map_err(|e| {
        format!("Invalid workdir: {}", e)
    })?;

    if !canonical.starts_with(&workdir_canonical) {
        return Err("Access denied: path outside allowed directory".to_string());
    }

    Ok(canonical)
}

async fn run_server(port: u16, enable_metrics: bool, metrics_port: u16, bind: &str, workdir: PathBuf) -> Result<()> {
    // Canonicalize workdir early to ensure it's valid
    let workdir = workdir.canonicalize().map_err(|e| {
        anyhow::anyhow!("Invalid workdir '{}': {}", workdir.display(), e)
    })?;

    println!("🚀 Varpulis Server");
    println!("==================");
    println!("WebSocket: ws://{}:{}/ws", bind, port);
    println!("Workdir:   {}", workdir.display());
    if enable_metrics {
        println!("Metrics:   http://{}:{}/metrics", bind, metrics_port);
    }
    println!();

    // Create alert channel
    let (alert_tx, mut alert_rx) = mpsc::channel::<Alert>(100);

    // Create shared state
    let state = Arc::new(RwLock::new(ServerState {
        engine: None,
        streams: Vec::new(),
        start_time: std::time::Instant::now(),
        alert_tx: alert_tx.clone(),
        workdir: workdir.clone(),
    }));

    // Create metrics if enabled
    let _prom_metrics = if enable_metrics {
        let metrics = Metrics::new();
        let server = MetricsServer::new(metrics.clone(), format!("127.0.0.1:{}", metrics_port));
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                tracing::error!("Metrics server error: {}", e);
            }
        });
        Some(metrics)
    } else {
        None
    };

    // Broadcast channel for alerts to all connected clients
    let (broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(100);
    let broadcast_tx = Arc::new(broadcast_tx);

    // Spawn alert forwarder
    let broadcast_tx_clone = broadcast_tx.clone();
    tokio::spawn(async move {
        while let Some(alert) = alert_rx.recv().await {
            let msg = WsMessage::Alert {
                id: uuid_simple(),
                alert_type: alert.alert_type,
                severity: alert.severity,
                message: alert.message,
                timestamp: chrono::Utc::now().to_rfc3339(),
                data: serde_json::to_value(&alert.data).unwrap_or_default(),
            };
            if let Ok(json) = serde_json::to_string(&msg) {
                let _ = broadcast_tx_clone.send(json);
            }
        }
    });

    // WebSocket route
    let state_filter = warp::any().map({
        let state = state.clone();
        move || state.clone()
    });

    let broadcast_filter = warp::any().map({
        let broadcast_tx = broadcast_tx.clone();
        move || broadcast_tx.clone()
    });

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(state_filter)
        .and(broadcast_filter)
        .map(
            |ws: warp::ws::Ws,
             state: Arc<RwLock<ServerState>>,
             broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>| {
                ws.on_upgrade(move |socket| handle_websocket(socket, state, broadcast_tx))
            },
        );

    // Health check route
    let health_route =
        warp::path("health").map(|| warp::reply::json(&serde_json::json!({"status": "ok"})));

    let routes = ws_route.or(health_route);

    // Parse bind address
    let bind_addr: std::net::IpAddr = bind.parse().map_err(|e| {
        anyhow::anyhow!("Invalid bind address '{}': {}", bind, e)
    })?;

    info!("Server listening on {}:{}", bind, port);
    warp::serve(routes).run((bind_addr, port)).await;

    Ok(())
}

async fn handle_websocket(
    ws: WebSocket,
    state: Arc<RwLock<ServerState>>,
    broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>,
) {
    let (ws_tx, mut ws_rx) = ws.split();
    let ws_tx = Arc::new(tokio::sync::Mutex::new(ws_tx));
    let mut broadcast_rx = broadcast_tx.subscribe();

    // Spawn task to forward broadcasts to this client
    let ws_tx_clone = ws_tx.clone();
    let forward_task = tokio::spawn(async move {
        while let Ok(msg) = broadcast_rx.recv().await {
            let mut tx = ws_tx_clone.lock().await;
            if tx.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Handle incoming messages
    while let Some(result) = ws_rx.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!("WebSocket error: {}", e);
                break;
            }
        };

        if msg.is_text() {
            let text = msg.to_str().unwrap_or("");
            if let Ok(ws_msg) = serde_json::from_str::<WsMessage>(text) {
                let response = handle_ws_message(ws_msg, &state).await;
                if let Ok(json) = serde_json::to_string(&response) {
                    let mut tx = ws_tx.lock().await;
                    if tx.send(Message::text(json)).await.is_err() {
                        break;
                    }
                }
            }
        } else if msg.is_close() {
            break;
        }
    }

    forward_task.abort();
    tracing::info!("WebSocket client disconnected");
}

async fn handle_ws_message(msg: WsMessage, state: &Arc<RwLock<ServerState>>) -> WsMessage {
    match msg {
        WsMessage::LoadFile { path } => {
            // Validate path to prevent path traversal attacks
            let workdir = {
                let state = state.read().await;
                state.workdir.clone()
            };

            let validated_path = match validate_path(&path, &workdir) {
                Ok(p) => p,
                Err(e) => {
                    return WsMessage::LoadResult {
                        success: false,
                        streams_loaded: 0,
                        error: Some(e),
                    };
                }
            };

            match std::fs::read_to_string(&validated_path) {
                Ok(source) => {
                    match parse(&source) {
                        Ok(program) => {
                            let mut state = state.write().await;
                            let engine_alert_tx = state.alert_tx.clone();
                            let mut engine = Engine::new(engine_alert_tx);

                            match engine.load(&program) {
                                Ok(()) => {
                                    let streams_count = engine.metrics().streams_count;
                                    state.engine = Some(engine);
                                    state.streams = vec![]; // TODO: populate from engine
                                    WsMessage::LoadResult {
                                        success: true,
                                        streams_loaded: streams_count,
                                        error: None,
                                    }
                                }
                                Err(e) => WsMessage::LoadResult {
                                    success: false,
                                    streams_loaded: 0,
                                    error: Some(e.to_string()),
                                },
                            }
                        }
                        Err(e) => WsMessage::LoadResult {
                            success: false,
                            streams_loaded: 0,
                            error: Some(format!("Parse error: {}", e)),
                        },
                    }
                }
                Err(_) => WsMessage::LoadResult {
                    success: false,
                    streams_loaded: 0,
                    // Generic error message to avoid information disclosure
                    error: Some("Failed to read file".to_string()),
                },
            }
        }

        WsMessage::GetStreams => {
            let state = state.read().await;
            WsMessage::Streams {
                data: state.streams.clone(),
            }
        }

        WsMessage::GetMetrics => {
            let state = state.read().await;
            let (events_processed, alerts_generated, active_streams) =
                if let Some(ref engine) = state.engine {
                    let m = engine.metrics();
                    (m.events_processed, m.alerts_generated, m.streams_count)
                } else {
                    (0, 0, 0)
                };

            WsMessage::Metrics {
                events_processed,
                alerts_generated,
                active_streams,
                uptime: state.start_time.elapsed().as_secs_f64(),
                memory_usage: 0, // TODO: implement
                cpu_usage: 0.0,  // TODO: implement
            }
        }

        WsMessage::InjectEvent { event_type, data } => {
            let mut state = state.write().await;
            if let Some(ref mut engine) = state.engine {
                // Create event from injected data
                let mut event = Event::new(&event_type);

                // Convert JSON data to event fields
                if let Some(obj) = data.as_object() {
                    for (key, value) in obj {
                        let v = json_to_value(value);
                        event.data.insert(key.clone(), v);
                    }
                }

                // Process the event
                match engine.process(event).await {
                    Ok(()) => WsMessage::EventInjected {
                        event_type: event_type.clone(),
                        success: true,
                    },
                    Err(e) => WsMessage::Error {
                        message: format!("Failed to process event: {}", e),
                    },
                }
            } else {
                WsMessage::Error {
                    message: "No engine loaded. Load a .vpl file first.".to_string(),
                }
            }
        }

        _ => WsMessage::Error {
            message: "Unknown message type".to_string(),
        },
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{:x}{:x}", duration.as_secs(), duration.subsec_nanos())
}

/// Convert a serde_json::Value to varpulis_core::Value
fn json_to_value(json: &serde_json::Value) -> varpulis_core::Value {
    use varpulis_core::Value;
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => {
            let map = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect();
            Value::Map(map)
        }
    }
}

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
    #[allow(unused_imports)]
    use varpulis_core::span::{Span, Spanned};

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
            info!("Skipping already imported file: {}", canonical_path.display());
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
