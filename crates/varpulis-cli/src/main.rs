//! Varpulis CLI - Command line interface for Varpulis streaming analytics engine

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use warp::ws::{Message, WebSocket};
use warp::Filter;

use varpulis_parser::parse;
use varpulis_runtime::engine::{Alert, Engine};
use varpulis_runtime::metrics::{Metrics, MetricsServer};
use varpulis_runtime::simulator::{SimulatorConfig, Simulator};

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
            let source = if let Some(path) = file {
                std::fs::read_to_string(&path)?
            } else if let Some(code) = code {
                code
            } else {
                anyhow::bail!("Either --file or --code must be provided");
            };

            run_program(&source).await?;
        }

        Commands::Parse { file } => {
            let source = std::fs::read_to_string(&file)?;
            parse_and_show(&source)?;
        }

        Commands::Demo { duration, anomalies, degradation, metrics, metrics_port } => {
            run_demo(duration, anomalies, degradation, metrics, metrics_port).await?;
        }

        Commands::Check { file } => {
            let source = std::fs::read_to_string(&file)?;
            check_syntax(&source)?;
        }

        Commands::Server { port, metrics, metrics_port } => {
            run_server(port, metrics, metrics_port).await?;
        }
    }

    Ok(())
}

async fn run_program(source: &str) -> Result<()> {
    info!("Parsing VarpulisQL program...");
    let program = parse(source).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    info!("Parsed {} statements", program.statements.len());

    // Create alert channel
    let (alert_tx, mut alert_rx) = mpsc::channel::<Alert>(100);

    // Create engine
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).map_err(|e| anyhow::anyhow!("Load error: {}", e))?;

    // Spawn alert handler
    tokio::spawn(async move {
        while let Some(alert) = alert_rx.recv().await {
            println!("\n📢 ALERT: {} ({})", alert.alert_type, alert.severity);
            println!("   Message: {}", alert.message);
            for (key, value) in &alert.data {
                println!("   {}: {}", key, value);
            }
        }
    });

    info!("Engine ready. Waiting for events...");
    
    // For now, just show that we parsed successfully
    let metrics = engine.metrics();
    println!("\n✅ Program loaded successfully!");
    println!("   Streams registered: {}", metrics.streams_count);

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
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn run_demo(duration_secs: u64, anomalies: bool, degradation: bool, enable_metrics: bool, metrics_port: u16) -> Result<()> {
    println!("🏢 Varpulis HVAC Building Demo");
    println!("================================");
    println!("Duration: {} seconds", duration_secs);
    println!("Anomalies: {}", if anomalies { "enabled" } else { "disabled" });
    println!("Degradation: {}", if degradation { "enabled" } else { "disabled" });
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
    println!("Average rate: {:.0} events/sec", event_count as f64 / start.elapsed().as_secs_f64());

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
    LoadFile { path: String },
    InjectEvent { event_type: String, data: serde_json::Value },
    GetStreams,
    GetMetrics,
    
    // Server -> Client
    LoadResult { success: bool, streams_loaded: usize, error: Option<String> },
    Streams { data: Vec<StreamInfoMsg> },
    Event { id: String, event_type: String, timestamp: String, data: serde_json::Value },
    Alert { id: String, alert_type: String, severity: String, message: String, timestamp: String, data: serde_json::Value },
    Metrics { events_processed: u64, alerts_generated: u64, active_streams: usize, uptime: f64, memory_usage: u64, cpu_usage: f64 },
    Error { message: String },
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
}

async fn run_server(port: u16, enable_metrics: bool, metrics_port: u16) -> Result<()> {
    println!("🚀 Varpulis Server");
    println!("==================");
    println!("WebSocket: ws://127.0.0.1:{}/ws", port);
    if enable_metrics {
        println!("Metrics:   http://127.0.0.1:{}/metrics", metrics_port);
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
    }));

    // Create metrics if enabled
    let prom_metrics = if enable_metrics {
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
        .map(|ws: warp::ws::Ws, state: Arc<RwLock<ServerState>>, broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>| {
            ws.on_upgrade(move |socket| handle_websocket(socket, state, broadcast_tx))
        });

    // Health check route
    let health_route = warp::path("health")
        .map(|| warp::reply::json(&serde_json::json!({"status": "ok"})));

    let routes = ws_route.or(health_route);

    info!("Server listening on 0.0.0.0:{}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;

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
            match std::fs::read_to_string(&path) {
                Ok(source) => {
                    match parse(&source) {
                        Ok(program) => {
                            let mut state = state.write().await;
                            let (alert_tx, _) = mpsc::channel::<Alert>(100);
                            let mut engine = Engine::new(alert_tx);
                            
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
                                }
                            }
                        }
                        Err(e) => WsMessage::LoadResult {
                            success: false,
                            streams_loaded: 0,
                            error: Some(format!("Parse error: {}", e)),
                        }
                    }
                }
                Err(e) => WsMessage::LoadResult {
                    success: false,
                    streams_loaded: 0,
                    error: Some(format!("Failed to read file: {}", e)),
                }
            }
        }

        WsMessage::GetStreams => {
            let state = state.read().await;
            WsMessage::Streams { data: state.streams.clone() }
        }

        WsMessage::GetMetrics => {
            let state = state.read().await;
            let (events_processed, alerts_generated, active_streams) = if let Some(ref engine) = state.engine {
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
            // TODO: implement event injection
            WsMessage::Error {
                message: format!("Event injection not yet implemented: {}", event_type),
            }
        }

        _ => WsMessage::Error {
            message: "Unknown message type".to_string(),
        }
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{:x}{:x}", duration.as_secs(), duration.subsec_nanos())
}
