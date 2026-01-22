//! Varpulis CLI - Command line interface for Varpulis streaming analytics engine

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use varpulis_parser::parse;
use varpulis_runtime::engine::{Alert, Engine};
use varpulis_runtime::simulator::{create_default_simulator, SimulatorConfig, Simulator};

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
    },

    /// Check syntax of a VarpulisQL file
    Check {
        /// Path to the .vpl file
        file: PathBuf,
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

        Commands::Demo { duration, anomalies, degradation } => {
            run_demo(duration, anomalies, degradation).await?;
        }

        Commands::Check { file } => {
            let source = std::fs::read_to_string(&file)?;
            check_syntax(&source)?;
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

async fn run_demo(duration_secs: u64, anomalies: bool, degradation: bool) -> Result<()> {
    println!("🏢 Varpulis HVAC Building Demo");
    println!("================================");
    println!("Duration: {} seconds", duration_secs);
    println!("Anomalies: {}", if anomalies { "enabled" } else { "disabled" });
    println!("Degradation: {}", if degradation { "enabled" } else { "disabled" });
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
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).map_err(|e| anyhow::anyhow!(e))?;

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
