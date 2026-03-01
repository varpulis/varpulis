use anyhow::Result;
use tokio::sync::mpsc;

use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::metrics::{Metrics, MetricsServer};
use varpulis_runtime::simulator::{Simulator, SimulatorConfig};

pub async fn run_demo(
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
    engine.load(&program)?;

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
