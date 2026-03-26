use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;
use varpulis_cli::output;
use varpulis_connectors::credentials::CredentialsStore;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

pub async fn run_program(
    source: &str,
    base_path: Option<&PathBuf>,
    credentials_store: Option<Arc<CredentialsStore>>,
) -> Result<()> {
    use varpulis_runtime::connector::ManagedConnectorRegistry;
    use varpulis_runtime::ContextOrchestrator;

    info!("Parsing VPL program...");
    let mut program = parse(source).map_err(|e| anyhow::anyhow!("Parse error: {e}"))?;
    info!("Parsed {} statements", program.statements.len());

    // Resolve imports
    varpulis_cli::resolve_imports(&mut program, base_path)?;

    // Create output event channel — clone tx before giving it to the engine
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(10_000);
    let output_tx_for_ctx = output_tx.clone();

    // Create engine with credentials store if provided
    let mut engine = Engine::builder().output(output_tx);
    if let Some(store) = credentials_store {
        engine = engine.credentials(store);
    }
    let mut engine = engine.build();
    engine
        .load_with_source(source, &program)
        .map_err(|e| anyhow::anyhow!("Load error:\n{e}"))?;

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
            Err(e) => anyhow::bail!("Failed to build context orchestrator: {e}"),
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
            .map_err(|e| anyhow::anyhow!("Registry build error: {e}"))?;

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
                .map_err(|e| anyhow::anyhow!("Source start error: {e}"))?;
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
                .map_err(|e| anyhow::anyhow!("Sink connection error: {e}"))?;
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
        let spinner = output::create_spinner("Waiting for events...");

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
                        spinner.set_message(format!(
                            "Events: {} | Output: {} | Rate: {:.0}/s",
                            metrics.events_processed,
                            metrics.output_events_emitted,
                            event_count as f64 / start.elapsed().as_secs_f64()
                        ));
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
                    spinner.finish_and_clear();
                    println!("\nStopping...");
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
                    println!("   {key}: {value}");
                }
            }
        });
    }

    Ok(())
}
