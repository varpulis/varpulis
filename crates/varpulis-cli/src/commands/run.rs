use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;
use varpulis_cli::output;
use varpulis_connectors::credentials::CredentialsStore;
use varpulis_connectors::ManagedConnectorRegistry;
use varpulis_parser::parse;
use varpulis_runtime::engine::{BarrierError, Engine, SourceCommitCoordinator};
use varpulis_runtime::event::Event;

/// Commits source consumer-group offsets through the managed connector
/// registry — the CLI's implementation of the engine's barrier commit seam.
///
/// The end-to-end 2PC sequence itself now lives in
/// [`Engine::barrier_commit_2pc`]; this wrapper just performs the out-of-band
/// offset commit the engine asks for, mapping the connector error into the
/// engine's [`BarrierError`].
struct RegistryCommitCoordinator<'a> {
    registry: &'a ManagedConnectorRegistry,
}

#[async_trait::async_trait]
impl SourceCommitCoordinator for RegistryCommitCoordinator<'_> {
    async fn stage_txn_offsets(
        &self,
        connector: &str,
        topic: &str,
        offsets: &std::collections::HashMap<i32, i64>,
    ) -> Result<(), BarrierError> {
        self.registry
            .stage_txn_offsets(connector, topic, offsets)
            .await
            .map_err(|e| BarrierError::OffsetCommit(e.to_string()))
    }

    async fn commit_offsets(
        &self,
        connector: &str,
        topic: &str,
        offsets: &std::collections::HashMap<i32, i64>,
    ) -> Result<(), BarrierError> {
        self.registry
            .commit_source_offsets(connector, topic, offsets)
            .await
            .map_err(|e| BarrierError::OffsetCommit(e.to_string()))
    }
}

pub async fn run_program(
    source: &str,
    base_path: Option<&PathBuf>,
    credentials_store: Option<Arc<CredentialsStore>>,
    quiet: bool,
    checkpoint_dir: Option<PathBuf>,
    checkpoint_interval: u64,
) -> Result<()> {
    use varpulis_runtime::connector::ManagedConnectorRegistry;
    use varpulis_runtime::ContextOrchestrator;

    info!("Parsing VPL program...");
    let mut program = parse(source).map_err(|e| anyhow::anyhow!("Parse error: {e}"))?;
    info!("Parsed {} statements", program.statements.len());

    // Resolve imports
    varpulis_cli::resolve_imports(&mut program, base_path)?;

    // Engine output wiring. In --quiet mode emitted events are dropped at
    // the source (no per-event clone, no channel hop, no drain task) —
    // sink deliveries via .to() are the only observable output. Otherwise
    // use the zero-copy SharedEvent channel and print from it.
    let mut engine_output_rx: Option<mpsc::Receiver<varpulis_runtime::event::SharedEvent>> = None;
    let mut engine = if quiet {
        Engine::builder().discard_output()
    } else {
        let (output_tx, output_rx) = mpsc::channel::<varpulis_runtime::event::SharedEvent>(10_000);
        engine_output_rx = Some(output_rx);
        Engine::builder().shared_output(output_tx)
    };
    if let Some(store) = credentials_store {
        engine = engine.credentials(store);
    }
    // Declare checkpoint intent BEFORE load so the compiler keeps windowed
    // aggregate state in checkpointable ops (skips columnar-aggregate fusion).
    // enable_checkpointing() below runs after load, which is too late to gate
    // fusion.
    engine = engine.plan_checkpointing(checkpoint_dir.is_some());
    let mut engine = engine.build();
    engine
        .load_with_source(source, &program)
        .map_err(|e| anyhow::anyhow!("Load error:\n{e}"))?;

    let metrics = engine.metrics();
    println!("\nProgram loaded successfully!");
    println!("   Streams registered: {}", metrics.streams_count);

    // Build context orchestrator if the program declares contexts.
    // Contexts still use the legacy owned-event channel; it gets its own
    // drain/print task since the engine channel no longer carries them.
    let has_contexts = engine.has_contexts();
    let mut orchestrator = if has_contexts {
        let (ctx_tx, mut ctx_rx) = mpsc::channel::<Event>(10_000);
        let ctx_quiet = quiet;
        tokio::spawn(async move {
            if ctx_quiet {
                while ctx_rx.recv().await.is_some() {}
            } else {
                while let Some(output_event) = ctx_rx.recv().await {
                    println!(
                        "OUTPUT EVENT: {} - {:?}",
                        output_event.event_type, output_event.data
                    );
                }
            }
        });
        match ContextOrchestrator::build(engine.context_map(), &program, ctx_tx, 1000) {
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

    // Enable disk-based checkpointing so stateful engine state (windows,
    // SASE runs, join buffers, variables, watermarks, source offsets) survives
    // crashes. On restart the engine auto-restores from the latest checkpoint,
    // and the 2PC barrier persists a fresh checkpoint before committing sinks.
    if let Some(ref cp_dir) = checkpoint_dir {
        std::fs::create_dir_all(cp_dir)?;
        let store: std::sync::Arc<dyn varpulis_runtime::persistence::StateStore> =
            std::sync::Arc::new(
                varpulis_runtime::persistence::FileStore::open(cp_dir)
                    .map_err(|e| anyhow::anyhow!("Checkpoint store error: {e}"))?,
            );
        let config = varpulis_runtime::persistence::CheckpointConfig {
            interval: std::time::Duration::from_secs(checkpoint_interval),
            max_checkpoints: 3,
            checkpoint_on_shutdown: true,
            key_prefix: "varpulis-run".to_string(),
        };
        engine
            .enable_checkpointing(store, config)
            .map_err(|e| anyhow::anyhow!("Checkpoint init error: {e}"))?;
        info!(
            "Checkpointing enabled: {} (every {}s)",
            cp_dir.display(),
            checkpoint_interval
        );
    }

    let bindings = engine.source_bindings().to_vec();

    if !bindings.is_empty() {
        // Create shared event channel for all source connectors. Connectors
        // produce **batches** (`Vec<Event>`), not single events — see
        // `ManagedConnector::start_source` and
        // `docs/development/kafka-source-batching.md` for the rationale.
        // Batching at the connector eliminates per-event ping-pong between
        // the consumer task and the run-loop, which was the dominant cost
        // at high event rates.
        let (event_tx, mut event_rx) = mpsc::channel::<Vec<Event>>(1_024);

        // Build managed connector registry — one connection per connector
        let mut registry = ManagedConnectorRegistry::from_configs(engine.connector_configs())
            .map_err(|e| anyhow::anyhow!("Registry build error: {e}"))?;

        // Bind the engine's source-offset registry to every managed connector
        // so replayable sources (Kafka) mirror their consumed offsets into
        // the engine's checkpoint state. Non-replayable connectors ignore
        // the call. Must happen BEFORE start_source so the consumer task
        // picks up the shared handle on its first iteration.
        registry.set_engine_offsets_registry(engine.source_offsets_handle());
        // Wire the cooperative source-pause flag so replayable sources stop
        // pulling while the checkpoint barrier drains in-flight events (C3).
        // Also before start_source, for the same reason.
        registry.set_source_pause_handle(engine.source_pause_handle());

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

        // Create shared sinks for EVERY connector referenced by the
        // pipeline (not just sources). Previously we only injected sinks
        // for `.from()` bindings — `.to()`-only connectors fell back to
        // the engine's legacy `KafkaSink` which uses the blocking
        // `producer.send(...).await` path (~6 ms per event). By using
        // the managed connector here we get the non-blocking
        // `send_result` path, matching Arroyo's pattern.
        let connector_names: Vec<String> = engine.connector_configs().keys().cloned().collect();
        for connector_name in &connector_names {
            let sink_keys = engine.sink_keys_for_connector(connector_name);
            if sink_keys.is_empty() {
                continue;
            }

            let default_topic = engine
                .get_connector(connector_name)
                .and_then(|c| c.topic.clone())
                .unwrap_or_else(|| format!("{}-output", connector_name));

            let mut created = 0usize;
            for sink_key in &sink_keys {
                let sink_topic =
                    if let Some(topic) = sink_key.strip_prefix(&format!("{}::", connector_name)) {
                        topic.to_string()
                    } else {
                        default_topic.clone()
                    };

                let empty_params = std::collections::HashMap::new();
                match registry.create_sink(connector_name, &sink_topic, &empty_params) {
                    Ok(sink) => {
                        engine.inject_sink(sink_key, sink);
                        created += 1;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to create sink for {}: {}", sink_key, e);
                    }
                }
            }

            if created > 0 {
                println!(
                    "  Sharing connection for {} sink(s) on connector {}",
                    created, connector_name
                );
            }
        }

        // Connect any remaining sinks that aren't registry-managed
        if engine.has_sink_operations() {
            engine
                .connect_sinks()
                .await
                .map_err(|e| anyhow::anyhow!("Sink connection error: {e}"))?;
        }

        // Start the first transactional epoch for any exactly-once sinks.
        // Subsequent epochs are opened by commit_sinks(id) → begin_epoch(id+1).
        engine.begin_epoch_sinks(0).await;

        // Spawn the output printer. In `--quiet` mode there is no channel at
        // all — the engine discards output events at the source, avoiding
        // the per-event clone + channel hop + stdout lock entirely.
        if let Some(mut output_rx) = engine_output_rx.take() {
            tokio::spawn(async move {
                while let Some(output_event) = output_rx.recv().await {
                    println!(
                        "OUTPUT EVENT: {} - {:?}",
                        output_event.event_type, output_event.data
                    );
                }
            });
        }

        println!("\nListening for events...");
        println!("   Press Ctrl+C to stop\n");

        let start = std::time::Instant::now();
        let mut last_report = std::time::Instant::now();
        let mut event_count = 0u64;
        let spinner = output::create_spinner("Waiting for events...");
        // Monotonic checkpoint epoch counter. Incremented on every barrier
        // so that 2PC sink `commit(id)` / `begin_epoch(id+1)` calls see a
        // strictly increasing id and can reject replays out of order.
        let mut checkpoint_id: u64 = 0;

        // Process event batches from all sources. Each `recv()` yields a
        // `Vec<Event>` produced by the connector — typically up to 256
        // events per batch. We further coalesce contiguous batches via
        // `try_recv` to keep `process_batch` calls fat (one async wake-up
        // can drain thousands of events).
        loop {
            tokio::select! {
                Some(mut batch) = event_rx.recv() => {
                    // Coalesce any additional batches that are already
                    // buffered — this is essentially free and lets
                    // process_batch amortize over even larger groups.
                    while let Ok(extra) = event_rx.try_recv() {
                        batch.extend(extra);
                    }
                    event_count += batch.len() as u64;

                    if let Some(ref mut orchestrator) = orchestrator {
                        for event in batch {
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
                        }
                    } else if let Err(e) = engine.process_batch(batch).await {
                        tracing::warn!("Engine batch error: {}", e);
                    }

                    // Progress report every 2 seconds
                    if last_report.elapsed() >= std::time::Duration::from_secs(2) {
                        let metrics = engine.metrics();
                        spinner.set_message(format!(
                            "Events: {} | Output: {} | Rate: {:.0}/s | Epoch: {}",
                            metrics.events_processed,
                            metrics.output_events_emitted,
                            event_count as f64 / start.elapsed().as_secs_f64(),
                            checkpoint_id
                        ));
                        last_report = std::time::Instant::now();

                        if let Some(ref mut orch) = orchestrator {
                            // Context-mode pipelines still use the legacy
                            // periodic `checkpoint_tick` path for now.
                            if let Err(e) = orch.checkpoint_tick() {
                                tracing::warn!("Checkpoint error: {}", e);
                            }
                        } else {
                            // Non-context path: run a full 2PC barrier that
                            // commits transactional sinks AND source offsets
                            // as a single checkpoint epoch.
                            checkpoint_id += 1;
                            let coordinator = RegistryCommitCoordinator { registry: &registry };
                            if let Err(e) = engine
                                .barrier_commit_2pc(checkpoint_id, &mut event_rx, &coordinator)
                                .await
                            {
                                tracing::warn!(
                                    "Checkpoint barrier failed (epoch {checkpoint_id}): {e}"
                                );
                            }
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    spinner.finish_and_clear();
                    println!("\nStopping...");

                    // Final 2PC barrier: commit any pending transactional
                    // sink data + source offsets before shutting down.
                    // Without this, events processed since the last barrier
                    // would be lost (uncommitted transaction).
                    if orchestrator.is_none() {
                        // Shutdown drain (C2b): graduate still-open
                        // event-time windows so their results reach the
                        // sinks before the final barrier commits.
                        if let Err(e) = engine.flush_final_watermark().await {
                            tracing::warn!("Watermark drain error: {}", e);
                        }
                        checkpoint_id += 1;
                        let coordinator = RegistryCommitCoordinator { registry: &registry };
                        if let Err(e) = engine
                            .barrier_commit_2pc(checkpoint_id, &mut event_rx, &coordinator)
                            .await
                        {
                            tracing::warn!("Checkpoint barrier failed (epoch {checkpoint_id}): {e}");
                        }
                    }

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
        if let Some(mut output_rx) = engine_output_rx.take() {
            tokio::spawn(async move {
                while let Some(output_event) = output_rx.recv().await {
                    println!("\nOUTPUT EVENT: {}", output_event.event_type);
                    for (key, value) in &output_event.data {
                        println!("   {key}: {value}");
                    }
                }
            });
        }
    }

    Ok(())
}
