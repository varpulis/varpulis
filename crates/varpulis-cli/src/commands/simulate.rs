use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use notify::{EventKind, RecursiveMode, Watcher};
use owo_colors::OwoColorize;
use rayon::prelude::*;
use tokio::sync::{mpsc, RwLock};
use tracing::info;
use varpulis_cli::output;
use varpulis_connectors::credentials::CredentialsStore;
use varpulis_parser::parse;
use varpulis_runtime::engine::trace::TraceEntry;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::{EventFileParser, EventFilePlayer, StreamingEventReader};
use varpulis_runtime::SharedEvent; // PERF: Zero-copy event sharing

#[allow(clippy::too_many_arguments)]
pub async fn run_simulation(
    program_path: &PathBuf,
    events_path: &PathBuf,
    timed: bool,
    verbose: bool,
    streaming: bool,
    workers: Option<usize>,
    partition_by: Option<&str>,
    quiet: bool,
    checkpoint_dir: Option<PathBuf>,
    checkpoint_interval: u64,
    credentials_store: Option<Arc<CredentialsStore>>,
    trace: bool,
) -> Result<()> {
    // Determine number of workers
    // Trace mode forces single-threaded for clear sequential output.
    let num_workers = if trace {
        1
    } else {
        workers.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
    };

    // Configure rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .build_global()
        .ok(); // Ignore if already initialized

    let mode_str = if timed {
        "timed (real-time replay)"
    } else if streaming {
        if num_workers > 1 {
            "streaming, parallel"
        } else {
            "streaming"
        }
    } else if num_workers > 1 {
        "preload, parallel"
    } else {
        "preload"
    };

    output::header("Varpulis Event Simulation");
    println!("Program: {}", program_path.display());
    println!("Events:  {}", events_path.display());
    println!("Mode:    {mode_str}");
    println!("Workers: {num_workers}");
    if let Some(key) = partition_by {
        println!("Partition: {key}");
    }
    println!();

    // Load and parse program
    let program_source = std::fs::read_to_string(program_path)?;
    let program = parse(&program_source).map_err(|e| anyhow::anyhow!("Parse error: {e}"))?;
    let program = Arc::new(program);
    info!(
        "Loaded program with {} statements",
        program.statements.len()
    );

    // Create output event channel (shared across all engines)
    // PERF: Use SharedEvent (Arc<Event>) channel for zero-copy event passing.
    //
    // The buffer is sized large enough that the engine's cooperative backpressure
    // path (see `send_output_shared`) is rarely hit even when piping output to a
    // slow consumer. Backpressure NEVER drops events — see #44 for context.
    let channel_size = if quiet {
        100_000
    } else {
        // 64K events per worker: large enough that the receiver can drain
        // most JSON-printing workloads without backpressure stalls.
        65_536 * num_workers
    };
    let (output_tx, mut output_rx) = mpsc::channel::<SharedEvent>(channel_size);

    // Create initial engine to show metrics
    // In quiet mode, use benchmark engine to skip channel overhead entirely
    // PERF: Use new_shared() for zero-copy SharedEvent channel
    let mut engine = if quiet {
        let mut b = Engine::builder();
        if let Some(ref store) = credentials_store {
            b = b.credentials(Arc::clone(store));
        }
        b.build()
    } else {
        let mut b = Engine::builder().shared_output(output_tx.clone());
        if let Some(ref store) = credentials_store {
            b = b.credentials(Arc::clone(store));
        }
        b.build()
    };
    engine
        .load_with_source(&program_source, &program)
        .map_err(|e| anyhow::anyhow!("Load error:\n{e}"))?;

    // Enable trace if requested (must be after load())
    if trace {
        engine.set_trace_enabled(true);
    }

    // Enable auto-checkpointing if requested (must be after load())
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
            key_prefix: "varpulis-simulate".to_string(),
        };
        engine
            .enable_checkpointing(store, config)
            .map_err(|e| anyhow::anyhow!("Checkpoint init error: {e}"))?;
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

    if trace {
        println!("Mode:    trace (single-threaded)");
    }
    println!("Starting simulation...\n");
    let start = std::time::Instant::now();

    // Shared counter for total events processed across all workers
    let total_events_processed = Arc::new(AtomicU64::new(0));

    // Trace mode: process events one at a time and print trace entries after each
    if trace {
        let events_source = std::fs::read_to_string(events_path)?;
        let events = EventFileParser::parse(&events_source)
            .map_err(|e| anyhow::anyhow!("Event file error: {e}"))?;
        let total_events = events.len();
        info!("Preloaded {} events for trace mode", total_events);

        for (idx, timed_event) in events.into_iter().enumerate() {
            let event = timed_event.event;
            let event_type = event.event_type.to_string();

            // Print event header
            print_trace_event_header(idx + 1, total_events, &event_type, &event);

            // Process the single event
            engine
                .process(event)
                .await
                .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;

            // Drain and print trace entries
            let entries = engine.drain_trace();
            print_trace_entries(&entries);
        }
    } else if !timed && !streaming && num_workers > 1 {
        // Parallel preload mode (default with multiple workers)
        let events_source = std::fs::read_to_string(events_path)?;
        let events = EventFileParser::parse(&events_source)
            .map_err(|e| anyhow::anyhow!("Event file error: {e}"))?;
        let total_events = events.len();
        info!("Preloaded {} events from file", total_events);

        // Probe pipeline to decide distribution strategy
        let mut probe_engine = Engine::new_benchmark();
        probe_engine
            .load(&program)
            .map_err(|e| anyhow::anyhow!("Probe engine load error: {e}"))?;
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
                println!("  Partition key: {partition_key}");
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
        let creds_arc = credentials_store.clone();

        let spinner = if !quiet && !verbose {
            let sp = output::create_spinner(&format!(
                "Processing {total_events} events across {num_workers} workers..."
            ));
            Some(sp)
        } else {
            None
        };

        tokio::task::spawn_blocking(move || {
            partitions
                .into_par_iter()
                .enumerate()
                .for_each(|(worker_id, partition_events)| {
                    if partition_events.is_empty() {
                        return;
                    }
                    let mut b = match &output_tx_arc {
                        Some(tx) => Engine::builder().shared_output(tx.clone()),
                        None => Engine::builder(),
                    };
                    if let Some(ref store) = creds_arc {
                        b = b.credentials(Arc::clone(store));
                    }
                    let mut worker_engine = b.build();
                    if let Err(e) = worker_engine.load(&program_arc) {
                        eprintln!("Worker {worker_id}: Failed to load program: {e}");
                        return;
                    }

                    // PERF: Pass entire partition as single batch — no extra copy
                    if let Err(e) = worker_engine.process_batch_sync(partition_events) {
                        eprintln!("Worker {worker_id}: Process error: {e}");
                        return;
                    }

                    let worker_metrics = worker_engine.metrics();
                    total_counter.fetch_add(worker_metrics.events_processed, Ordering::Relaxed);
                    output_counter
                        .fetch_add(worker_metrics.output_events_emitted, Ordering::Relaxed);
                });
        })
        .await
        .map_err(|e| anyhow::anyhow!("Spawn blocking failed: {e}"))?;

        if let Some(sp) = spinner {
            sp.finish_and_clear();
        }
    } else if !timed && !streaming {
        // Single-threaded preload mode (default)
        let events_source = std::fs::read_to_string(events_path)?;
        let events = EventFileParser::parse(&events_source)
            .map_err(|e| anyhow::anyhow!("Event file error: {e}"))?;
        info!("Preloaded {} events from file", events.len());

        // PERF: Extract owned events (zero-clone), use sync path when no sinks
        let use_sync = !engine.has_sink_operations();
        let mut all_events: Vec<Event> = events.into_iter().map(|te| te.event).collect();
        let total_events = all_events.len();

        let pb = if !quiet && !verbose {
            Some(output::create_progress_bar(total_events as u64))
        } else {
            None
        };

        const BATCH_SIZE: usize = 10000;

        if use_sync {
            // PERF: Sync path avoids async overhead (tokio runtime, .await suspension)
            let mut batch_idx = 0;
            while !all_events.is_empty() {
                let end = BATCH_SIZE.min(all_events.len());
                let batch: Vec<Event> = all_events.drain(..end).collect();
                let batch_len = batch.len();
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
                    .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
                engine
                    .checkpoint_tick()
                    .map_err(|e| anyhow::anyhow!("Checkpoint error: {e}"))?;
                if let Some(ref pb) = pb {
                    pb.inc(batch_len as u64);
                }
                batch_idx += 1;
            }
        } else {
            // Async path needed for .to() sink operations
            let mut batch_idx = 0;
            while !all_events.is_empty() {
                let end = BATCH_SIZE.min(all_events.len());
                let batch: Vec<Event> = all_events.drain(..end).collect();
                let batch_len = batch.len();
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
                    .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
                engine
                    .checkpoint_tick()
                    .map_err(|e| anyhow::anyhow!("Checkpoint error: {e}"))?;
                if let Some(ref pb) = pb {
                    pb.inc(batch_len as u64);
                }
                batch_idx += 1;
            }
        }

        if let Some(pb) = pb {
            pb.finish_and_clear();
        }
    } else if streaming && num_workers > 1 {
        // Parallel streaming mode (--streaming with multiple workers)
        const BATCH_SIZE: usize = 50000;

        let mut event_reader = StreamingEventReader::from_file(events_path)
            .map_err(|e| anyhow::anyhow!("Failed to open event file: {e}"))?;

        // Probe pipeline to decide distribution strategy
        let mut probe_engine = Engine::new_benchmark();
        probe_engine
            .load(&program)
            .map_err(|e| anyhow::anyhow!("Probe engine load error: {e}"))?;
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
                let mut b = if quiet {
                    Engine::builder()
                } else {
                    Engine::builder().shared_output(output_tx.clone())
                };
                if let Some(ref store) = credentials_store {
                    b = b.credentials(Arc::clone(store));
                }
                let mut w_engine = b.build();
                if let Err(e) = w_engine.load(&program) {
                    eprintln!("Failed to load program: {e}");
                }
                Mutex::new(w_engine)
            })
            .collect();
        let worker_engines = Arc::new(worker_engines);

        let spinner = if !quiet && !verbose {
            Some(output::create_spinner("Streaming events..."))
        } else {
            None
        };

        loop {
            let mut events: Vec<Event> = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                match event_reader.next() {
                    Some(Ok(event)) => events.push(event),
                    Some(Err(e)) => return Err(anyhow::anyhow!("Parse error: {e}")),
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
            if let Some(ref sp) = spinner {
                sp.set_message(format!(
                    "Streaming events... {} read",
                    event_reader.events_read()
                ));
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
                        let mut engine_guard =
                            engines[worker_id].lock().unwrap_or_else(|e| e.into_inner());
                        if let Err(e) = engine_guard.process_batch_sync(partition_events) {
                            eprintln!("Worker {worker_id}: Process error: {e}");
                        }
                    });
            })
            .await
            .ok();
        }

        if let Some(sp) = spinner {
            sp.finish_and_clear();
        }

        // Collect final metrics from all worker engines
        for (i, engine_mutex) in worker_engines.iter().enumerate() {
            let w_engine = engine_mutex.lock().unwrap_or_else(|e| e.into_inner());
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
    } else if streaming {
        // Single-threaded streaming mode (--streaming for huge files)
        const BATCH_SIZE: usize = 10000;
        let use_sync = !engine.has_sink_operations();

        let mut event_reader = StreamingEventReader::from_file(events_path)
            .map_err(|e| anyhow::anyhow!("Failed to open event file: {e}"))?;

        let spinner = if !quiet && !verbose {
            Some(output::create_spinner("Streaming events..."))
        } else {
            None
        };

        let mut batch: Vec<Event> = Vec::with_capacity(BATCH_SIZE);
        let mut batch_count = 0;

        loop {
            // Fill batch from streaming reader (batch is empty after std::mem::take)
            batch.reserve(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                match event_reader.next() {
                    Some(Ok(event)) => batch.push(event),
                    Some(Err(e)) => return Err(anyhow::anyhow!("Parse error: {e}")),
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
            if let Some(ref sp) = spinner {
                sp.set_message(format!(
                    "Streaming events... {} read",
                    event_reader.events_read()
                ));
            }

            // PERF: Use sync path when no .to() sinks to avoid async overhead
            if use_sync {
                engine
                    .process_batch_sync(std::mem::take(&mut batch))
                    .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
            } else {
                engine
                    .process_batch(std::mem::take(&mut batch))
                    .await
                    .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
            }
            engine
                .checkpoint_tick()
                .map_err(|e| anyhow::anyhow!("Checkpoint error: {e}"))?;
        }

        if let Some(sp) = spinner {
            sp.finish_and_clear();
        }

        info!("Streamed {} events from file", event_reader.events_read());
    } else {
        // Timed mode (--timed) - replay events with real-time delays
        let events_source = std::fs::read_to_string(events_path)?;
        let events = EventFileParser::parse(&events_source)
            .map_err(|e| anyhow::anyhow!("Event file error: {e}"))?;
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
                .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
        }

        player_handle
            .await?
            .map_err(|e| anyhow::anyhow!("Player error: {e}"))?;
    }

    // Flush any remaining session windows after all events are processed
    if engine.has_session_windows() {
        engine
            .flush_expired_sessions()
            .await
            .map_err(|e| anyhow::anyhow!("Session flush error: {e}"))?;
    }

    // Final checkpoint on shutdown
    if engine.has_checkpointing() {
        engine
            .force_checkpoint()
            .map_err(|e| anyhow::anyhow!("Final checkpoint error: {e}"))?;
    }

    // Wait a bit for any pending output events
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Summary
    let elapsed = start.elapsed();

    // Get total events processed (from parallel counter or single engine)
    let events_processed = if num_workers > 1 && !timed {
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

    println!();
    output::success("Simulation Complete");
    println!("======================");
    println!("Duration:         {elapsed:?}");
    println!("Events processed: {events_processed}");
    println!("Workers used:     {num_workers}");
    println!("Output events emitted: {output_events_count}");
    let event_rate = events_processed as f64 / elapsed.as_secs_f64();
    if output::color_enabled() {
        println!(
            "Event rate:       {}",
            format!("{event_rate:.1} events/sec").cyan()
        );
    } else {
        println!("Event rate:       {event_rate:.1} events/sec");
    }

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

/// Watch for file changes and re-run simulation automatically.
///
/// Uses `notify::RecommendedWatcher` to watch the .vpl and .evt files.
/// On any change, debounces for 300ms, clears the terminal, and re-runs
/// the simulation. Parse/load errors are displayed but do not stop the watcher.
#[allow(clippy::too_many_arguments)]
pub async fn run_watch_loop(
    program_path: &PathBuf,
    events_path: &PathBuf,
    timed: bool,
    verbose: bool,
    streaming: bool,
    workers: Option<usize>,
    partition_by: Option<&str>,
    quiet: bool,
    checkpoint_dir: Option<PathBuf>,
    checkpoint_interval: u64,
    credentials_store: Option<Arc<CredentialsStore>>,
    trace: bool,
) -> Result<()> {
    // Resolve to canonical paths for watching
    let program_canonical =
        std::fs::canonicalize(program_path).unwrap_or_else(|_| program_path.clone());
    let events_canonical =
        std::fs::canonicalize(events_path).unwrap_or_else(|_| events_path.clone());

    output::header("Varpulis Watch Mode");
    println!("Watching: {}", program_canonical.display());
    println!("Watching: {}", events_canonical.display());
    println!("Press Ctrl+C to stop.\n");

    // Create a tokio mpsc channel for notify events
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

    // Set up the file watcher with a callback that sends to our tokio channel
    let tx = notify_tx.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            match event.kind {
                EventKind::Modify(_) | EventKind::Create(_) => {
                    let _ = tx.send(());
                }
                _ => {}
            }
        }
    })
    .map_err(|e| anyhow::anyhow!("Failed to create file watcher: {e}"))?;

    // Watch the parent directories (handles editors that do atomic saves via rename)
    let program_dir = program_canonical
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let events_dir = events_canonical
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));

    watcher
        .watch(program_dir, RecursiveMode::NonRecursive)
        .map_err(|e| anyhow::anyhow!("Failed to watch {}: {e}", program_dir.display()))?;

    // Only watch events dir separately if it differs from the program dir
    if events_dir != program_dir {
        watcher
            .watch(events_dir, RecursiveMode::NonRecursive)
            .map_err(|e| anyhow::anyhow!("Failed to watch {}: {e}", events_dir.display()))?;
    }

    // Keep partition_by as an owned String for the loop
    let partition_by_owned = partition_by.map(|s| s.to_string());

    // Run simulation once immediately
    let result = run_simulation(
        program_path,
        events_path,
        timed,
        verbose,
        streaming,
        workers,
        partition_by_owned.as_deref(),
        quiet,
        checkpoint_dir.clone(),
        checkpoint_interval,
        credentials_store.clone(),
        trace,
    )
    .await;

    if let Err(e) = result {
        output::error(&format!("{e}"));
    }

    println!("\n--- Watching for changes... ---\n");

    // Main watch loop
    loop {
        tokio::select! {
            // Wait for a file change notification
            Some(()) = notify_rx.recv() => {
                // Debounce: drain any additional events that arrive within 300ms
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                while notify_rx.try_recv().is_ok() {}

                // Clear terminal
                print!("\x1B[2J\x1B[H");
                std::io::stdout().flush().ok();

                let now = chrono::Local::now().format("%H:%M:%S");
                output::success(&format!("Reloaded at {now}"));
                println!();

                let result = run_simulation(
                    program_path,
                    events_path,
                    timed,
                    verbose,
                    streaming,
                    workers,
                    partition_by_owned.as_deref(),
                    quiet,
                    checkpoint_dir.clone(),
                    checkpoint_interval,
                    credentials_store.clone(),
                    trace,
                )
                .await;

                if let Err(e) = result {
                    output::error(&format!("{e}"));
                }

                println!("\n--- Watching for changes... ---\n");
            }

            // Handle Ctrl+C gracefully
            _ = tokio::signal::ctrl_c() => {
                println!();
                output::success("Watch mode stopped.");
                break;
            }
        }
    }

    Ok(())
}

// =========================================================================
// Trace output helpers
// =========================================================================

/// Print a coloured header line for an incoming event in trace mode.
fn print_trace_event_header(
    index: usize,
    total: usize,
    event_type: &str,
    event: &varpulis_runtime::event::Event,
) {
    let fields: Vec<String> = event.data.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let fields_str = if fields.is_empty() {
        String::new()
    } else {
        format!(" {{ {} }}", fields.join(", "))
    };

    if output::color_enabled() {
        println!(
            "{} [{}/{}] {}{}",
            "EVENT".bold().blue(),
            index,
            total,
            event_type.bold(),
            fields_str.dimmed(),
        );
    } else {
        println!("EVENT [{index}/{total}] {event_type}{fields_str}");
    }
}

/// Print a list of trace entries with coloured output.
fn print_trace_entries(entries: &[TraceEntry]) {
    if entries.is_empty() {
        if output::color_enabled() {
            println!("  {} no matching streams", "->".dimmed());
        } else {
            println!("  -> no matching streams");
        }
        println!();
        return;
    }

    for entry in entries {
        match entry {
            TraceEntry::StreamMatched {
                stream_name,
                event_type,
            } => {
                if output::color_enabled() {
                    println!(
                        "  {} stream {} matched on {}",
                        "->".blue(),
                        stream_name.bold().blue(),
                        event_type.dimmed(),
                    );
                } else {
                    println!("  -> stream {stream_name} matched on {event_type}");
                }
            }
            TraceEntry::OperatorResult {
                stream_name: _,
                op_name,
                passed,
                detail,
            } => {
                let detail_str = detail
                    .as_ref()
                    .map(|d| format!(" ({d})"))
                    .unwrap_or_default();
                if *passed {
                    if output::color_enabled() {
                        println!(
                            "     {} {}{} {}",
                            "|".dimmed(),
                            op_name.green(),
                            detail_str.dimmed(),
                            "PASS".green().bold(),
                        );
                    } else {
                        println!("     | {op_name}{detail_str} PASS");
                    }
                } else if output::color_enabled() {
                    println!(
                        "     {} {}{} {}",
                        "|".dimmed(),
                        op_name.dimmed(),
                        detail_str.dimmed(),
                        "BLOCK".red(),
                    );
                } else {
                    println!("     | {op_name}{detail_str} BLOCK");
                }
            }
            TraceEntry::PatternState {
                stream_name: _,
                active_runs,
                completed,
            } => {
                if output::color_enabled() {
                    println!(
                        "     {} pattern: {} active run(s), {} completed",
                        "|".dimmed(),
                        format!("{active_runs}").cyan(),
                        format!("{completed}").cyan(),
                    );
                } else {
                    println!("     | pattern: {active_runs} active run(s), {completed} completed");
                }
            }
            TraceEntry::EventEmitted {
                stream_name,
                fields,
            } => {
                let fields_str: Vec<String> =
                    fields.iter().map(|(k, v)| format!("{k}={v}")).collect();
                if output::color_enabled() {
                    println!(
                        "  {} {} emitted {{ {} }}",
                        "<-".green().bold(),
                        stream_name.green().bold(),
                        fields_str.join(", ").green(),
                    );
                } else {
                    println!("  <- {stream_name} emitted {{ {} }}", fields_str.join(", "));
                }
            }
        }
    }
    println!();
}
