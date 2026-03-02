use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use rayon::prelude::*;
use tokio::sync::{mpsc, RwLock};
use tracing::info;
use varpulis_parser::parse;
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

    println!("Varpulis Event Simulation");
    println!("============================");
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
        .map_err(|e| anyhow::anyhow!("Load error:\n{e}"))?;

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

    println!("Starting simulation...\n");
    let start = std::time::Instant::now();

    // Shared counter for total events processed across all workers
    let total_events_processed = Arc::new(AtomicU64::new(0));

    // Process events based on mode
    if !timed && !streaming && num_workers > 1 {
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
                    .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
                engine
                    .checkpoint_tick()
                    .map_err(|e| anyhow::anyhow!("Checkpoint error: {e}"))?;
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
                    .map_err(|e| anyhow::anyhow!("Process error: {e}"))?;
                engine
                    .checkpoint_tick()
                    .map_err(|e| anyhow::anyhow!("Checkpoint error: {e}"))?;
                batch_idx += 1;
            }
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
                let mut w_engine = if quiet {
                    Engine::new_benchmark()
                } else {
                    Engine::new_shared(output_tx.clone())
                };
                if let Err(e) = w_engine.load(&program) {
                    eprintln!("Failed to load program: {e}");
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

    println!("\nSimulation Complete");
    println!("======================");
    println!("Duration:         {elapsed:?}");
    println!("Events processed: {events_processed}");
    println!("Workers used:     {num_workers}");
    println!("Output events emitted: {output_events_count}");
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
