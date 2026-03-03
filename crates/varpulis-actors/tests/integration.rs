//! Integration tests for multi-actor pipeline wiring and backpressure.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use varpulis_actors::mailbox::Envelope;
use varpulis_actors::*;

/// Source actor that generates numbered events.
struct EventSource {
    count: u64,
    max_events: u64,
    downstream: Option<MailboxSender<EventProcessor>>,
}

impl EventSource {
    const fn new(max_events: u64) -> Self {
        Self {
            count: 0,
            max_events,
            downstream: None,
        }
    }

    fn with_downstream(mut self, sender: MailboxSender<EventProcessor>) -> Self {
        self.downstream = Some(sender);
        self
    }
}

#[derive(Debug, Clone)]
struct NumberEvent(#[allow(dead_code)] u64);

#[async_trait]
impl Actor for EventSource {
    type ObservableState = u64;

    fn name(&self) -> &'static str {
        "event-source"
    }

    fn observable_state(&self) -> u64 {
        self.count
    }

    async fn run(mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus> {
        if let Some(downstream) = &self.downstream {
            for i in 0..self.max_events {
                if ctx.is_shutdown() {
                    return Ok(());
                }
                self.count = i + 1;
                if downstream.send(NumberEvent(i)).await.is_err() {
                    return Err(ActorExitStatus::Failure("downstream closed".to_string()));
                }
            }
        }
        // Wait for shutdown while handling observe requests
        loop {
            tokio::select! {
                () = ctx.shutdown.cancelled() => return Ok(()),
                msg = ctx.mailbox.recv() => {
                    match msg {
                        Some(Envelope::Observe(tx)) => {
                            let _ = tx.send(self.observable_state());
                        }
                        Some(_) => {}
                        None => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Processing actor that receives events and counts them.
struct EventProcessor {
    processed: Arc<AtomicU64>,
}

impl EventProcessor {
    const fn new(counter: Arc<AtomicU64>) -> Self {
        Self { processed: counter }
    }
}

#[async_trait]
impl Actor for EventProcessor {
    type ObservableState = u64;

    fn name(&self) -> &'static str {
        "event-processor"
    }

    fn observable_state(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    async fn run(mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorExitStatus> {
        loop {
            tokio::select! {
                () = ctx.shutdown.cancelled() => return Ok(()),
                msg = ctx.mailbox.recv() => {
                    match msg {
                        Some(Envelope::Message(_)) => {
                            self.processed.fetch_add(1, Ordering::Relaxed);
                        }
                        Some(Envelope::Observe(tx)) => {
                            let _ = tx.send(self.observable_state());
                        }
                        Some(_) => {}
                        None => return Ok(()),
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn test_multi_actor_pipeline() {
    let processed_count = Arc::new(AtomicU64::new(0));
    let mut runtime = Runtime::new();

    let processor_handle = runtime.spawn(EventProcessor::new(processed_count.clone()), 100);

    let source = EventSource::new(50).with_downstream(processor_handle.sender().clone());
    let source_handle = runtime.spawn(source, 10);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = processed_count.load(Ordering::Relaxed);
    assert_eq!(count, 50, "expected 50 events processed, got {count}");

    let source_state = source_handle.observe().await.unwrap();
    assert_eq!(source_state, 50);

    assert_eq!(source_handle.check_health(), Health::Healthy);
    assert_eq!(processor_handle.check_health(), Health::Healthy);

    runtime.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_backpressure_via_bounded_channels() {
    let processed_count = Arc::new(AtomicU64::new(0));
    let mut runtime = Runtime::new();

    let processor_handle = runtime.spawn(EventProcessor::new(processed_count.clone()), 2);

    let source = EventSource::new(100).with_downstream(processor_handle.sender().clone());
    let _source_handle = runtime.spawn(source, 10);

    tokio::time::sleep(Duration::from_secs(5)).await;

    let count = processed_count.load(Ordering::Relaxed);
    assert_eq!(
        count, 100,
        "expected 100 events, got {count} (backpressure should not drop)"
    );

    runtime.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
}
