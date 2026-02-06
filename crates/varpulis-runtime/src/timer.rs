//! Timer module for periodic event generation
//!
//! This module provides functionality for spawning timer tasks that
//! periodically generate events, similar to Apama's `on wait(period)`.

use crate::event::Event;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::debug;

/// Spawn a timer task that periodically sends timer events
///
/// # Arguments
/// * `interval_ns` - Interval between timer fires in nanoseconds
/// * `initial_delay_ns` - Optional initial delay before first fire in nanoseconds
/// * `timer_event_type` - Event type name for timer events
/// * `event_tx` - Channel to send timer events
///
/// # Returns
/// A JoinHandle for the spawned timer task
pub fn spawn_timer(
    interval_ns: u64,
    initial_delay_ns: Option<u64>,
    timer_event_type: String,
    event_tx: mpsc::Sender<Event>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Apply initial delay if specified
        if let Some(delay_ns) = initial_delay_ns {
            let delay = Duration::from_nanos(delay_ns);
            debug!(
                "Timer {} waiting for initial delay: {:?}",
                timer_event_type, delay
            );
            tokio::time::sleep(delay).await;
        }

        let interval = Duration::from_nanos(interval_ns);
        debug!(
            "Timer {} starting with interval: {:?}",
            timer_event_type, interval
        );

        let mut interval_timer = tokio::time::interval(interval);
        // Skip the immediate first tick
        interval_timer.tick().await;

        loop {
            interval_timer.tick().await;

            // Create timer event
            let mut event = Event::new(timer_event_type.clone());
            event.data.insert(
                "timestamp".to_string(),
                varpulis_core::Value::Int(chrono::Utc::now().timestamp_millis()),
            );

            debug!("Timer {} fired", timer_event_type);

            // Send timer event
            if event_tx.send(event).await.is_err() {
                debug!("Timer {} stopping: channel closed", timer_event_type);
                break;
            }
        }
    })
}

/// Timer manager that tracks spawned timer tasks
pub struct TimerManager {
    handles: Vec<JoinHandle<()>>,
}

impl TimerManager {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    /// Spawn timers from engine configuration
    pub fn spawn_timers(
        &mut self,
        timers: Vec<(u64, Option<u64>, String)>,
        event_tx: mpsc::Sender<Event>,
    ) {
        for (interval_ns, initial_delay_ns, timer_event_type) in timers {
            let handle = spawn_timer(
                interval_ns,
                initial_delay_ns,
                timer_event_type,
                event_tx.clone(),
            );
            self.handles.push(handle);
        }
    }

    /// Stop all timer tasks
    pub fn stop_all(&mut self) {
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }
}

impl Default for TimerManager {
    fn default() -> Self {
        Self::new()
    }
}
