//! Time-accelerated testing infrastructure
//!
//! Provides [`SimulatedScheduler`] for deterministic timer simulation and
//! [`TestUniverse`] for end-to-end VPL program testing without real-time waits.
//!
//! # Example
//!
//! ```rust,no_run
//! use varpulis_runtime::testing::TestUniverse;
//! use std::time::Duration;
//!
//! # async fn example() {
//! let mut universe = TestUniverse::from_source(r#"
//!     stream heartbeat = timer(10ms).emit(type: "heartbeat")
//! "#).unwrap();
//!
//! let events = universe.advance(Duration::from_millis(35)).await;
//! assert_eq!(events.len(), 3); // 10ms, 20ms, 30ms — deterministic
//! # }
//! ```

use crate::engine::Engine;
use crate::event::Event;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::time::Duration;
use tokio::sync::mpsc;

/// Internal timer state for the simulated scheduler.
struct TimerState {
    /// Interval between fires in nanoseconds
    interval_ns: u64,
    /// Event type emitted on fire
    event_type: String,
    /// Absolute time of next fire
    next_fire: DateTime<Utc>,
}

/// Simulated scheduler that generates timer events deterministically.
///
/// Instead of waiting for real time to pass, `advance()` jumps the clock
/// forward and collects all timer events that would have fired in that window.
pub struct SimulatedScheduler {
    timers: Vec<TimerState>,
    current_time: DateTime<Utc>,
}

impl SimulatedScheduler {
    /// Create a new scheduler starting at the given time.
    pub const fn new(start_time: DateTime<Utc>) -> Self {
        Self {
            timers: Vec::new(),
            current_time: start_time,
        }
    }

    /// Register timers from engine configuration.
    ///
    /// Each tuple is `(interval_ns, initial_delay_ns, event_type)`.
    pub fn register_timers(&mut self, configs: Vec<(u64, Option<u64>, String)>) {
        for (interval_ns, initial_delay_ns, event_type) in configs {
            let delay_ns = initial_delay_ns.unwrap_or(0);
            let first_fire = self.current_time
                + ChronoDuration::nanoseconds(delay_ns as i64)
                + ChronoDuration::nanoseconds(interval_ns as i64);

            self.timers.push(TimerState {
                interval_ns,
                event_type,
                next_fire: first_fire,
            });
        }
    }

    /// Advance the clock by `duration` and return all timer events that fire.
    ///
    /// Events are returned in chronological order. Multiple events from the
    /// same timer are generated if the advance spans multiple intervals.
    pub fn advance(&mut self, duration: Duration) -> Vec<Event> {
        let target_time =
            self.current_time + ChronoDuration::nanoseconds(duration.as_nanos() as i64);
        let mut events = Vec::new();

        for timer in &mut self.timers {
            while timer.next_fire <= target_time {
                let mut event = Event::new(timer.event_type.clone());
                event.data.insert(
                    "timestamp".into(),
                    varpulis_core::Value::Int(timer.next_fire.timestamp_millis()),
                );
                events.push((timer.next_fire, event));
                timer.next_fire += ChronoDuration::nanoseconds(timer.interval_ns as i64);
            }
        }

        // Sort by fire time for deterministic ordering
        events.sort_by_key(|(t, _)| *t);

        self.current_time = target_time;

        events.into_iter().map(|(_, e)| e).collect()
    }

    /// Get the current simulated time.
    pub const fn now(&self) -> DateTime<Utc> {
        self.current_time
    }
}

/// Test universe for end-to-end VPL program testing.
///
/// Combines an [`Engine`] with a [`SimulatedScheduler`] to enable deterministic,
/// instant testing of VPL programs that include timer-based streams.
pub struct TestUniverse {
    engine: Engine,
    scheduler: SimulatedScheduler,
    output_rx: mpsc::Receiver<Event>,
}

impl TestUniverse {
    /// Create a test universe from VPL source code.
    ///
    /// Parses the source, creates an engine, and registers any timer streams
    /// with the simulated scheduler.
    pub fn from_source(vpl_source: &str) -> Result<Self, String> {
        let program =
            varpulis_parser::parse(vpl_source).map_err(|e| format!("parse error: {e}"))?;

        let (output_tx, output_rx) = mpsc::channel(10_000);
        let mut engine = Engine::new(output_tx);
        engine
            .load(&program)
            .map_err(|e| format!("load error: {e}"))?;

        let start_time = Utc::now();
        let mut scheduler = SimulatedScheduler::new(start_time);

        // Extract timer configs from the engine
        let timer_configs = engine.get_timers();
        scheduler.register_timers(timer_configs);

        Ok(Self {
            engine,
            scheduler,
            output_rx,
        })
    }

    /// Send an event into the engine for processing.
    pub async fn send(&mut self, event: Event) -> Result<(), String> {
        self.engine
            .process(event)
            .await
            .map_err(|e| format!("process error: {e}"))?;
        Ok(())
    }

    /// Create and send an event with the given type (no extra fields).
    pub async fn send_at_now(&mut self, event_type: &str) -> Result<(), String> {
        let event = Event::new(event_type);
        self.send(event).await
    }

    /// Advance simulated time and process all timer events that fire.
    ///
    /// Returns all output events produced during the advance.
    pub async fn advance(&mut self, duration: Duration) -> Vec<Event> {
        let timer_events = self.scheduler.advance(duration);

        // Feed timer events through the engine
        for event in timer_events {
            let _ = self.engine.process(event).await;
        }

        // Drain output
        self.drain_output()
    }

    /// Drain all currently buffered output events.
    pub fn drain_output(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Ok(event) = self.output_rx.try_recv() {
            events.push(event);
        }
        events
    }

    /// Get the current simulated time.
    pub const fn now(&self) -> DateTime<Utc> {
        self.scheduler.now()
    }

    /// Get a reference to the underlying engine.
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get a mutable reference to the underlying engine.
    pub const fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulated_scheduler_basic() {
        let start = Utc::now();
        let mut scheduler = SimulatedScheduler::new(start);
        scheduler.register_timers(vec![(
            10_000_000, // 10ms
            None,
            "Timer".to_string(),
        )]);

        // Advance 35ms — should get 3 timer events (10ms, 20ms, 30ms)
        let events = scheduler.advance(Duration::from_millis(35));
        assert_eq!(events.len(), 3);
        for event in &events {
            assert_eq!(event.event_type.as_ref(), "Timer");
        }
    }

    #[test]
    fn test_simulated_scheduler_with_delay() {
        let start = Utc::now();
        let mut scheduler = SimulatedScheduler::new(start);
        scheduler.register_timers(vec![(
            10_000_000,       // 10ms interval
            Some(20_000_000), // 20ms initial delay
            "Delayed".to_string(),
        )]);

        // Advance 25ms — first fire at 30ms (20ms delay + 10ms), so 0 events
        let events = scheduler.advance(Duration::from_millis(25));
        assert_eq!(events.len(), 0);

        // Advance 10 more ms (total 35ms) — first fire at 30ms, so 1 event
        let events = scheduler.advance(Duration::from_millis(10));
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_simulated_scheduler_multiple_timers() {
        let start = Utc::now();
        let mut scheduler = SimulatedScheduler::new(start);
        scheduler.register_timers(vec![
            (10_000_000, None, "Fast".to_string()),
            (20_000_000, None, "Slow".to_string()),
        ]);

        // Advance 25ms: Fast fires at 10, 20; Slow fires at 20
        let events = scheduler.advance(Duration::from_millis(25));
        assert_eq!(events.len(), 3);

        let fast_count = events
            .iter()
            .filter(|e| e.event_type.as_ref() == "Fast")
            .count();
        let slow_count = events
            .iter()
            .filter(|e| e.event_type.as_ref() == "Slow")
            .count();
        assert_eq!(fast_count, 2);
        assert_eq!(slow_count, 1);
    }

    #[test]
    fn test_simulated_scheduler_zero_advance() {
        let start = Utc::now();
        let mut scheduler = SimulatedScheduler::new(start);
        scheduler.register_timers(vec![(10_000_000, None, "Timer".to_string())]);

        let events = scheduler.advance(Duration::ZERO);
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_simulated_scheduler_now_advances() {
        let start = Utc::now();
        let mut scheduler = SimulatedScheduler::new(start);
        assert_eq!(scheduler.now(), start);

        scheduler.advance(Duration::from_secs(1));
        assert!(scheduler.now() > start);
    }

    #[tokio::test]
    async fn test_universe_basic_stream() {
        let mut universe = TestUniverse::from_source(
            r"
            stream HighTemp = SensorReading
                .where(temperature > 100)
                .emit(sensor: sensor_id, temp: temperature)
            ",
        )
        .unwrap();

        let event = Event::new("SensorReading")
            .with_field("temperature", 105)
            .with_field("sensor_id", "S1");
        universe.send(event).await.unwrap();

        let outputs = universe.drain_output();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].event_type.as_ref(), "HighTemp");
    }

    #[tokio::test]
    async fn test_universe_filters_correctly() {
        let mut universe = TestUniverse::from_source(
            r"
            stream HighTemp = SensorReading
                .where(temperature > 100)
                .emit(temp: temperature)
            ",
        )
        .unwrap();

        // Below threshold — should not produce output
        let event = Event::new("SensorReading").with_field("temperature", 50);
        universe.send(event).await.unwrap();

        let outputs = universe.drain_output();
        assert_eq!(outputs.len(), 0);
    }
}
