//! Comprehensive SASE+ metrics (MET-01)

use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Latency histogram with exponential buckets
/// Buckets: <1µs, <10µs, <100µs, <1ms, <10ms, <100ms, <1s, >1s
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; 8],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl LatencyHistogram {
    /// Create a new latency histogram
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a latency measurement
    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;

        let bucket_idx = match micros {
            0..=1 => 0,
            2..=10 => 1,
            11..=100 => 2,
            101..=1_000 => 3,
            1_001..=10_000 => 4,
            10_001..=100_000 => 5,
            100_001..=1_000_000 => 6,
            _ => 7,
        };

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get approximate percentile value
    pub fn percentile(&self, p: f64) -> Duration {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return Duration::ZERO;
        }

        let target = (total as f64 * p) as u64;
        let bucket_maxes = [1u64, 10, 100, 1_000, 10_000, 100_000, 1_000_000, u64::MAX];

        let mut cumulative = 0u64;
        for (idx, &max_micros) in bucket_maxes.iter().enumerate() {
            cumulative += self.buckets[idx].load(Ordering::Relaxed);
            if cumulative >= target {
                return Duration::from_micros(max_micros.min(1_000_000));
            }
        }

        Duration::from_secs(1)
    }

    /// Get the mean latency
    pub fn mean(&self) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        Duration::from_micros(self.sum_micros.load(Ordering::Relaxed) / count)
    }

    /// Get total count of measurements
    pub fn total_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get bucket counts (for Prometheus histogram format)
    pub fn bucket_counts(&self) -> [u64; 8] {
        let mut counts = [0u64; 8];
        for (i, bucket) in self.buckets.iter().enumerate() {
            counts[i] = bucket.load(Ordering::Relaxed);
        }
        counts
    }
}

/// Comprehensive metrics for the SASE+ engine
#[derive(Debug, Default)]
pub struct SaseMetrics {
    // === Event counters ===
    /// Total events processed
    pub events_processed: AtomicU64,
    /// Events that triggered at least one transition
    pub events_matched: AtomicU64,
    /// Events that were ignored (no run interested)
    pub events_ignored: AtomicU64,
    /// Events that were late but accepted
    pub events_late_accepted: AtomicU64,
    /// Events that were too late and dropped
    pub events_late_dropped: AtomicU64,

    // === Run counters ===
    /// Total runs created
    pub runs_created: AtomicU64,
    /// Runs that completed successfully (matches emitted)
    pub runs_completed: AtomicU64,
    /// Runs that expired due to timeout
    pub runs_expired: AtomicU64,
    /// Runs invalidated by negation
    pub runs_invalidated: AtomicU64,
    /// Runs dropped due to backpressure
    pub runs_dropped: AtomicU64,
    /// Runs evicted due to backpressure
    pub runs_evicted: AtomicU64,

    // === Match counters ===
    /// Total matches emitted
    pub matches_emitted: AtomicU64,

    // === Latency histograms ===
    /// Event processing latency
    pub event_latency: LatencyHistogram,
    /// Match duration (from first to last event)
    pub match_duration: LatencyHistogram,

    // === Memory metrics ===
    /// Peak active runs observed
    pub peak_active_runs: AtomicU64,

    // === Timestamps ===
    /// Last event timestamp (epoch millis)
    pub last_event_time: AtomicU64,
    /// Current watermark (epoch millis)
    pub current_watermark: AtomicU64,
}

impl SaseMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event being processed
    pub fn record_event_processed(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an event that matched
    pub fn record_event_matched(&self) {
        self.events_matched.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an event that was ignored
    pub fn record_event_ignored(&self) {
        self.events_ignored.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a late event that was accepted
    pub fn record_late_event_accepted(&self) {
        self.events_late_accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a late event that was dropped
    pub fn record_late_event_dropped(&self) {
        self.events_late_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being created
    pub fn record_run_created(&self) {
        self.runs_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run completing
    pub fn record_run_completed(&self) {
        self.runs_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run expiring
    pub fn record_run_expired(&self) {
        self.runs_expired.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being invalidated
    pub fn record_run_invalidated(&self) {
        self.runs_invalidated.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being dropped
    pub fn record_run_dropped(&self) {
        self.runs_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being evicted
    pub fn record_run_evicted(&self) {
        self.runs_evicted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record matches emitted
    pub fn record_matches(&self, count: usize) {
        self.matches_emitted
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Record event processing latency
    pub fn record_event_latency(&self, latency: Duration) {
        self.event_latency.record(latency);
    }

    /// Record match duration
    pub fn record_match_duration(&self, duration: Duration) {
        self.match_duration.record(duration);
    }

    /// Update peak active runs
    pub fn update_peak_runs(&self, current: usize) {
        self.peak_active_runs
            .fetch_max(current as u64, Ordering::Relaxed);
    }

    /// Update last event time
    pub fn update_last_event_time(&self, timestamp: DateTime<Utc>) {
        self.last_event_time
            .store(timestamp.timestamp_millis() as u64, Ordering::Relaxed);
    }

    /// Update current watermark
    pub fn update_watermark(&self, watermark: DateTime<Utc>) {
        self.current_watermark
            .store(watermark.timestamp_millis() as u64, Ordering::Relaxed);
    }

    /// Export metrics in Prometheus format
    pub fn to_prometheus(&self, prefix: &str) -> String {
        let mut output = String::new();

        // Event counters
        output.push_str(&format!(
            "# HELP {prefix}_events_total Total events processed\n\
             # TYPE {prefix}_events_total counter\n\
             {prefix}_events_total {}\n\n",
            self.events_processed.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_matched Events that triggered transitions\n\
             # TYPE {prefix}_events_matched counter\n\
             {prefix}_events_matched {}\n\n",
            self.events_matched.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_ignored Events ignored (no interest)\n\
             # TYPE {prefix}_events_ignored counter\n\
             {prefix}_events_ignored {}\n\n",
            self.events_ignored.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_late_accepted Late events still processed\n\
             # TYPE {prefix}_events_late_accepted counter\n\
             {prefix}_events_late_accepted {}\n\n",
            self.events_late_accepted.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_late_dropped Late events dropped\n\
             # TYPE {prefix}_events_late_dropped counter\n\
             {prefix}_events_late_dropped {}\n\n",
            self.events_late_dropped.load(Ordering::Relaxed)
        ));

        // Run counters
        output.push_str(&format!(
            "# HELP {prefix}_runs_created Total runs created\n\
             # TYPE {prefix}_runs_created counter\n\
             {prefix}_runs_created {}\n\n",
            self.runs_created.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_completed Runs completed successfully\n\
             # TYPE {prefix}_runs_completed counter\n\
             {prefix}_runs_completed {}\n\n",
            self.runs_completed.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_expired Runs expired by timeout\n\
             # TYPE {prefix}_runs_expired counter\n\
             {prefix}_runs_expired {}\n\n",
            self.runs_expired.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_invalidated Runs invalidated by negation\n\
             # TYPE {prefix}_runs_invalidated counter\n\
             {prefix}_runs_invalidated {}\n\n",
            self.runs_invalidated.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_dropped Runs dropped by backpressure\n\
             # TYPE {prefix}_runs_dropped counter\n\
             {prefix}_runs_dropped {}\n\n",
            self.runs_dropped.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_evicted Runs evicted by backpressure\n\
             # TYPE {prefix}_runs_evicted counter\n\
             {prefix}_runs_evicted {}\n\n",
            self.runs_evicted.load(Ordering::Relaxed)
        ));

        // Matches
        output.push_str(&format!(
            "# HELP {prefix}_matches_total Total matches emitted\n\
             # TYPE {prefix}_matches_total counter\n\
             {prefix}_matches_total {}\n\n",
            self.matches_emitted.load(Ordering::Relaxed)
        ));

        // Peak runs
        output.push_str(&format!(
            "# HELP {prefix}_peak_active_runs Peak concurrent active runs\n\
             # TYPE {prefix}_peak_active_runs gauge\n\
             {prefix}_peak_active_runs {}\n\n",
            self.peak_active_runs.load(Ordering::Relaxed)
        ));

        // Latency percentiles
        output.push_str(&format!(
            "# HELP {prefix}_event_latency_p50_us Event processing latency p50 in microseconds\n\
             # TYPE {prefix}_event_latency_p50_us gauge\n\
             {prefix}_event_latency_p50_us {}\n\n",
            self.event_latency.percentile(0.5).as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_event_latency_p99_us Event processing latency p99 in microseconds\n\
             # TYPE {prefix}_event_latency_p99_us gauge\n\
             {prefix}_event_latency_p99_us {}\n\n",
            self.event_latency.percentile(0.99).as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_event_latency_mean_us Event processing latency mean in microseconds\n\
             # TYPE {prefix}_event_latency_mean_us gauge\n\
             {prefix}_event_latency_mean_us {}\n\n",
            self.event_latency.mean().as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_match_duration_p50_us Match duration p50 in microseconds\n\
             # TYPE {prefix}_match_duration_p50_us gauge\n\
             {prefix}_match_duration_p50_us {}\n\n",
            self.match_duration.percentile(0.5).as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_match_duration_p99_us Match duration p99 in microseconds\n\
             # TYPE {prefix}_match_duration_p99_us gauge\n\
             {prefix}_match_duration_p99_us {}\n\n",
            self.match_duration.percentile(0.99).as_micros()
        ));

        // Watermark
        output.push_str(&format!(
            "# HELP {prefix}_watermark_epoch_ms Current watermark epoch milliseconds\n\
             # TYPE {prefix}_watermark_epoch_ms gauge\n\
             {prefix}_watermark_epoch_ms {}\n\n",
            self.current_watermark.load(Ordering::Relaxed)
        ));

        output
    }

    /// Get a summary of current metrics
    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_matched: self.events_matched.load(Ordering::Relaxed),
            events_ignored: self.events_ignored.load(Ordering::Relaxed),
            events_late_accepted: self.events_late_accepted.load(Ordering::Relaxed),
            events_late_dropped: self.events_late_dropped.load(Ordering::Relaxed),
            runs_created: self.runs_created.load(Ordering::Relaxed),
            runs_completed: self.runs_completed.load(Ordering::Relaxed),
            runs_expired: self.runs_expired.load(Ordering::Relaxed),
            runs_invalidated: self.runs_invalidated.load(Ordering::Relaxed),
            runs_dropped: self.runs_dropped.load(Ordering::Relaxed),
            runs_evicted: self.runs_evicted.load(Ordering::Relaxed),
            matches_emitted: self.matches_emitted.load(Ordering::Relaxed),
            peak_active_runs: self.peak_active_runs.load(Ordering::Relaxed),
            event_latency_p50_us: self.event_latency.percentile(0.5).as_micros() as u64,
            event_latency_p99_us: self.event_latency.percentile(0.99).as_micros() as u64,
            event_latency_mean_us: self.event_latency.mean().as_micros() as u64,
        }
    }
}

/// Summary of metrics for easier access
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub events_processed: u64,
    pub events_matched: u64,
    pub events_ignored: u64,
    pub events_late_accepted: u64,
    pub events_late_dropped: u64,
    pub runs_created: u64,
    pub runs_completed: u64,
    pub runs_expired: u64,
    pub runs_invalidated: u64,
    pub runs_dropped: u64,
    pub runs_evicted: u64,
    pub matches_emitted: u64,
    pub peak_active_runs: u64,
    pub event_latency_p50_us: u64,
    pub event_latency_p99_us: u64,
    pub event_latency_mean_us: u64,
}
