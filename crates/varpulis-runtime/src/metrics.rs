//! Prometheus metrics for Varpulis

use std::sync::Arc;

use prometheus::{CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Metrics collection for Varpulis engine
#[derive(Debug, Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    pub events_total: CounterVec,
    pub events_processed: CounterVec,
    pub output_events_total: CounterVec,
    pub processing_latency: HistogramVec,
    pub stream_queue_size: GaugeVec,
    pub active_streams: Gauge,
    pub dlq_events_total: prometheus::Counter,
    pub queue_pressure_ratio: GaugeVec,
    // Per-operator processing latency
    pub operator_latency: HistogramVec,
    // Pattern match counter
    pub pattern_matches_total: CounterVec,
    // Window fill level (current events in window)
    pub window_fill_level: GaugeVec,
    // Connector health (1 = healthy, 0 = unhealthy)
    pub connector_health: GaugeVec,
    // Events sent per sink connector
    pub connector_events_sent: CounterVec,
    // Events dropped due to backpressure
    pub backpressure_drops: CounterVec,
    // Per-tenant metrics (SaaS)
    pub tenant_events_total: CounterVec,
    pub tenant_events_rate: GaugeVec,
    pub tenant_pipelines_active: GaugeVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let events_total = CounterVec::new(
            Opts::new("varpulis_events_total", "Total events received"),
            &["event_type"],
        )
        .expect("failed to create events_total counter");

        let events_processed = CounterVec::new(
            Opts::new("varpulis_events_processed", "Events processed by stream"),
            &["stream"],
        )
        .expect("failed to create events_processed counter");

        let output_events_total = CounterVec::new(
            Opts::new(
                "varpulis_output_events_total",
                "Total output events emitted",
            ),
            &["stream", "event_type"],
        )
        .expect("failed to create output_events_total counter");

        let processing_latency = HistogramVec::new(
            HistogramOpts::new(
                "varpulis_processing_latency_seconds",
                "Event processing latency",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
            ]),
            &["stream"],
        )
        .expect("failed to create processing_latency histogram");

        let stream_queue_size = GaugeVec::new(
            Opts::new("varpulis_stream_queue_size", "Stream queue size"),
            &["stream"],
        )
        .expect("failed to create stream_queue_size gauge");

        let active_streams = Gauge::new("varpulis_active_streams", "Number of active streams")
            .expect("failed to create active_streams gauge");

        let dlq_events_total = prometheus::Counter::new(
            "varpulis_dlq_events_total",
            "Total events written to dead letter queue",
        )
        .expect("failed to create dlq_events_total counter");

        let queue_pressure_ratio = GaugeVec::new(
            Opts::new(
                "varpulis_queue_pressure_ratio",
                "Queue pressure ratio (pending_events / max_queue_depth)",
            ),
            &["stream"],
        )
        .expect("failed to create queue_pressure_ratio gauge");

        let operator_latency = HistogramVec::new(
            HistogramOpts::new(
                "varpulis_operator_latency_seconds",
                "Per-operator processing latency",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
            ]),
            &["stream", "operator"],
        )
        .expect("failed to create operator_latency histogram");

        let pattern_matches_total = CounterVec::new(
            Opts::new(
                "varpulis_pattern_matches_total",
                "Total pattern matches detected",
            ),
            &["stream"],
        )
        .expect("failed to create pattern_matches_total counter");

        let window_fill_level = GaugeVec::new(
            Opts::new(
                "varpulis_window_fill_level",
                "Current number of events in window",
            ),
            &["stream", "window_type"],
        )
        .expect("failed to create window_fill_level gauge");

        let connector_health = GaugeVec::new(
            Opts::new(
                "varpulis_connector_health",
                "Connector health status (1 = healthy, 0 = unhealthy)",
            ),
            &["connector", "type"],
        )
        .expect("failed to create connector_health gauge");

        let connector_events_sent = CounterVec::new(
            Opts::new(
                "varpulis_connector_events_sent",
                "Total events sent per sink connector",
            ),
            &["connector"],
        )
        .expect("failed to create connector_events_sent counter");

        let backpressure_drops = CounterVec::new(
            Opts::new(
                "varpulis_backpressure_drops",
                "Events dropped due to backpressure",
            ),
            &["stream"],
        )
        .expect("failed to create backpressure_drops counter");

        let tenant_events_total = CounterVec::new(
            Opts::new(
                "varpulis_tenant_events_total",
                "Total events processed per tenant",
            ),
            &["tenant_id"],
        )
        .expect("failed to create tenant_events_total counter");

        let tenant_events_rate = GaugeVec::new(
            Opts::new(
                "varpulis_tenant_events_rate",
                "Current events per second per tenant",
            ),
            &["tenant_id"],
        )
        .expect("failed to create tenant_events_rate gauge");

        let tenant_pipelines_active = GaugeVec::new(
            Opts::new(
                "varpulis_tenant_pipelines_active",
                "Number of active pipelines per tenant",
            ),
            &["tenant_id"],
        )
        .expect("failed to create tenant_pipelines_active gauge");

        registry
            .register(Box::new(events_total.clone()))
            .expect("failed to register events_total");
        registry
            .register(Box::new(events_processed.clone()))
            .expect("failed to register events_processed");
        registry
            .register(Box::new(output_events_total.clone()))
            .expect("failed to register output_events_total");
        registry
            .register(Box::new(processing_latency.clone()))
            .expect("failed to register processing_latency");
        registry
            .register(Box::new(stream_queue_size.clone()))
            .expect("failed to register stream_queue_size");
        registry
            .register(Box::new(active_streams.clone()))
            .expect("failed to register active_streams");
        registry
            .register(Box::new(dlq_events_total.clone()))
            .expect("failed to register dlq_events_total");
        registry
            .register(Box::new(queue_pressure_ratio.clone()))
            .expect("failed to register queue_pressure_ratio");
        registry
            .register(Box::new(operator_latency.clone()))
            .expect("failed to register operator_latency");
        registry
            .register(Box::new(pattern_matches_total.clone()))
            .expect("failed to register pattern_matches_total");
        registry
            .register(Box::new(window_fill_level.clone()))
            .expect("failed to register window_fill_level");
        registry
            .register(Box::new(connector_health.clone()))
            .expect("failed to register connector_health");
        registry
            .register(Box::new(connector_events_sent.clone()))
            .expect("failed to register connector_events_sent");
        registry
            .register(Box::new(backpressure_drops.clone()))
            .expect("failed to register backpressure_drops");
        registry
            .register(Box::new(tenant_events_total.clone()))
            .expect("failed to register tenant_events_total");
        registry
            .register(Box::new(tenant_events_rate.clone()))
            .expect("failed to register tenant_events_rate");
        registry
            .register(Box::new(tenant_pipelines_active.clone()))
            .expect("failed to register tenant_pipelines_active");

        Self {
            registry: Arc::new(registry),
            events_total,
            events_processed,
            output_events_total,
            processing_latency,
            stream_queue_size,
            active_streams,
            dlq_events_total,
            queue_pressure_ratio,
            operator_latency,
            pattern_matches_total,
            window_fill_level,
            connector_health,
            connector_events_sent,
            backpressure_drops,
            tenant_events_total,
            tenant_events_rate,
            tenant_pipelines_active,
        }
    }

    /// Record an incoming event
    pub fn record_event(&self, event_type: &str) {
        self.events_total.with_label_values(&[event_type]).inc();
    }

    /// Record event processing
    pub fn record_processing(&self, stream: &str, latency_secs: f64) {
        self.events_processed.with_label_values(&[stream]).inc();
        self.processing_latency
            .with_label_values(&[stream])
            .observe(latency_secs);
    }

    /// Record an output event
    pub fn record_output_event(&self, stream: &str, event_type: &str) {
        self.output_events_total
            .with_label_values(&[stream, event_type])
            .inc();
    }

    /// Set stream count
    pub fn set_stream_count(&self, count: usize) {
        self.active_streams.set(count as f64);
    }

    /// Record a tenant event (SaaS per-tenant metrics)
    pub fn record_tenant_event(&self, tenant_id: &str) {
        self.tenant_events_total
            .with_label_values(&[tenant_id])
            .inc();
    }

    /// Set tenant event rate
    pub fn set_tenant_event_rate(&self, tenant_id: &str, rate: f64) {
        self.tenant_events_rate
            .with_label_values(&[tenant_id])
            .set(rate);
    }

    /// Record per-operator latency
    pub fn record_operator_latency(&self, stream: &str, operator: &str, latency_secs: f64) {
        self.operator_latency
            .with_label_values(&[stream, operator])
            .observe(latency_secs);
    }

    /// Record a pattern match
    pub fn record_pattern_match(&self, stream: &str) {
        self.pattern_matches_total
            .with_label_values(&[stream])
            .inc();
    }

    /// Set the window fill level
    pub fn set_window_fill_level(&self, stream: &str, window_type: &str, level: f64) {
        self.window_fill_level
            .with_label_values(&[stream, window_type])
            .set(level);
    }

    /// Set connector health status
    pub fn set_connector_health(&self, connector: &str, connector_type: &str, healthy: bool) {
        self.connector_health
            .with_label_values(&[connector, connector_type])
            .set(if healthy { 1.0 } else { 0.0 });
    }

    /// Record events sent by a connector
    pub fn record_connector_event_sent(&self, connector: &str) {
        self.connector_events_sent
            .with_label_values(&[connector])
            .inc();
    }

    /// Record a backpressure drop
    pub fn record_backpressure_drop(&self, stream: &str) {
        self.backpressure_drops.with_label_values(&[stream]).inc();
    }

    /// Set tenant active pipeline count
    pub fn set_tenant_pipelines(&self, tenant_id: &str, count: usize) {
        self.tenant_pipelines_active
            .with_label_values(&[tenant_id])
            .set(count as f64);
    }

    /// Get Prometheus text output
    pub fn gather(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP server for Prometheus metrics endpoint
#[derive(Debug)]
pub struct MetricsServer {
    metrics: Metrics,
    addr: String,
}

impl MetricsServer {
    pub fn new(metrics: Metrics, addr: impl Into<String>) -> Self {
        Self {
            metrics,
            addr: addr.into(),
        }
    }

    /// Run the metrics HTTP server
    pub async fn run(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("Metrics server listening on http://{}/metrics", self.addr);

        loop {
            let (mut socket, _addr) = listener.accept().await?;

            let metrics_output = self.metrics.gather();

            // Simple HTTP response
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                metrics_output.len(),
                metrics_output
            );

            if let Err(e) = socket.write_all(response.as_bytes()).await {
                error!("Failed to write response: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics() {
        let metrics = Metrics::new();
        metrics.record_event("TestEvent");
        metrics.record_processing("test_stream", 0.001);
        metrics.record_output_event("test_stream", "TestOutput");
        metrics.set_stream_count(5);

        let output = metrics.gather();
        assert!(output.contains("varpulis_events_total"));
        assert!(output.contains("varpulis_output_events_total"));
    }

    #[test]
    fn test_metrics_default() {
        let metrics = Metrics::default();
        metrics.record_event("Test");
        let output = metrics.gather();
        assert!(output.contains("varpulis_events_total"));
    }

    #[test]
    fn test_metrics_multiple_events() {
        let metrics = Metrics::new();
        for i in 0..10 {
            metrics.record_event(&format!("Event{i}"));
        }
        let output = metrics.gather();
        assert!(output.contains("varpulis_events_total"));
    }

    #[test]
    fn test_metrics_multiple_output_events() {
        let metrics = Metrics::new();
        metrics.record_output_event("stream1", "OutputA");
        metrics.record_output_event("stream1", "OutputB");
        metrics.record_output_event("stream2", "OutputC");
        let output = metrics.gather();
        assert!(output.contains("varpulis_output_events_total"));
    }

    #[test]
    fn test_metrics_processing_histogram() {
        let metrics = Metrics::new();
        metrics.record_processing("stream1", 0.001);
        metrics.record_processing("stream1", 0.002);
        metrics.record_processing("stream2", 0.005);
        let output = metrics.gather();
        assert!(output.contains("varpulis_processing_latency_seconds"));
    }

    #[test]
    fn test_metrics_server_new() {
        let metrics = Metrics::new();
        let server = MetricsServer::new(metrics, "127.0.0.1:0");
        assert_eq!(server.addr, "127.0.0.1:0");
    }

    #[test]
    fn test_metrics_stream_queue_size() {
        let metrics = Metrics::new();
        metrics
            .stream_queue_size
            .with_label_values(&["stream1"])
            .set(100.0);
        metrics
            .stream_queue_size
            .with_label_values(&["stream2"])
            .set(50.0);

        let output = metrics.gather();
        assert!(output.contains("varpulis_stream_queue_size"));
    }

    #[test]
    fn test_metrics_active_streams() {
        let metrics = Metrics::new();
        metrics.set_stream_count(10);

        let output = metrics.gather();
        assert!(output.contains("varpulis_active_streams"));
    }

    #[test]
    fn test_metrics_latency_buckets() {
        let metrics = Metrics::new();

        // Record latencies in different buckets
        metrics.record_processing("fast", 0.0001); // < 0.1ms
        metrics.record_processing("fast", 0.0005); // < 0.5ms
        metrics.record_processing("medium", 0.01); // 10ms
        metrics.record_processing("slow", 0.5); // 500ms

        let output = metrics.gather();
        assert!(output.contains("varpulis_processing_latency_seconds_bucket"));
    }

    #[test]
    fn test_metrics_event_types() {
        let metrics = Metrics::new();

        metrics.record_event("TemperatureReading");
        metrics.record_event("TemperatureReading");
        metrics.record_event("HumidityReading");
        metrics.record_event("PressureReading");

        let output = metrics.gather();
        assert!(output.contains("TemperatureReading"));
        assert!(output.contains("HumidityReading"));
        assert!(output.contains("PressureReading"));
    }

    #[test]
    fn test_metrics_output_event_streams() {
        let metrics = Metrics::new();

        metrics.record_output_event("HighTempAlert", "HighTemp");
        metrics.record_output_event("HumidityAlert", "LowHumidity");
        metrics.record_output_event("SystemHealth", "HealthCheck");

        let output = metrics.gather();
        assert!(output.contains("HighTempAlert"));
        assert!(output.contains("HumidityAlert"));
        assert!(output.contains("SystemHealth"));
    }

    #[test]
    fn test_metrics_clone() {
        let metrics1 = Metrics::new();
        metrics1.record_event("TestEvent");

        let metrics2 = metrics1;
        metrics2.record_event("AnotherEvent");

        // Both should see all events (they share the same registry)
        let output = metrics2.gather();
        assert!(output.contains("TestEvent"));
        assert!(output.contains("AnotherEvent"));
    }

    #[test]
    fn test_metrics_server_with_string() {
        let metrics = Metrics::new();
        let addr = String::from("0.0.0.0:9090");
        let server = MetricsServer::new(metrics, addr);
        assert_eq!(server.addr, "0.0.0.0:9090");
    }

    #[test]
    fn test_metrics_many_streams() {
        let metrics = Metrics::new();

        for i in 0..20 {
            let stream_name = format!("stream_{i}");
            metrics.record_processing(&stream_name, 0.001 * i as f64);
        }

        let output = metrics.gather();
        assert!(output.contains("stream_0"));
        assert!(output.contains("stream_19"));
    }

    #[test]
    fn test_queue_pressure_ratio_gauge() {
        let metrics = Metrics::new();
        metrics
            .queue_pressure_ratio
            .with_label_values(&["_all"])
            .set(0.75);
        let output = metrics.gather();
        assert!(output.contains("varpulis_queue_pressure_ratio"));
    }
}
