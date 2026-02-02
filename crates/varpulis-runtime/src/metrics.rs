//! Prometheus metrics for Varpulis

use prometheus::{CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Metrics collection for Varpulis engine
#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    pub events_total: CounterVec,
    pub events_processed: CounterVec,
    pub alerts_total: CounterVec,
    pub processing_latency: HistogramVec,
    pub stream_queue_size: GaugeVec,
    pub active_streams: Gauge,
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

        let alerts_total = CounterVec::new(
            Opts::new("varpulis_alerts_total", "Total alerts generated"),
            &["alert_type", "severity"],
        )
        .expect("failed to create alerts_total counter");

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

        registry
            .register(Box::new(events_total.clone()))
            .expect("failed to register events_total");
        registry
            .register(Box::new(events_processed.clone()))
            .expect("failed to register events_processed");
        registry
            .register(Box::new(alerts_total.clone()))
            .expect("failed to register alerts_total");
        registry
            .register(Box::new(processing_latency.clone()))
            .expect("failed to register processing_latency");
        registry
            .register(Box::new(stream_queue_size.clone()))
            .expect("failed to register stream_queue_size");
        registry
            .register(Box::new(active_streams.clone()))
            .expect("failed to register active_streams");

        Self {
            registry: Arc::new(registry),
            events_total,
            events_processed,
            alerts_total,
            processing_latency,
            stream_queue_size,
            active_streams,
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

    /// Record an alert
    pub fn record_alert(&self, alert_type: &str, severity: &str) {
        self.alerts_total
            .with_label_values(&[alert_type, severity])
            .inc();
    }

    /// Set stream count
    pub fn set_stream_count(&self, count: usize) {
        self.active_streams.set(count as f64);
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
    pub async fn run(&self) -> anyhow::Result<()> {
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
        metrics.record_alert("test_alert", "warning");
        metrics.set_stream_count(5);

        let output = metrics.gather();
        assert!(output.contains("varpulis_events_total"));
        assert!(output.contains("varpulis_alerts_total"));
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
            metrics.record_event(&format!("Event{}", i));
        }
        let output = metrics.gather();
        assert!(output.contains("varpulis_events_total"));
    }

    #[test]
    fn test_metrics_multiple_alerts() {
        let metrics = Metrics::new();
        metrics.record_alert("critical", "high");
        metrics.record_alert("warning", "medium");
        metrics.record_alert("info", "low");
        let output = metrics.gather();
        assert!(output.contains("varpulis_alerts_total"));
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
        metrics.record_event("Alert");

        let output = metrics.gather();
        assert!(output.contains("TemperatureReading"));
        assert!(output.contains("HumidityReading"));
        assert!(output.contains("Alert"));
    }

    #[test]
    fn test_metrics_alert_severities() {
        let metrics = Metrics::new();

        metrics.record_alert("temperature_high", "critical");
        metrics.record_alert("humidity_low", "warning");
        metrics.record_alert("system_health", "info");

        let output = metrics.gather();
        assert!(output.contains("critical"));
        assert!(output.contains("warning"));
        assert!(output.contains("info"));
    }

    #[test]
    fn test_metrics_clone() {
        let metrics1 = Metrics::new();
        metrics1.record_event("TestEvent");

        let metrics2 = metrics1.clone();
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
            let stream_name = format!("stream_{}", i);
            metrics.record_processing(&stream_name, 0.001 * i as f64);
        }

        let output = metrics.gather();
        assert!(output.contains("stream_0"));
        assert!(output.contains("stream_19"));
    }
}
