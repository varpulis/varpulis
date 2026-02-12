//! Prometheus metrics for Varpulis cluster operations.

use prometheus::{CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};
use std::sync::Arc;

/// Cluster-specific Prometheus metrics.
#[derive(Clone)]
pub struct ClusterPrometheusMetrics {
    registry: Arc<Registry>,
    /// Number of workers by status (ready, unhealthy, draining, registering).
    pub workers_total: GaugeVec,
    /// Number of deployed pipeline groups.
    pub pipeline_groups_total: Gauge,
    /// Number of pipeline deployments.
    pub deployments_total: Gauge,
    /// Migration counter by result (success, failure).
    pub migrations_total: CounterVec,
    /// Migration duration in seconds.
    pub migration_duration_seconds: HistogramVec,
    /// Health sweep duration in seconds.
    pub health_sweep_duration_seconds: HistogramVec,
    /// Deploy duration in seconds.
    pub deploy_duration_seconds: HistogramVec,
}

impl ClusterPrometheusMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let workers_total = GaugeVec::new(
            Opts::new(
                "varpulis_cluster_workers_total",
                "Number of workers by status",
            ),
            &["status"],
        )
        .expect("failed to create workers_total gauge");

        let pipeline_groups_total = Gauge::new(
            "varpulis_cluster_pipeline_groups_total",
            "Number of deployed pipeline groups",
        )
        .expect("failed to create pipeline_groups_total gauge");

        let deployments_total = Gauge::new(
            "varpulis_cluster_deployments_total",
            "Number of pipeline deployments",
        )
        .expect("failed to create deployments_total gauge");

        let migrations_total = CounterVec::new(
            Opts::new(
                "varpulis_cluster_migrations_total",
                "Total migrations by result",
            ),
            &["result"],
        )
        .expect("failed to create migrations_total counter");

        let migration_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "varpulis_cluster_migration_duration_seconds",
                "Migration duration in seconds",
            )
            .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]),
            &["result"],
        )
        .expect("failed to create migration_duration_seconds histogram");

        let health_sweep_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "varpulis_cluster_health_sweep_duration_seconds",
                "Health sweep duration in seconds",
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
            &["workers_checked"],
        )
        .expect("failed to create health_sweep_duration_seconds histogram");

        let deploy_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "varpulis_cluster_deploy_duration_seconds",
                "Pipeline group deploy duration in seconds",
            )
            .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]),
            &["result"],
        )
        .expect("failed to create deploy_duration_seconds histogram");

        registry
            .register(Box::new(workers_total.clone()))
            .expect("failed to register workers_total");
        registry
            .register(Box::new(pipeline_groups_total.clone()))
            .expect("failed to register pipeline_groups_total");
        registry
            .register(Box::new(deployments_total.clone()))
            .expect("failed to register deployments_total");
        registry
            .register(Box::new(migrations_total.clone()))
            .expect("failed to register migrations_total");
        registry
            .register(Box::new(migration_duration_seconds.clone()))
            .expect("failed to register migration_duration_seconds");
        registry
            .register(Box::new(health_sweep_duration_seconds.clone()))
            .expect("failed to register health_sweep_duration_seconds");
        registry
            .register(Box::new(deploy_duration_seconds.clone()))
            .expect("failed to register deploy_duration_seconds");

        Self {
            registry: Arc::new(registry),
            workers_total,
            pipeline_groups_total,
            deployments_total,
            migrations_total,
            migration_duration_seconds,
            health_sweep_duration_seconds,
            deploy_duration_seconds,
        }
    }

    /// Record worker counts by status.
    pub fn set_worker_counts(&self, ready: usize, unhealthy: usize, draining: usize) {
        self.workers_total
            .with_label_values(&["ready"])
            .set(ready as f64);
        self.workers_total
            .with_label_values(&["unhealthy"])
            .set(unhealthy as f64);
        self.workers_total
            .with_label_values(&["draining"])
            .set(draining as f64);
    }

    /// Record pipeline group and deployment counts.
    pub fn set_deployment_counts(&self, groups: usize, deployments: usize) {
        self.pipeline_groups_total.set(groups as f64);
        self.deployments_total.set(deployments as f64);
    }

    /// Record a completed migration.
    pub fn record_migration(&self, success: bool, duration_secs: f64) {
        let result = if success { "success" } else { "failure" };
        self.migrations_total.with_label_values(&[result]).inc();
        self.migration_duration_seconds
            .with_label_values(&[result])
            .observe(duration_secs);
    }

    /// Record a health sweep.
    pub fn record_health_sweep(&self, workers_checked: usize, duration_secs: f64) {
        self.health_sweep_duration_seconds
            .with_label_values(&[&workers_checked.to_string()])
            .observe(duration_secs);
    }

    /// Record a deploy operation.
    pub fn record_deploy(&self, success: bool, duration_secs: f64) {
        let result = if success { "success" } else { "failure" };
        self.deploy_duration_seconds
            .with_label_values(&[result])
            .observe(duration_secs);
    }

    /// Get Prometheus text output.
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

impl Default for ClusterPrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_metrics_new() {
        let m = ClusterPrometheusMetrics::new();
        m.set_worker_counts(0, 0, 0);
        m.set_deployment_counts(0, 0);
        let output = m.gather();
        assert!(output.contains("varpulis_cluster_workers_total"));
        assert!(output.contains("varpulis_cluster_pipeline_groups_total"));
        assert!(output.contains("varpulis_cluster_deployments_total"));
    }

    #[test]
    fn test_worker_counts() {
        let m = ClusterPrometheusMetrics::new();
        m.set_worker_counts(3, 1, 0);
        let output = m.gather();
        assert!(output.contains("varpulis_cluster_workers_total"));
    }

    #[test]
    fn test_deployment_counts() {
        let m = ClusterPrometheusMetrics::new();
        m.set_deployment_counts(2, 6);
        let output = m.gather();
        assert!(output.contains("varpulis_cluster_pipeline_groups_total"));
        assert!(output.contains("varpulis_cluster_deployments_total"));
    }

    #[test]
    fn test_record_migration() {
        let m = ClusterPrometheusMetrics::new();
        m.record_migration(true, 1.5);
        m.record_migration(false, 5.0);
        let output = m.gather();
        assert!(output.contains("varpulis_cluster_migrations_total"));
        assert!(output.contains("varpulis_cluster_migration_duration_seconds"));
    }

    #[test]
    fn test_record_health_sweep() {
        let m = ClusterPrometheusMetrics::new();
        m.record_health_sweep(4, 0.001);
        let output = m.gather();
        assert!(output.contains("varpulis_cluster_health_sweep_duration_seconds"));
    }

    #[test]
    fn test_record_deploy() {
        let m = ClusterPrometheusMetrics::new();
        m.record_deploy(true, 2.3);
        m.record_deploy(false, 0.1);
        let output = m.gather();
        assert!(output.contains("varpulis_cluster_deploy_duration_seconds"));
    }

    #[test]
    fn test_gather_output() {
        let m = ClusterPrometheusMetrics::new();
        m.set_worker_counts(2, 0, 1);
        m.set_deployment_counts(1, 3);
        m.record_migration(true, 0.5);
        m.record_deploy(true, 1.0);
        m.record_health_sweep(3, 0.002);
        let output = m.gather();
        assert!(!output.is_empty());
    }

    #[test]
    fn test_clone() {
        let m1 = ClusterPrometheusMetrics::new();
        m1.record_deploy(true, 1.0);
        let m2 = m1.clone();
        m2.record_deploy(true, 2.0);
        // Both share the same registry
        let output = m1.gather();
        assert!(output.contains("varpulis_cluster_deploy_duration_seconds"));
    }

    #[test]
    fn test_default() {
        let m = ClusterPrometheusMetrics::default();
        let output = m.gather();
        assert!(output.contains("varpulis_cluster"));
    }
}
