// Time series data point
export interface DataPoint {
  timestamp: number // Unix timestamp in ms
  value: number
}

// Time series for a single metric
export interface TimeSeries {
  name: string
  labels: Record<string, string>
  data: DataPoint[]
}

// Metric types
export type MetricType = 'counter' | 'gauge' | 'histogram' | 'summary'

// Prometheus-style metric
export interface Metric {
  name: string
  type: MetricType
  help: string
  values: MetricValue[]
}

export interface MetricValue {
  labels: Record<string, string>
  value: number
  timestamp?: number
}

// Histogram bucket
export interface HistogramBucket {
  le: number // less than or equal
  count: number
}

// Histogram metric data
export interface HistogramData {
  buckets: HistogramBucket[]
  sum: number
  count: number
  // Computed percentiles
  p50?: number
  p90?: number
  p95?: number
  p99?: number
}

// Aggregated metrics for dashboard
export interface AggregatedMetrics {
  events_processed_total: number
  events_emitted_total: number
  errors_total: number
  throughput_eps: number // events per second
  avg_latency_ms: number
  active_streams: number
  uptime_secs: number
}

// Per-worker metrics
export interface WorkerMetrics {
  worker_id: string
  events_processed: number
  events_emitted: number
  errors: number
  throughput_eps: number
  avg_latency_ms: number
  cpu_usage: number
  memory_usage_mb: number
}

// Per-pipeline metrics
export interface PipelineDetailedMetrics {
  pipeline_id: string
  pipeline_name: string
  worker_id: string
  events_in: TimeSeries
  events_out: TimeSeries
  latency_histogram: HistogramData
  errors: TimeSeries
  active_streams: number
}

// Forecast metrics from .forecast() pipelines
export interface ForecastMetrics {
  forecast_probability: number
  forecast_confidence: number
  forecast_lower: number
  forecast_upper: number
  forecast_state: string
  forecast_time_ns: number
}

// Metrics query parameters
export interface MetricsQuery {
  start_time?: number // Unix timestamp
  end_time?: number
  step?: number // Resolution in seconds
  worker_id?: string
  pipeline_id?: string
  metric_names?: string[]
}

// Time range presets
export type TimeRangePreset = '5m' | '15m' | '1h' | '6h' | '24h' | 'custom'

export interface TimeRange {
  preset: TimeRangePreset
  start?: Date
  end?: Date
}

// Chart configuration
export interface ChartConfig {
  title: string
  type: 'line' | 'bar' | 'gauge' | 'histogram'
  series: string[]
  refreshInterval?: number // ms
  yAxisLabel?: string
  yAxisMin?: number
  yAxisMax?: number
}

// Metrics store state
export interface MetricsState {
  throughput: TimeSeries
  latency: HistogramData
  errors: TimeSeries
  workerMetrics: WorkerMetrics[]
  aggregated: AggregatedMetrics
  timeRange: TimeRange
  loading: boolean
  error: string | null
}
