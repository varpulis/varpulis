import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type {
  TimeSeries,
  DataPoint,
  HistogramData,
  AggregatedMetrics,
  WorkerMetrics,
  TimeRange,
  TimeRangePreset,
  ForecastMetrics,
} from '@/types/metrics'

const MAX_DATA_POINTS = 300 // 5 minutes at 1s resolution

export const useMetricsStore = defineStore('metrics', () => {
  // State
  const throughput = ref<TimeSeries>({
    name: 'throughput',
    labels: {},
    data: [],
  })

  const latency = ref<HistogramData>({
    buckets: [],
    sum: 0,
    count: 0,
    p50: 0,
    p90: 0,
    p95: 0,
    p99: 0,
  })

  const errors = ref<TimeSeries>({
    name: 'errors',
    labels: {},
    data: [],
  })

  const workerMetrics = ref<WorkerMetrics[]>([])

  // Forecast-specific data
  const forecastData = ref<DataPoint[]>([])
  const forecastConfidence = ref(0)
  const forecastInterval = ref<{ lower: number; upper: number }>({ lower: 0, upper: 0 })
  const forecastState = ref('')

  const aggregated = ref<AggregatedMetrics>({
    events_processed_total: 0,
    events_emitted_total: 0,
    errors_total: 0,
    throughput_eps: 0,
    avg_latency_ms: 0,
    active_streams: 0,
    uptime_secs: 0,
  })

  const timeRange = ref<TimeRange>({
    preset: '5m',
  })

  const loading = ref(false)
  const error = ref<string | null>(null)
  const lastUpdate = ref<Date | null>(null)

  // Computed
  const throughputData = computed(() => throughput.value.data)

  const latencyPercentiles = computed(() => ({
    p50: latency.value.p50 || 0,
    p90: latency.value.p90 || 0,
    p95: latency.value.p95 || 0,
    p99: latency.value.p99 || 0,
  }))

  const errorRate = computed(() => {
    if (aggregated.value.events_processed_total === 0) return 0
    return (aggregated.value.errors_total / aggregated.value.events_processed_total) * 100
  })

  const timeRangeMs = computed(() => {
    const presets: Record<TimeRangePreset, number> = {
      '5m': 5 * 60 * 1000,
      '15m': 15 * 60 * 1000,
      '1h': 60 * 60 * 1000,
      '6h': 6 * 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      custom: 0,
    }
    if (timeRange.value.preset === 'custom' && timeRange.value.start && timeRange.value.end) {
      return timeRange.value.end.getTime() - timeRange.value.start.getTime()
    }
    return presets[timeRange.value.preset]
  })

  // Actions
  function updateMetrics(data: {
    events_processed: number
    events_emitted: number
    errors: number
    active_streams: number
    uptime_secs: number
    throughput_eps: number
    avg_latency_ms: number
  }): void {
    const now = Date.now()

    // Update aggregated metrics
    aggregated.value = {
      events_processed_total: data.events_processed,
      events_emitted_total: data.events_emitted,
      errors_total: data.errors,
      throughput_eps: data.throughput_eps,
      avg_latency_ms: data.avg_latency_ms,
      active_streams: data.active_streams,
      uptime_secs: data.uptime_secs,
    }

    // Add data point to throughput time series
    addDataPoint(throughput.value, { timestamp: now, value: data.throughput_eps })

    // Add data point to errors time series
    addDataPoint(errors.value, { timestamp: now, value: data.errors })

    lastUpdate.value = new Date(now)
  }

  function updateLatencyHistogram(data: HistogramData): void {
    latency.value = data
  }

  function updateWorkerMetrics(metrics: WorkerMetrics[]): void {
    workerMetrics.value = metrics
  }

  function addDataPoint(series: TimeSeries, point: DataPoint): void {
    series.data.push(point)

    // Trim old data points
    const cutoff = Date.now() - timeRangeMs.value
    series.data = series.data.filter((p) => p.timestamp >= cutoff)

    // Also limit total points
    if (series.data.length > MAX_DATA_POINTS) {
      series.data = series.data.slice(-MAX_DATA_POINTS)
    }
  }

  function setTimeRange(preset: TimeRangePreset): void {
    timeRange.value = { preset }
    // Clear existing data when changing time range
    throughput.value.data = []
    errors.value.data = []
  }

  function setCustomTimeRange(start: Date, end: Date): void {
    timeRange.value = {
      preset: 'custom',
      start,
      end,
    }
  }

  function clearMetrics(): void {
    throughput.value.data = []
    errors.value.data = []
    latency.value = {
      buckets: [],
      sum: 0,
      count: 0,
    }
    workerMetrics.value = []
    aggregated.value = {
      events_processed_total: 0,
      events_emitted_total: 0,
      errors_total: 0,
      throughput_eps: 0,
      avg_latency_ms: 0,
      active_streams: 0,
      uptime_secs: 0,
    }
  }

  function updateForecast(data: ForecastMetrics): void {
    const now = Date.now()
    forecastConfidence.value = data.forecast_confidence
    forecastInterval.value = { lower: data.forecast_lower, upper: data.forecast_upper }
    forecastState.value = data.forecast_state

    forecastData.value.push({ timestamp: now, value: data.forecast_probability })
    const cutoff = now - timeRangeMs.value
    forecastData.value = forecastData.value.filter((p) => p.timestamp >= cutoff)
    if (forecastData.value.length > MAX_DATA_POINTS) {
      forecastData.value = forecastData.value.slice(-MAX_DATA_POINTS)
    }
  }

  // Parse Prometheus metrics format
  function parsePrometheusMetrics(text: string): void {
    const lines = text.split('\n')
    const metrics: Record<string, number> = {}

    for (const line of lines) {
      if (line.startsWith('#') || !line.trim()) continue

      const match = line.match(/^(\w+)(?:\{[^}]*\})?\s+([\d.e+-]+)/)
      if (match) {
        const [, name, value] = match
        metrics[name] = parseFloat(value)
      }
    }

    // Map Prometheus metrics to our format
    if ('varpulis_events_processed_total' in metrics) {
      updateMetrics({
        events_processed: metrics['varpulis_events_processed_total'] || 0,
        events_emitted: metrics['varpulis_events_emitted_total'] || 0,
        errors: metrics['varpulis_errors_total'] || 0,
        active_streams: metrics['varpulis_active_streams'] || 0,
        uptime_secs: metrics['varpulis_uptime_seconds'] || 0,
        throughput_eps: metrics['varpulis_throughput_eps'] || 0,
        avg_latency_ms: metrics['varpulis_avg_latency_ms'] || 0,
      })
    }
  }

  return {
    // State
    throughput,
    latency,
    errors,
    workerMetrics,
    aggregated,
    timeRange,
    loading,
    error,
    lastUpdate,
    forecastData,
    forecastConfidence,
    forecastInterval,
    forecastState,

    // Computed
    throughputData,
    latencyPercentiles,
    errorRate,
    timeRangeMs,

    // Actions
    updateMetrics,
    updateLatencyHistogram,
    updateWorkerMetrics,
    updateForecast,
    setTimeRange,
    setCustomTimeRange,
    clearMetrics,
    parsePrometheusMetrics,
  }
})
