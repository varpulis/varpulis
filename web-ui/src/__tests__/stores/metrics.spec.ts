import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useMetricsStore } from '@/stores/metrics'

describe('useMetricsStore', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  // --- Initial State ---

  it('has correct initial state', () => {
    const store = useMetricsStore()
    expect(store.throughput.name).toBe('throughput')
    expect(store.throughput.data).toEqual([])
    expect(store.errors.name).toBe('errors')
    expect(store.errors.data).toEqual([])
    expect(store.latency.buckets).toEqual([])
    expect(store.latency.sum).toBe(0)
    expect(store.latency.count).toBe(0)
    expect(store.workerMetrics).toEqual([])
    expect(store.loading).toBe(false)
    expect(store.error).toBeNull()
    expect(store.lastUpdate).toBeNull()
    expect(store.forecastData).toEqual([])
    expect(store.forecastConfidence).toBe(0)
    expect(store.forecastState).toBe('')
  })

  it('has zeroed aggregated metrics initially', () => {
    const store = useMetricsStore()
    expect(store.aggregated).toEqual({
      events_processed_total: 0,
      events_emitted_total: 0,
      errors_total: 0,
      throughput_eps: 0,
      avg_latency_ms: 0,
      active_streams: 0,
      uptime_secs: 0,
    })
  })

  it('has default time range of 5m', () => {
    const store = useMetricsStore()
    expect(store.timeRange.preset).toBe('5m')
  })

  // --- Computed: throughputData ---

  it('throughputData returns throughput.data', () => {
    const store = useMetricsStore()
    store.throughput.data = [{ timestamp: 1000, value: 42 }]
    expect(store.throughputData).toEqual([{ timestamp: 1000, value: 42 }])
  })

  // --- Computed: latencyPercentiles ---

  it('latencyPercentiles returns zeroes initially', () => {
    const store = useMetricsStore()
    expect(store.latencyPercentiles).toEqual({ p50: 0, p90: 0, p95: 0, p99: 0 })
  })

  it('latencyPercentiles reflects updated latency data', () => {
    const store = useMetricsStore()
    store.updateLatencyHistogram({
      buckets: [],
      sum: 100,
      count: 10,
      p50: 5.0,
      p90: 12.0,
      p95: 15.0,
      p99: 20.0,
    })
    expect(store.latencyPercentiles).toEqual({ p50: 5.0, p90: 12.0, p95: 15.0, p99: 20.0 })
  })

  // --- Computed: errorRate ---

  it('errorRate returns 0 when no events processed', () => {
    const store = useMetricsStore()
    expect(store.errorRate).toBe(0)
  })

  it('errorRate calculates percentage correctly', () => {
    const store = useMetricsStore()
    store.aggregated.events_processed_total = 1000
    store.aggregated.errors_total = 50
    expect(store.errorRate).toBe(5.0)
  })

  // --- Computed: timeRangeMs ---

  it('timeRangeMs returns correct milliseconds for presets', () => {
    const store = useMetricsStore()
    expect(store.timeRangeMs).toBe(5 * 60 * 1000) // 5m default

    store.setTimeRange('15m')
    expect(store.timeRangeMs).toBe(15 * 60 * 1000)

    store.setTimeRange('1h')
    expect(store.timeRangeMs).toBe(60 * 60 * 1000)

    store.setTimeRange('6h')
    expect(store.timeRangeMs).toBe(6 * 60 * 60 * 1000)

    store.setTimeRange('24h')
    expect(store.timeRangeMs).toBe(24 * 60 * 60 * 1000)
  })

  it('timeRangeMs returns difference for custom range', () => {
    const store = useMetricsStore()
    const start = new Date('2026-01-01T00:00:00Z')
    const end = new Date('2026-01-01T02:00:00Z')
    store.setCustomTimeRange(start, end)
    expect(store.timeRangeMs).toBe(2 * 60 * 60 * 1000)
  })

  // --- Action: updateMetrics ---

  it('updateMetrics updates aggregated values and appends data points', () => {
    const store = useMetricsStore()
    store.updateMetrics({
      events_processed: 5000,
      events_emitted: 3000,
      errors: 10,
      active_streams: 5,
      uptime_secs: 120,
      throughput_eps: 250,
      avg_latency_ms: 2.5,
    })

    expect(store.aggregated.events_processed_total).toBe(5000)
    expect(store.aggregated.events_emitted_total).toBe(3000)
    expect(store.aggregated.errors_total).toBe(10)
    expect(store.aggregated.throughput_eps).toBe(250)
    expect(store.aggregated.avg_latency_ms).toBe(2.5)
    expect(store.aggregated.active_streams).toBe(5)
    expect(store.aggregated.uptime_secs).toBe(120)

    // Data points should have been added
    expect(store.throughput.data).toHaveLength(1)
    expect(store.throughput.data[0].value).toBe(250)
    expect(store.errors.data).toHaveLength(1)
    expect(store.errors.data[0].value).toBe(10)
    expect(store.lastUpdate).not.toBeNull()
  })

  it('updateMetrics appends multiple data points over time', () => {
    const store = useMetricsStore()
    store.updateMetrics({
      events_processed: 100, events_emitted: 50, errors: 1,
      active_streams: 1, uptime_secs: 10, throughput_eps: 10, avg_latency_ms: 1,
    })
    store.updateMetrics({
      events_processed: 200, events_emitted: 100, errors: 2,
      active_streams: 1, uptime_secs: 20, throughput_eps: 20, avg_latency_ms: 1.5,
    })
    expect(store.throughput.data).toHaveLength(2)
    expect(store.errors.data).toHaveLength(2)
    // Aggregated reflects last update
    expect(store.aggregated.events_processed_total).toBe(200)
  })

  // --- Action: updateLatencyHistogram ---

  it('updateLatencyHistogram replaces latency data', () => {
    const store = useMetricsStore()
    store.updateLatencyHistogram({
      buckets: [{ le: 10, count: 5 }, { le: 50, count: 8 }],
      sum: 200,
      count: 10,
      p50: 8,
      p90: 40,
      p95: 45,
      p99: 49,
    })
    expect(store.latency.sum).toBe(200)
    expect(store.latency.count).toBe(10)
    expect(store.latency.buckets).toHaveLength(2)
  })

  // --- Action: updateWorkerMetrics ---

  it('updateWorkerMetrics replaces worker metrics array', () => {
    const store = useMetricsStore()
    store.updateWorkerMetrics([
      { worker_id: 'w1', events_processed: 100, events_emitted: 50, errors: 0, throughput_eps: 10, avg_latency_ms: 1, cpu_usage: 25, memory_usage_mb: 128 },
    ])
    expect(store.workerMetrics).toHaveLength(1)
    expect(store.workerMetrics[0].worker_id).toBe('w1')

    // Replace with new data
    store.updateWorkerMetrics([])
    expect(store.workerMetrics).toEqual([])
  })

  // --- Action: updateForecast ---

  it('updateForecast appends forecast data and updates state', () => {
    const store = useMetricsStore()
    store.updateForecast({
      forecast_probability: 0.85,
      forecast_confidence: 0.9,
      forecast_lower: 0.75,
      forecast_upper: 0.95,
      forecast_state: 'state_3',
      forecast_time_ns: 1000000,
    })
    expect(store.forecastData).toHaveLength(1)
    expect(store.forecastData[0].value).toBe(0.85)
    expect(store.forecastConfidence).toBe(0.9)
    expect(store.forecastInterval).toEqual({ lower: 0.75, upper: 0.95 })
    expect(store.forecastState).toBe('state_3')
  })

  // --- Action: setTimeRange ---

  it('setTimeRange changes preset and clears existing data', () => {
    const store = useMetricsStore()
    store.updateMetrics({
      events_processed: 100, events_emitted: 50, errors: 1,
      active_streams: 1, uptime_secs: 10, throughput_eps: 10, avg_latency_ms: 1,
    })
    expect(store.throughput.data).toHaveLength(1)

    store.setTimeRange('1h')
    expect(store.timeRange.preset).toBe('1h')
    expect(store.throughput.data).toEqual([])
    expect(store.errors.data).toEqual([])
  })

  // --- Action: setCustomTimeRange ---

  it('setCustomTimeRange sets preset to custom with start and end', () => {
    const store = useMetricsStore()
    const start = new Date('2026-01-01T00:00:00Z')
    const end = new Date('2026-01-01T06:00:00Z')
    store.setCustomTimeRange(start, end)
    expect(store.timeRange.preset).toBe('custom')
    expect(store.timeRange.start).toEqual(start)
    expect(store.timeRange.end).toEqual(end)
  })

  // --- Action: clearMetrics ---

  it('clearMetrics resets all metrics to defaults', () => {
    const store = useMetricsStore()
    // Populate some data first
    store.updateMetrics({
      events_processed: 5000, events_emitted: 3000, errors: 10,
      active_streams: 5, uptime_secs: 120, throughput_eps: 250, avg_latency_ms: 2.5,
    })
    store.updateLatencyHistogram({
      buckets: [{ le: 10, count: 5 }], sum: 50, count: 5,
    })
    store.updateWorkerMetrics([
      { worker_id: 'w1', events_processed: 100, events_emitted: 50, errors: 0, throughput_eps: 10, avg_latency_ms: 1, cpu_usage: 25, memory_usage_mb: 128 },
    ])

    store.clearMetrics()

    expect(store.throughput.data).toEqual([])
    expect(store.errors.data).toEqual([])
    expect(store.latency.buckets).toEqual([])
    expect(store.latency.sum).toBe(0)
    expect(store.latency.count).toBe(0)
    expect(store.workerMetrics).toEqual([])
    expect(store.aggregated.events_processed_total).toBe(0)
    expect(store.aggregated.throughput_eps).toBe(0)
  })

  // --- Action: parsePrometheusMetrics ---

  it('parsePrometheusMetrics parses Prometheus text format', () => {
    const store = useMetricsStore()
    const promText = [
      '# HELP varpulis_events_processed_total Total events processed',
      '# TYPE varpulis_events_processed_total counter',
      'varpulis_events_processed_total 42000',
      'varpulis_events_emitted_total 21000',
      'varpulis_errors_total 5',
      'varpulis_active_streams 3',
      'varpulis_uptime_seconds 3600',
      'varpulis_throughput_eps 500',
      'varpulis_avg_latency_ms 1.2',
    ].join('\n')

    store.parsePrometheusMetrics(promText)

    expect(store.aggregated.events_processed_total).toBe(42000)
    expect(store.aggregated.events_emitted_total).toBe(21000)
    expect(store.aggregated.errors_total).toBe(5)
    expect(store.aggregated.active_streams).toBe(3)
    expect(store.aggregated.uptime_secs).toBe(3600)
    expect(store.aggregated.throughput_eps).toBe(500)
    expect(store.aggregated.avg_latency_ms).toBe(1.2)
  })

  it('parsePrometheusMetrics ignores text without varpulis_events_processed_total', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics('some_other_metric 42\n')
    expect(store.aggregated.events_processed_total).toBe(0)
  })

  it('parsePrometheusMetrics handles empty input', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics('')
    expect(store.aggregated.events_processed_total).toBe(0)
  })

  it('parsePrometheusMetrics parses metrics with labels', () => {
    const store = useMetricsStore()
    const promText = [
      'varpulis_events_processed_total{pipeline="main"} 1000',
      'varpulis_events_emitted_total 500',
      'varpulis_errors_total 0',
      'varpulis_active_streams 1',
      'varpulis_uptime_seconds 60',
      'varpulis_throughput_eps 100',
      'varpulis_avg_latency_ms 0.5',
    ].join('\n')

    store.parsePrometheusMetrics(promText)
    expect(store.aggregated.events_processed_total).toBe(1000)
  })

  it('parsePrometheusMetrics handles scientific notation', () => {
    const store = useMetricsStore()
    const promText = [
      'varpulis_events_processed_total 1e6',
      'varpulis_events_emitted_total 5e5',
      'varpulis_errors_total 0',
      'varpulis_active_streams 0',
      'varpulis_uptime_seconds 0',
      'varpulis_throughput_eps 0',
      'varpulis_avg_latency_ms 0',
    ].join('\n')

    store.parsePrometheusMetrics(promText)
    expect(store.aggregated.events_processed_total).toBe(1000000)
    expect(store.aggregated.events_emitted_total).toBe(500000)
  })
})
