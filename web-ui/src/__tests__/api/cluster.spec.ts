import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useMetricsStore } from '@/stores/metrics'

/**
 * The cluster API module (src/api/cluster.ts) contains a private
 * parsePrometheusText function that is not exported. However, the
 * metrics store exposes an equivalent parsePrometheusMetrics action
 * that parses the same Prometheus text format. We test the Prometheus
 * text parsing logic via the metrics store to validate the data
 * transformation without needing network calls.
 */
describe('Prometheus text parsing (via metrics store)', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  it('parses basic metric lines without labels', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics(
      'varpulis_events_processed_total 12345\n' +
      'varpulis_events_emitted_total 6789\n' +
      'varpulis_errors_total 42\n' +
      'varpulis_active_streams 7\n' +
      'varpulis_uptime_seconds 999\n' +
      'varpulis_throughput_eps 500\n' +
      'varpulis_avg_latency_ms 2.5\n'
    )
    expect(store.aggregated.events_processed_total).toBe(12345)
    expect(store.aggregated.events_emitted_total).toBe(6789)
    expect(store.aggregated.errors_total).toBe(42)
    expect(store.aggregated.active_streams).toBe(7)
    expect(store.aggregated.uptime_secs).toBe(999)
    expect(store.aggregated.throughput_eps).toBe(500)
    expect(store.aggregated.avg_latency_ms).toBe(2.5)
  })

  it('parses metric lines with labels', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics(
      'varpulis_events_processed_total{pipeline="main"} 5000\n' +
      'varpulis_events_emitted_total{pipeline="main"} 2500\n' +
      'varpulis_errors_total 0\n' +
      'varpulis_active_streams 1\n' +
      'varpulis_uptime_seconds 300\n' +
      'varpulis_throughput_eps 50\n' +
      'varpulis_avg_latency_ms 0.8\n'
    )
    expect(store.aggregated.events_processed_total).toBe(5000)
  })

  it('skips comment lines', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics(
      '# HELP varpulis_events_processed_total Total events\n' +
      '# TYPE varpulis_events_processed_total counter\n' +
      'varpulis_events_processed_total 100\n' +
      'varpulis_events_emitted_total 50\n' +
      'varpulis_errors_total 0\n' +
      'varpulis_active_streams 0\n' +
      'varpulis_uptime_seconds 0\n' +
      'varpulis_throughput_eps 0\n' +
      'varpulis_avg_latency_ms 0\n'
    )
    expect(store.aggregated.events_processed_total).toBe(100)
  })

  it('skips blank lines', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics(
      '\n' +
      'varpulis_events_processed_total 77\n' +
      '\n' +
      'varpulis_events_emitted_total 33\n' +
      'varpulis_errors_total 0\n' +
      'varpulis_active_streams 0\n' +
      'varpulis_uptime_seconds 0\n' +
      'varpulis_throughput_eps 0\n' +
      'varpulis_avg_latency_ms 0\n'
    )
    expect(store.aggregated.events_processed_total).toBe(77)
    expect(store.aggregated.events_emitted_total).toBe(33)
  })

  it('handles scientific notation values', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics(
      'varpulis_events_processed_total 2.5e4\n' +
      'varpulis_events_emitted_total 1e3\n' +
      'varpulis_errors_total 0\n' +
      'varpulis_active_streams 0\n' +
      'varpulis_uptime_seconds 0\n' +
      'varpulis_throughput_eps 0\n' +
      'varpulis_avg_latency_ms 0\n'
    )
    expect(store.aggregated.events_processed_total).toBe(25000)
    expect(store.aggregated.events_emitted_total).toBe(1000)
  })

  it('defaults missing metrics to 0', () => {
    const store = useMetricsStore()
    // Only events_processed_total triggers the mapping;
    // missing metrics default to 0 via || 0
    store.parsePrometheusMetrics('varpulis_events_processed_total 999\n')
    expect(store.aggregated.events_processed_total).toBe(999)
    expect(store.aggregated.events_emitted_total).toBe(0)
    expect(store.aggregated.errors_total).toBe(0)
    expect(store.aggregated.throughput_eps).toBe(0)
  })

  it('does not update metrics when varpulis_events_processed_total is absent', () => {
    const store = useMetricsStore()
    store.aggregated.events_processed_total = 42 // pre-existing
    store.parsePrometheusMetrics('unrelated_metric 999\n')
    expect(store.aggregated.events_processed_total).toBe(42) // unchanged
  })

  it('handles decimal values', () => {
    const store = useMetricsStore()
    store.parsePrometheusMetrics(
      'varpulis_events_processed_total 1000\n' +
      'varpulis_events_emitted_total 500\n' +
      'varpulis_errors_total 0\n' +
      'varpulis_active_streams 0\n' +
      'varpulis_uptime_seconds 0\n' +
      'varpulis_throughput_eps 123.456\n' +
      'varpulis_avg_latency_ms 0.789\n'
    )
    expect(store.aggregated.throughput_eps).toBeCloseTo(123.456)
    expect(store.aggregated.avg_latency_ms).toBeCloseTo(0.789)
  })
})
