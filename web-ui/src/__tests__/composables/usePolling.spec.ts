import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { ref } from 'vue'
import { usePolling } from '@/composables/usePolling'

/**
 * The usePolling composable uses onMounted/onUnmounted lifecycle hooks,
 * which only fire inside a Vue component context. We test the returned
 * execute/start/stop/restart functions directly (outside component context)
 * to validate the pure logic: data updates, loading state, error handling.
 */
describe('usePolling', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('execute sets loading true then false after fetch completes', async () => {
    const fetchFn = vi.fn().mockResolvedValue('result-data')

    const { data, loading, error, execute } = usePolling(fetchFn, {
      interval: 5000,
      immediate: false,
    })

    expect(data.value).toBeNull()
    expect(loading.value).toBe(false)

    const promise = execute()
    expect(loading.value).toBe(true)

    await promise

    expect(loading.value).toBe(false)
    expect(data.value).toBe('result-data')
    expect(error.value).toBeNull()
  })

  it('execute sets error on fetch failure', async () => {
    const fetchFn = vi.fn().mockRejectedValue(new Error('network error'))

    const { data, loading, error, execute } = usePolling(fetchFn, {
      interval: 5000,
      immediate: false,
    })

    await execute()

    expect(loading.value).toBe(false)
    expect(data.value).toBeNull()
    expect(error.value).toBeInstanceOf(Error)
    expect(error.value!.message).toBe('network error')
  })

  it('execute calls onError callback when fetch fails', async () => {
    const onError = vi.fn()
    const fetchFn = vi.fn().mockRejectedValue(new Error('fail'))

    const { execute } = usePolling(fetchFn, {
      interval: 5000,
      immediate: false,
      onError,
    })

    await execute()

    expect(onError).toHaveBeenCalledOnce()
    expect(onError).toHaveBeenCalledWith(expect.any(Error))
  })

  it('execute converts non-Error rejections to Error objects', async () => {
    const fetchFn = vi.fn().mockRejectedValue('string-error')

    const { error, execute } = usePolling(fetchFn, {
      interval: 5000,
      immediate: false,
    })

    await execute()

    expect(error.value).toBeInstanceOf(Error)
    expect(error.value!.message).toBe('string-error')
  })

  it('execute updates lastUpdate timestamp on success', async () => {
    const fetchFn = vi.fn().mockResolvedValue('ok')

    const { lastUpdate, execute } = usePolling(fetchFn, {
      interval: 5000,
      immediate: false,
    })

    expect(lastUpdate.value).toBeNull()
    await execute()
    expect(lastUpdate.value).toBeInstanceOf(Date)
  })

  it('start begins periodic calls via setInterval', async () => {
    const fetchFn = vi.fn().mockResolvedValue('data')

    const { start, stop } = usePolling(fetchFn, {
      interval: 1000,
      immediate: false,
    })

    start()

    // Should not have called yet (setInterval fires after the first interval)
    expect(fetchFn).not.toHaveBeenCalled()

    // Advance 1 second
    vi.advanceTimersByTime(1000)
    expect(fetchFn).toHaveBeenCalledTimes(1)

    // Advance another second
    vi.advanceTimersByTime(1000)
    expect(fetchFn).toHaveBeenCalledTimes(2)

    stop()
  })

  it('stop clears the interval', async () => {
    const fetchFn = vi.fn().mockResolvedValue('data')

    const { start, stop } = usePolling(fetchFn, {
      interval: 1000,
      immediate: false,
    })

    start()
    vi.advanceTimersByTime(1000)
    expect(fetchFn).toHaveBeenCalledTimes(1)

    stop()

    vi.advanceTimersByTime(5000)
    // Should still be 1 because interval was cleared
    expect(fetchFn).toHaveBeenCalledTimes(1)
  })

  it('start is idempotent - calling twice does not create duplicate intervals', async () => {
    const fetchFn = vi.fn().mockResolvedValue('data')

    const { start, stop } = usePolling(fetchFn, {
      interval: 1000,
      immediate: false,
    })

    start()
    start() // second call should be a no-op

    vi.advanceTimersByTime(1000)
    expect(fetchFn).toHaveBeenCalledTimes(1) // not 2

    stop()
  })

  it('restart stops and starts with the same interval', async () => {
    const fetchFn = vi.fn().mockResolvedValue('data')

    const { start, restart, stop } = usePolling(fetchFn, {
      interval: 1000,
      immediate: false,
    })

    start()
    vi.advanceTimersByTime(500) // halfway through first interval
    restart() // resets the interval timer

    vi.advanceTimersByTime(500) // 500ms after restart - not enough
    expect(fetchFn).not.toHaveBeenCalled()

    vi.advanceTimersByTime(500) // 1000ms after restart
    expect(fetchFn).toHaveBeenCalledTimes(1)

    stop()
  })

  it('supports ref-based interval', async () => {
    const fetchFn = vi.fn().mockResolvedValue('data')
    const intervalRef = ref(2000)

    const { start, stop } = usePolling(fetchFn, {
      interval: intervalRef,
      immediate: false,
    })

    start()

    vi.advanceTimersByTime(2000)
    expect(fetchFn).toHaveBeenCalledTimes(1)

    vi.advanceTimersByTime(2000)
    expect(fetchFn).toHaveBeenCalledTimes(2)

    stop()
  })

  it('execute clears previous error on new call', async () => {
    let callCount = 0
    const fetchFn = vi.fn().mockImplementation(() => {
      callCount++
      if (callCount === 1) return Promise.reject(new Error('first fail'))
      return Promise.resolve('ok')
    })

    const { data, error, execute } = usePolling(fetchFn, {
      interval: 5000,
      immediate: false,
    })

    await execute()
    expect(error.value).not.toBeNull()

    await execute()
    expect(error.value).toBeNull()
    expect(data.value).toBe('ok')
  })
})
