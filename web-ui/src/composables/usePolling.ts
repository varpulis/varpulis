import { ref, onMounted, onUnmounted, watch, type Ref } from 'vue'

export interface UsePollingOptions {
  /** Polling interval in milliseconds */
  interval: number | Ref<number>
  /** Whether to start polling immediately */
  immediate?: boolean
  /** Whether polling is enabled */
  enabled?: Ref<boolean>
  /** Callback when polling fails */
  onError?: (error: Error) => void
}

export function usePolling<T>(
  fetchFn: () => Promise<T>,
  options: UsePollingOptions
) {
  const { interval, immediate = true, enabled, onError } = options

  const data = ref<T | null>(null) as Ref<T | null>
  const loading = ref(false)
  const error = ref<Error | null>(null)
  const lastUpdate = ref<Date | null>(null)

  let intervalId: ReturnType<typeof setInterval> | null = null

  async function execute(): Promise<void> {
    loading.value = true
    error.value = null

    try {
      data.value = await fetchFn()
      lastUpdate.value = new Date()
    } catch (e) {
      error.value = e instanceof Error ? e : new Error(String(e))
      onError?.(error.value)
    } finally {
      loading.value = false
    }
  }

  function start(): void {
    if (intervalId) return

    const ms = typeof interval === 'number' ? interval : interval.value
    intervalId = setInterval(execute, ms)
  }

  function stop(): void {
    if (intervalId) {
      clearInterval(intervalId)
      intervalId = null
    }
  }

  function restart(): void {
    stop()
    start()
  }

  // Watch interval changes
  if (typeof interval !== 'number') {
    watch(interval, () => {
      if (intervalId) {
        restart()
      }
    })
  }

  // Watch enabled state
  if (enabled) {
    watch(enabled, (isEnabled) => {
      if (isEnabled) {
        start()
      } else {
        stop()
      }
    })
  }

  onMounted(() => {
    if (immediate) {
      execute()
    }

    if (!enabled || enabled.value) {
      start()
    }
  })

  onUnmounted(() => {
    stop()
  })

  return {
    data,
    loading,
    error,
    lastUpdate,
    execute,
    start,
    stop,
    restart,
  }
}

/**
 * Simplified polling hook for common use cases
 */
export function usePoll<T>(
  fetchFn: () => Promise<T>,
  intervalMs: number = 5000
) {
  return usePolling(fetchFn, {
    interval: intervalMs,
    immediate: true,
  })
}
