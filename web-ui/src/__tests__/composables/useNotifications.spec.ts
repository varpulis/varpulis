import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useNotifications } from '@/composables/useNotifications'
import { useSettingsStore } from '@/stores/settings'

describe('useNotifications', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    vi.useFakeTimers()
  })

  afterEach(() => {
    // Clear all notifications between tests (module-level ref persists)
    const { clear } = useNotifications()
    clear()
    vi.useRealTimers()
  })

  // --- notify ---

  it('notify adds a notification with generated id', () => {
    const { notify, notifications } = useNotifications()
    const id = notify({ type: 'info', title: 'Test' })
    expect(id).toMatch(/^notification-/)
    expect(notifications.value).toHaveLength(1)
    expect(notifications.value[0].title).toBe('Test')
    expect(notifications.value[0].type).toBe('info')
  })

  it('notify sets default closable and timeout', () => {
    const { notify, notifications } = useNotifications()
    notify({ type: 'info', title: 'Defaults' })
    expect(notifications.value[0].closable).toBe(true)
    expect(notifications.value[0].timeout).toBe(5000)
  })

  it('notify respects custom timeout and closable', () => {
    const { notify, notifications } = useNotifications()
    notify({ type: 'info', title: 'Custom', timeout: 10000, closable: false })
    expect(notifications.value[0].timeout).toBe(10000)
    expect(notifications.value[0].closable).toBe(false)
  })

  it('notify does nothing when showNotifications is disabled', () => {
    const settingsStore = useSettingsStore()
    settingsStore.updateSetting('showNotifications', false)

    const { notify, notifications } = useNotifications()
    const id = notify({ type: 'info', title: 'Hidden' })
    expect(id).toBe('')
    expect(notifications.value).toHaveLength(0)
  })

  it('notify auto-removes notification after timeout', () => {
    const { notify, notifications } = useNotifications()
    notify({ type: 'info', title: 'AutoDismiss', timeout: 3000 })
    expect(notifications.value).toHaveLength(1)

    vi.advanceTimersByTime(3000)
    expect(notifications.value).toHaveLength(0)
  })

  it('notify does not auto-remove when timeout is 0', () => {
    const { notify, notifications } = useNotifications()
    notify({ type: 'info', title: 'Persistent', timeout: 0 })
    expect(notifications.value).toHaveLength(1)

    vi.advanceTimersByTime(60000)
    expect(notifications.value).toHaveLength(1)
  })

  // --- convenience methods ---

  it('success creates a success notification', () => {
    const { success, notifications } = useNotifications()
    success('Deployed', 'Pipeline group deployed')
    expect(notifications.value).toHaveLength(1)
    expect(notifications.value[0].type).toBe('success')
    expect(notifications.value[0].title).toBe('Deployed')
    expect(notifications.value[0].message).toBe('Pipeline group deployed')
  })

  it('info creates an info notification', () => {
    const { info, notifications } = useNotifications()
    info('FYI')
    expect(notifications.value).toHaveLength(1)
    expect(notifications.value[0].type).toBe('info')
  })

  it('warning creates a warning notification', () => {
    const { warning, notifications } = useNotifications()
    warning('Slow', 'Latency above threshold')
    expect(notifications.value).toHaveLength(1)
    expect(notifications.value[0].type).toBe('warning')
  })

  it('error creates an error notification with 10s timeout', () => {
    const { error, notifications } = useNotifications()
    error('Failed', 'Connection refused')
    expect(notifications.value).toHaveLength(1)
    expect(notifications.value[0].type).toBe('error')
    expect(notifications.value[0].timeout).toBe(10000)
    expect(notifications.value[0].title).toBe('Failed')
    expect(notifications.value[0].message).toBe('Connection refused')
  })

  // --- remove ---

  it('remove deletes notification by id', () => {
    const { notify, remove, notifications } = useNotifications()
    const id1 = notify({ type: 'info', title: 'First', timeout: 0 })
    const id2 = notify({ type: 'info', title: 'Second', timeout: 0 })
    expect(notifications.value).toHaveLength(2)

    remove(id1)
    expect(notifications.value).toHaveLength(1)
    expect(notifications.value[0].id).toBe(id2)
  })

  it('remove does nothing for nonexistent id', () => {
    const { notify, remove, notifications } = useNotifications()
    notify({ type: 'info', title: 'Keep', timeout: 0 })
    remove('nonexistent-id')
    expect(notifications.value).toHaveLength(1)
  })

  // --- clear ---

  it('clear removes all notifications', () => {
    const { notify, clear, notifications } = useNotifications()
    notify({ type: 'info', title: 'A', timeout: 0 })
    notify({ type: 'warning', title: 'B', timeout: 0 })
    notify({ type: 'error', title: 'C', timeout: 0 })
    expect(notifications.value).toHaveLength(3)

    clear()
    expect(notifications.value).toHaveLength(0)
  })

  // --- multiple notifications ---

  it('multiple notifications accumulate', () => {
    const { success, warning, error, notifications } = useNotifications()
    success('A')
    warning('B')
    error('C')
    expect(notifications.value).toHaveLength(3)
    expect(notifications.value.map((n) => n.type)).toEqual(['success', 'warning', 'error'])
  })
})
