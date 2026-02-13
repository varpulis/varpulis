import { describe, it, expect, beforeEach, vi } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useSettingsStore } from '@/stores/settings'

describe('useSettingsStore', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  it('loads default settings when localStorage is empty', () => {
    const store = useSettingsStore()
    expect(store.theme).toBe('dark')
    expect(store.refreshInterval).toBe(5)
    expect(store.editorFontSize).toBe(14)
    expect(store.apiKey).toBe('')
  })

  it('persists settings to localStorage on update', async () => {
    const store = useSettingsStore()
    store.updateSetting('theme', 'light')
    // Allow watcher to fire
    await vi.dynamicImportSettled()
    const stored = JSON.parse(localStorage.getItem('varpulis_settings') || '{}')
    expect(stored.theme).toBe('light')
  })

  it('excludes apiKey from localStorage persistence', async () => {
    const store = useSettingsStore()
    store.updateSetting('apiKey', 'secret-key-123')
    await vi.dynamicImportSettled()
    const stored = JSON.parse(localStorage.getItem('varpulis_settings') || '{}')
    expect(stored.apiKey).toBeUndefined()
  })

  it('exportSettings excludes apiKey', () => {
    const store = useSettingsStore()
    store.updateSetting('apiKey', 'secret-key-123')
    const exported = JSON.parse(store.exportSettings())
    expect(exported.apiKey).toBeUndefined()
    expect(exported.theme).toBe('dark')
  })

  it('importSettings applies valid settings', () => {
    const store = useSettingsStore()
    const result = store.importSettings(JSON.stringify({
      theme: 'light',
      refreshInterval: 10,
      editorFontSize: 18,
    }))
    expect(result).toBe(true)
    expect(store.theme).toBe('light')
    expect(store.refreshInterval).toBe(10)
    expect(store.editorFontSize).toBe(18)
  })

  it('importSettings clamps refreshInterval to minimum 1', () => {
    const store = useSettingsStore()
    store.importSettings(JSON.stringify({ refreshInterval: 0 }))
    expect(store.refreshInterval).toBe(5) // 0 is falsy, so default stays
  })

  it('importSettings returns false on invalid JSON', () => {
    const store = useSettingsStore()
    const result = store.importSettings('not json')
    expect(result).toBe(false)
  })

  it('resetSettings restores defaults', () => {
    const store = useSettingsStore()
    store.updateSetting('theme', 'light')
    store.updateSetting('refreshInterval', 30)
    store.resetSettings()
    expect(store.theme).toBe('dark')
    expect(store.refreshInterval).toBe(5)
  })
})
