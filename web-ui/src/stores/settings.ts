import { defineStore } from 'pinia'
import { ref, watch } from 'vue'

interface Settings {
  theme: 'light' | 'dark' | 'system'
  refreshInterval: number // seconds
  metricsTimeRange: '5m' | '15m' | '1h' | '6h' | '24h'
  showNotifications: boolean
  soundEnabled: boolean
  apiKey: string
  coordinatorUrl: string
  workerPollInterval: number // seconds
  maxEventLogSize: number
  editorFontSize: number
  editorTheme: 'vs-dark' | 'vs-light' | 'hc-black'
}

const DEFAULT_SETTINGS: Settings = {
  theme: 'dark',
  refreshInterval: 5,
  metricsTimeRange: '5m',
  showNotifications: true,
  soundEnabled: false,
  apiKey: '',
  coordinatorUrl: '',
  workerPollInterval: 5,
  maxEventLogSize: 1000,
  editorFontSize: 14,
  editorTheme: 'vs-dark',
}

const STORAGE_KEY = 'varpulis_settings'

function loadSettings(): Settings {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      return { ...DEFAULT_SETTINGS, ...JSON.parse(stored) }
    }
  } catch {
    console.warn('Failed to load settings from localStorage')
  }
  return { ...DEFAULT_SETTINGS }
}

function saveSettings(settings: Settings): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(settings))
  } catch {
    console.warn('Failed to save settings to localStorage')
  }
}

export const useSettingsStore = defineStore('settings', () => {
  // State
  const settings = ref<Settings>(loadSettings())

  // Individual setting refs for easier access
  const theme = ref(settings.value.theme)
  const refreshInterval = ref(settings.value.refreshInterval)
  const metricsTimeRange = ref(settings.value.metricsTimeRange)
  const showNotifications = ref(settings.value.showNotifications)
  const soundEnabled = ref(settings.value.soundEnabled)
  const apiKey = ref(settings.value.apiKey)
  const coordinatorUrl = ref(settings.value.coordinatorUrl)
  const workerPollInterval = ref(settings.value.workerPollInterval)
  const maxEventLogSize = ref(settings.value.maxEventLogSize)
  const editorFontSize = ref(settings.value.editorFontSize)
  const editorTheme = ref(settings.value.editorTheme)

  // Watch for changes and save
  watch(
    [
      theme,
      refreshInterval,
      metricsTimeRange,
      showNotifications,
      soundEnabled,
      apiKey,
      coordinatorUrl,
      workerPollInterval,
      maxEventLogSize,
      editorFontSize,
      editorTheme,
    ],
    () => {
      const newSettings: Settings = {
        theme: theme.value,
        refreshInterval: refreshInterval.value,
        metricsTimeRange: metricsTimeRange.value,
        showNotifications: showNotifications.value,
        soundEnabled: soundEnabled.value,
        apiKey: apiKey.value,
        coordinatorUrl: coordinatorUrl.value,
        workerPollInterval: workerPollInterval.value,
        maxEventLogSize: maxEventLogSize.value,
        editorFontSize: editorFontSize.value,
        editorTheme: editorTheme.value,
      }
      settings.value = newSettings
      saveSettings(newSettings)
    },
    { deep: true }
  )

  // Actions
  function updateSetting<K extends keyof Settings>(key: K, value: Settings[K]): void {
    switch (key) {
      case 'theme':
        theme.value = value as Settings['theme']
        break
      case 'refreshInterval':
        refreshInterval.value = value as number
        break
      case 'metricsTimeRange':
        metricsTimeRange.value = value as Settings['metricsTimeRange']
        break
      case 'showNotifications':
        showNotifications.value = value as boolean
        break
      case 'soundEnabled':
        soundEnabled.value = value as boolean
        break
      case 'apiKey':
        apiKey.value = value as string
        break
      case 'coordinatorUrl':
        coordinatorUrl.value = value as string
        break
      case 'workerPollInterval':
        workerPollInterval.value = value as number
        break
      case 'maxEventLogSize':
        maxEventLogSize.value = value as number
        break
      case 'editorFontSize':
        editorFontSize.value = value as number
        break
      case 'editorTheme':
        editorTheme.value = value as Settings['editorTheme']
        break
    }
  }

  function resetSettings(): void {
    theme.value = DEFAULT_SETTINGS.theme
    refreshInterval.value = DEFAULT_SETTINGS.refreshInterval
    metricsTimeRange.value = DEFAULT_SETTINGS.metricsTimeRange
    showNotifications.value = DEFAULT_SETTINGS.showNotifications
    soundEnabled.value = DEFAULT_SETTINGS.soundEnabled
    apiKey.value = DEFAULT_SETTINGS.apiKey
    coordinatorUrl.value = DEFAULT_SETTINGS.coordinatorUrl
    workerPollInterval.value = DEFAULT_SETTINGS.workerPollInterval
    maxEventLogSize.value = DEFAULT_SETTINGS.maxEventLogSize
    editorFontSize.value = DEFAULT_SETTINGS.editorFontSize
    editorTheme.value = DEFAULT_SETTINGS.editorTheme
  }

  function exportSettings(): string {
    return JSON.stringify(settings.value, null, 2)
  }

  function importSettings(json: string): boolean {
    try {
      const imported = JSON.parse(json) as Partial<Settings>
      if (imported.theme) theme.value = imported.theme
      if (imported.refreshInterval) refreshInterval.value = imported.refreshInterval
      if (imported.metricsTimeRange) metricsTimeRange.value = imported.metricsTimeRange
      if (imported.showNotifications !== undefined) showNotifications.value = imported.showNotifications
      if (imported.soundEnabled !== undefined) soundEnabled.value = imported.soundEnabled
      if (imported.apiKey !== undefined) apiKey.value = imported.apiKey
      if (imported.coordinatorUrl !== undefined) coordinatorUrl.value = imported.coordinatorUrl
      if (imported.workerPollInterval) workerPollInterval.value = imported.workerPollInterval
      if (imported.maxEventLogSize) maxEventLogSize.value = imported.maxEventLogSize
      if (imported.editorFontSize) editorFontSize.value = imported.editorFontSize
      if (imported.editorTheme) editorTheme.value = imported.editorTheme
      return true
    } catch {
      return false
    }
  }

  return {
    // State
    settings,
    theme,
    refreshInterval,
    metricsTimeRange,
    showNotifications,
    soundEnabled,
    apiKey,
    coordinatorUrl,
    workerPollInterval,
    maxEventLogSize,
    editorFontSize,
    editorTheme,

    // Actions
    updateSetting,
    resetSettings,
    exportSettings,
    importSettings,
  }
})
