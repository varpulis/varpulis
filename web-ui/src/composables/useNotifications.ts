import { ref, computed } from 'vue'
import { useSettingsStore } from '@/stores/settings'

export interface Notification {
  id: string
  type: 'success' | 'info' | 'warning' | 'error'
  title: string
  message?: string
  timeout?: number
  closable?: boolean
}

const notifications = ref<Notification[]>([])
let idCounter = 0

export function useNotifications() {
  const settingsStore = useSettingsStore()

  const showNotifications = computed(() => settingsStore.showNotifications)
  const soundEnabled = computed(() => settingsStore.soundEnabled)

  function notify(notification: Omit<Notification, 'id'>): string {
    if (!showNotifications.value) {
      return ''
    }

    const id = `notification-${++idCounter}`
    const fullNotification: Notification = {
      id,
      closable: true,
      timeout: 5000,
      ...notification,
    }

    notifications.value.push(fullNotification)

    // Play sound for errors and warnings
    if (soundEnabled.value && (notification.type === 'error' || notification.type === 'warning')) {
      playNotificationSound(notification.type)
    }

    // Auto-remove after timeout
    if (fullNotification.timeout && fullNotification.timeout > 0) {
      setTimeout(() => {
        remove(id)
      }, fullNotification.timeout)
    }

    return id
  }

  function success(title: string, message?: string): string {
    return notify({ type: 'success', title, message })
  }

  function info(title: string, message?: string): string {
    return notify({ type: 'info', title, message })
  }

  function warning(title: string, message?: string): string {
    return notify({ type: 'warning', title, message })
  }

  function error(title: string, message?: string): string {
    return notify({ type: 'error', title, message, timeout: 10000 })
  }

  function remove(id: string): void {
    const index = notifications.value.findIndex((n) => n.id === id)
    if (index !== -1) {
      notifications.value.splice(index, 1)
    }
  }

  function clear(): void {
    notifications.value = []
  }

  return {
    notifications,
    notify,
    success,
    info,
    warning,
    error,
    remove,
    clear,
  }
}

// Simple audio notification (uses Web Audio API)
function playNotificationSound(type: 'error' | 'warning'): void {
  try {
    const audioContext = new AudioContext()
    const oscillator = audioContext.createOscillator()
    const gainNode = audioContext.createGain()

    oscillator.connect(gainNode)
    gainNode.connect(audioContext.destination)

    // Different frequencies for different notification types
    oscillator.frequency.value = type === 'error' ? 440 : 880
    oscillator.type = 'sine'

    gainNode.gain.setValueAtTime(0.1, audioContext.currentTime)
    gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.3)

    oscillator.start()
    oscillator.stop(audioContext.currentTime + 0.3)
  } catch {
    // Audio not available
  }
}
