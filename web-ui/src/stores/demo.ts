import { defineStore } from 'pinia'
import { ref } from 'vue'

interface DemoSession {
  scenarioId: string
  groupId: string
}

export const useDemoStore = defineStore('demo', () => {
  const activeSessions = ref<Map<string, DemoSession>>(new Map())

  function getSession(scenarioId: string): DemoSession | undefined {
    return activeSessions.value.get(scenarioId)
  }

  function setSession(scenarioId: string, groupId: string) {
    activeSessions.value.set(scenarioId, { scenarioId, groupId })
  }

  function removeSession(scenarioId: string) {
    activeSessions.value.delete(scenarioId)
  }

  function hasActiveSession(scenarioId: string): boolean {
    return activeSessions.value.has(scenarioId)
  }

  return {
    activeSessions,
    getSession,
    setSession,
    removeSession,
    hasActiveSession,
  }
})
